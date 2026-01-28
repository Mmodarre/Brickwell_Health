"""
Claim propensity model for Brickwell Health Simulator.

Models claim frequency and amounts based on demographics and APRA 2024-2025 data.
Uses Poisson-based frequency by age group and lognormal severity distributions.
"""

from typing import Any

import numpy as np
from numpy.random import Generator as RNG

from brickwell_health.config.models import ClaimsConfig
from brickwell_health.domain.enums import DentalServiceType
from brickwell_health.reference.loader import ReferenceDataLoader


class ClaimPropensityModel:
    """
    Models claim propensity and amounts based on APRA-validated parameters.

    Factors considered:
    - Age group (Poisson lambda varies by age bracket)
    - Service type (different frequencies and cost distributions)
    - High-value claims (8.8% of hospital claims > $10k)
    - Dental sub-categories (preventative/general/major)
    """

    # Default age group boundaries (used if config not provided)
    AGE_GROUP_BOUNDARIES = [
        (0, 17, "0-17"),
        (18, 30, "18-30"),
        (31, 45, "31-45"),
        (46, 60, "46-60"),
        (61, 70, "61-70"),
        (71, 999, "71+"),
    ]

    # Legacy service type weights (used if config not available for a service)
    EXTRAS_SERVICE_WEIGHTS = {
        "Dental": 0.35,
        "Optical": 0.20,
        "Physiotherapy": 0.15,
        "Chiropractic": 0.08,
        "Podiatry": 0.05,
        "Psychology": 0.05,
        "Massage": 0.05,
        "Acupuncture": 0.03,
        "Other": 0.04,
    }

    # Legacy average claim amounts (fallback)
    AVG_CLAIM_AMOUNTS = {
        "Dental": 280.0,  # Weighted average of sub-categories
        "Optical": 350.0,
        "Physiotherapy": 85.0,
        "Chiropractic": 70.0,
        "Podiatry": 60.0,
        "Psychology": 150.0,
        "Massage": 80.0,
        "Acupuncture": 70.0,
        "Other": 85.0,
        "Hospital": 4500.0,
        "Ambulance": 950.0,
    }

    def __init__(
        self,
        rng: RNG,
        reference: ReferenceDataLoader | None = None,
        config: ClaimsConfig | None = None,
    ):
        """
        Initialize the claim propensity model.

        Args:
            rng: NumPy random number generator
            reference: Optional reference data loader
            config: Optional claims configuration (uses defaults if not provided)
        """
        self.rng = rng
        self.reference = reference
        self.config = config or ClaimsConfig()

    def _get_age_group(self, age: int) -> str:
        """
        Map age to frequency parameter group.

        Args:
            age: Age in years

        Returns:
            Age group string (e.g., '18-30', '61-70', '71+')
        """
        for min_age, max_age, group in self.AGE_GROUP_BOUNDARIES:
            if min_age <= age <= max_age:
                return group
        return "71+"  # Default for very old ages

    def get_age_multiplier(self, age: int) -> float:
        """
        Get claim propensity multiplier for an age (legacy compatibility).

        Args:
            age: Age in years

        Returns:
            Multiplier for base claim rate
        """
        # Map to approximate multipliers based on hospital frequency
        age_group = self._get_age_group(age)
        base_rate = self.config.hospital_frequency.get("31-45", 0.5)
        group_rate = self.config.hospital_frequency.get(age_group, base_rate)
        return group_rate / base_rate if base_rate > 0 else 1.0

    # ========================================================================
    # HOSPITAL CLAIMS
    # ========================================================================

    def get_hospital_admission_rate(self, age: int) -> float:
        """
        Get annual hospital admission rate (Poisson lambda) for a member.

        Uses age-group specific rates from APRA data.

        Args:
            age: Member's age

        Returns:
            Poisson lambda (expected admissions per year)
        """
        age_group = self._get_age_group(age)
        return self.config.hospital_frequency.get(age_group, 0.5)

    def sample_hospital_claim_amount(self, age: int | None = None) -> float:
        """
        Sample hospital claim amount using lognormal + high-claim distribution.

        8.8% of claims are sampled from a tiered high-claim distribution (>$10k).
        The remaining 91.2% come from a lognormal distribution (can also exceed $10k).

        Args:
            age: Optional age for any age-based adjustments

        Returns:
            Claim amount in dollars
        """
        # Check if this is a high-value claim (8.8% probability)
        if self.rng.random() < self.config.high_claim_probability:
            return self._sample_high_claim()

        # Standard claim: lognormal distribution
        # mu=8.0, sigma=1.5 produces median ~$2,981 matching APRA data
        mu = self.config.hospital_severity.mu
        sigma = self.config.hospital_severity.sigma
        return self.rng.lognormal(mu, sigma)

    def _sample_high_claim(self) -> float:
        """
        Sample from tiered high-claim distribution (>$10k).

        Returns:
            High-value claim amount
        """
        tiers = self.config.high_claim_tiers
        if not tiers:
            # Fallback if no tiers configured
            return self.rng.uniform(10000, 50000)

        # Extract weights and normalize
        weights = [tier.weight for tier in tiers]
        total = sum(weights)
        probs = [w / total for w in weights]

        # Sample tier index
        tier_idx = self.rng.choice(len(tiers), p=probs)
        tier = tiers[tier_idx]

        # Uniform sample within tier range
        min_val, max_val = tier.range
        return self.rng.uniform(min_val, max_val)

    # ========================================================================
    # DENTAL CLAIMS (Sub-categorized)
    # ========================================================================

    def get_dental_claim_rate(self, dental_type: DentalServiceType) -> float:
        """
        Get annual claim rate for a dental sub-category.

        Args:
            dental_type: Dental service sub-category

        Returns:
            Poisson lambda (expected claims per year)
        """
        type_key = dental_type.value.lower()
        return self.config.dental_frequency.get(type_key, 1.0)

    def get_total_dental_rate(self) -> float:
        """
        Get total annual dental claim rate (sum of all sub-categories).

        Returns:
            Total expected dental claims per year
        """
        return sum(self.config.dental_frequency.values())

    def sample_dental_service_type(self) -> DentalServiceType:
        """
        Sample dental sub-category based on frequency weights.

        Returns:
            DentalServiceType (PREVENTATIVE, GENERAL, or MAJOR)
        """
        # Use frequencies as weights
        types = [
            DentalServiceType.PREVENTATIVE,
            DentalServiceType.GENERAL,
            DentalServiceType.MAJOR,
        ]
        weights = [
            self.config.dental_frequency.get("preventative", 2.0),
            self.config.dental_frequency.get("general", 0.5),
            self.config.dental_frequency.get("major", 0.1),
        ]
        total = sum(weights)
        probs = [w / total for w in weights]

        # Use index selection to avoid numpy returning string instead of enum
        idx = self.rng.choice(len(types), p=probs)
        return types[idx]

    def sample_dental_claim_amount(self, dental_type: DentalServiceType) -> float:
        """
        Sample dental claim amount using type-specific normal distribution.

        Args:
            dental_type: Dental service sub-category

        Returns:
            Claim amount in dollars
        """
        type_key = dental_type.value.lower()
        cost_config = self.config.dental_costs.get(type_key)

        if cost_config:
            mean = cost_config.mean
            std = cost_config.std
        else:
            # Fallback defaults
            defaults = {
                "preventative": (175, 35),
                "general": (280, 90),
                "major": (1300, 450),
            }
            mean, std = defaults.get(type_key, (200, 50))

        # Sample from normal, enforce minimum
        amount = self.rng.normal(mean, std)
        min_amounts = {"preventative": 50, "general": 100, "major": 500}
        min_amount = min_amounts.get(type_key, 50)

        return max(min_amount, round(amount, 2))

    # ========================================================================
    # OTHER EXTRAS CLAIMS
    # ========================================================================

    def get_extras_claim_rate(self, age: int) -> float:
        """
        Get annual extras claim rate for a member.

        Combines all extras service frequencies.

        Args:
            age: Member's age

        Returns:
            Expected claims per year
        """
        # Sum of all extras service frequencies
        total_rate = (
            self.get_total_dental_rate()
            + self._get_optical_rate(age)
            + self._get_physiotherapy_rate(age)
            + self._get_chiropractic_rate(age)
        )

        # Add estimated rates for other services (podiatry, psychology, etc.)
        other_rate = 0.5  # Approximate for less common services

        return total_rate + other_rate

    def _get_optical_rate(self, age: int) -> float:
        """Get optical claim frequency."""
        return self.config.optical.frequency

    def _get_physiotherapy_rate(self, age: int) -> float:
        """Get physiotherapy claim frequency with age adjustment."""
        base_rate = self.config.physiotherapy.frequency
        if age >= 65:
            return base_rate * self.config.physiotherapy.age_65_multiplier
        return base_rate

    def _get_chiropractic_rate(self, age: int) -> float:
        """Get chiropractic claim frequency."""
        return self.config.chiropractic.frequency

    def sample_extras_service_type(self) -> str:
        """
        Sample an extras service type.

        Returns:
            Service type name
        """
        # Calculate weights based on configured frequencies
        dental_weight = self.get_total_dental_rate()
        optical_weight = self.config.optical.frequency
        physio_weight = self.config.physiotherapy.frequency
        chiro_weight = self.config.chiropractic.frequency
        other_weight = 0.5  # Other services

        services = ["Dental", "Optical", "Physiotherapy", "Chiropractic", "Other"]
        weights = [dental_weight, optical_weight, physio_weight, chiro_weight, other_weight]
        total = sum(weights)
        probs = [w / total for w in weights]

        return self.rng.choice(services, p=probs)

    def sample_claim_amount(
        self,
        service_type: str,
        age: int | None = None,
    ) -> float:
        """
        Sample a claim amount for a service type.

        Args:
            service_type: Type of service
            age: Optional age for adjustment

        Returns:
            Claim amount in dollars
        """
        # Hospital claims use special distribution
        if service_type == "Hospital":
            return self.sample_hospital_claim_amount(age)

        # Dental claims - sample sub-type first, then amount
        if service_type == "Dental":
            dental_type = self.sample_dental_service_type()
            return self.sample_dental_claim_amount(dental_type)

        # Other extras - use configured parameters or fallback
        if service_type == "Optical":
            mean = self.config.optical.mean
            std = self.config.optical.std
            min_val = 150
        elif service_type == "Physiotherapy":
            mean = self.config.physiotherapy.mean
            std = self.config.physiotherapy.std
            min_val = 50
        elif service_type == "Chiropractic":
            mean = self.config.chiropractic.mean
            std = self.config.chiropractic.std
            min_val = 50
        elif service_type == "Ambulance":
            mean = self.config.ambulance.mean
            std = self.config.ambulance.std
            min_val = 200
        else:
            # Fallback for other services
            mean = self.AVG_CLAIM_AMOUNTS.get(service_type, 85.0)
            std = mean * 0.3  # 30% standard deviation
            min_val = 20

        amount = self.rng.normal(mean, std)
        return max(min_val, round(amount, 2))

    # ========================================================================
    # AMBULANCE CLAIMS
    # ========================================================================

    def get_ambulance_claim_rate(self, age: int) -> float:
        """
        Get annual ambulance claim rate for a member.

        Args:
            age: Member's age

        Returns:
            Expected claims per year
        """
        base_rate = self.config.ambulance.frequency
        # Slight age adjustment - older members more likely to need ambulance
        if age >= 65:
            return base_rate * 1.5
        return base_rate

    # ========================================================================
    # BENEFIT CALCULATIONS
    # ========================================================================

    def sample_benefit_percentage(
        self,
        service_type: str,
    ) -> float:
        """
        Sample benefit percentage for a service.

        Args:
            service_type: Type of service

        Returns:
            Benefit percentage (0-1)
        """
        # Base percentages by service type
        base_pct = {
            "Dental": 0.60,
            "Optical": 0.65,
            "Physiotherapy": 0.75,
            "Chiropractic": 0.70,
            "Podiatry": 0.70,
            "Psychology": 0.60,
            "Hospital": 1.00,  # Full benefit for contracted
            "Ambulance": 1.00,
        }

        base = base_pct.get(service_type, 0.65)

        # Add some variation
        variation = self.rng.uniform(-0.10, 0.10)

        return max(0.40, min(1.00, base + variation))

    # ========================================================================
    # HOSPITAL ADMISSION DETAILS
    # ========================================================================

    def sample_hospital_length_of_stay(
        self,
        admission_type: str,
        age: int,
    ) -> int:
        """
        Sample hospital length of stay.

        Args:
            admission_type: Elective/Emergency/Maternity
            age: Patient's age

        Returns:
            Length of stay in days
        """
        if admission_type == "DaySurgery":
            return 0

        # Base LOS by admission type
        if admission_type == "Emergency":
            base_los = 3.0
        elif admission_type == "Maternity":
            base_los = 2.5
        else:  # Elective
            base_los = 2.0

        # Age adjustment
        if age > 75:
            base_los *= 1.5
        elif age > 65:
            base_los *= 1.2

        # Sample from exponential
        los = self.rng.exponential(base_los)

        return max(1, int(los))

    def sample_clinical_category(
        self,
        age: int,
        gender: str,
    ) -> int:
        """
        Sample a clinical category for hospital admission.

        Args:
            age: Patient's age
            gender: Patient's gender

        Returns:
            Clinical category ID
        """
        # Common clinical categories (simplified)
        categories = list(range(1, 39))  # 38 clinical categories

        # Weight by age/gender patterns
        weights = [1.0] * len(categories)

        # Adjust weights based on demographics
        if gender == "Female" and 20 <= age <= 45:
            # Higher weight for obstetric (category ~30)
            if len(weights) > 30:
                weights[29] = 5.0

        if age > 65:
            # Higher weight for cardiac, joint (categories ~5, ~15)
            if len(weights) > 5:
                weights[4] = 3.0
            if len(weights) > 15:
                weights[14] = 3.0

        total = sum(weights)
        probs = [w / total for w in weights]

        return int(self.rng.choice(categories, p=probs))

    # ========================================================================
    # LEGACY COMPATIBILITY
    # ========================================================================

    def should_generate_extras_claim(
        self,
        age: int,
        days_since_last_claim: float,
    ) -> bool:
        """
        Determine if an extras claim should be generated (legacy method).

        Uses Poisson process with age-adjusted rate.

        Args:
            age: Member's age
            days_since_last_claim: Days since last claim check

        Returns:
            True if a claim should be generated
        """
        annual_rate = self.get_extras_claim_rate(age)
        daily_rate = annual_rate / 365.0
        expected = daily_rate * days_since_last_claim

        # Poisson probability of at least one event
        return self.rng.random() < (1 - np.exp(-expected))
