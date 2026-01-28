"""
Unit tests for ClaimPropensityModel with APRA-based claiming patterns.
"""

import numpy as np
import pytest

from brickwell_health.config.models import ClaimsConfig
from brickwell_health.domain.enums import DentalServiceType
from brickwell_health.statistics.claim_propensity import ClaimPropensityModel


class TestAgeGroupMapping:
    """Tests for age group mapping functionality."""

    def test_age_group_young_adult(self, test_rng: np.random.Generator):
        """Young adult (18-30) maps to correct age group."""
        model = ClaimPropensityModel(test_rng)

        assert model._get_age_group(18) == "18-30"
        assert model._get_age_group(25) == "18-30"
        assert model._get_age_group(30) == "18-30"

    def test_age_group_middle_age(self, test_rng: np.random.Generator):
        """Middle age (31-45) maps to correct age group."""
        model = ClaimPropensityModel(test_rng)

        assert model._get_age_group(31) == "31-45"
        assert model._get_age_group(40) == "31-45"
        assert model._get_age_group(45) == "31-45"

    def test_age_group_established(self, test_rng: np.random.Generator):
        """Established (46-60) maps to correct age group."""
        model = ClaimPropensityModel(test_rng)

        assert model._get_age_group(46) == "46-60"
        assert model._get_age_group(55) == "46-60"
        assert model._get_age_group(60) == "46-60"

    def test_age_group_senior(self, test_rng: np.random.Generator):
        """Senior (61-70) maps to correct age group."""
        model = ClaimPropensityModel(test_rng)

        assert model._get_age_group(61) == "61-70"
        assert model._get_age_group(65) == "61-70"
        assert model._get_age_group(70) == "61-70"

    def test_age_group_elderly(self, test_rng: np.random.Generator):
        """Elderly (71+) maps to correct age group."""
        model = ClaimPropensityModel(test_rng)

        assert model._get_age_group(71) == "71+"
        assert model._get_age_group(80) == "71+"
        assert model._get_age_group(95) == "71+"

    def test_age_group_child(self, test_rng: np.random.Generator):
        """Child (0-17) maps to correct age group."""
        model = ClaimPropensityModel(test_rng)

        assert model._get_age_group(5) == "0-17"
        assert model._get_age_group(17) == "0-17"


class TestHospitalAdmissionRate:
    """Tests for hospital admission rate based on age."""

    def test_hospital_rate_increases_with_age(self, test_rng: np.random.Generator):
        """Hospital admission rate should increase with age."""
        model = ClaimPropensityModel(test_rng)

        rate_young = model.get_hospital_admission_rate(25)
        rate_middle = model.get_hospital_admission_rate(40)
        rate_senior = model.get_hospital_admission_rate(65)
        rate_elderly = model.get_hospital_admission_rate(75)

        assert rate_young < rate_middle < rate_senior < rate_elderly

    def test_hospital_rate_matches_config(self, test_rng: np.random.Generator):
        """Hospital rate should match configured Poisson lambda."""
        config = ClaimsConfig()
        model = ClaimPropensityModel(test_rng, config=config)

        # Young adult should get 0.3
        assert model.get_hospital_admission_rate(25) == 0.3

        # Senior should get 2.0
        assert model.get_hospital_admission_rate(65) == 2.0

        # Elderly should get 2.5
        assert model.get_hospital_admission_rate(75) == 2.5


class TestHighClaimDistribution:
    """Tests for high-value claim distribution (>$10k)."""

    def test_high_claim_above_threshold(self, test_rng: np.random.Generator):
        """High claims should always be >= $10,000."""
        model = ClaimPropensityModel(test_rng)

        # Sample many high claims
        high_claims = [model._sample_high_claim() for _ in range(100)]

        assert all(claim >= 10000 for claim in high_claims)

    def test_high_claim_distribution_range(self, test_rng: np.random.Generator):
        """High claims should fall within configured tier ranges."""
        model = ClaimPropensityModel(test_rng)

        # Sample many high claims
        high_claims = [model._sample_high_claim() for _ in range(1000)]

        # Most should be in the $10k-$20k range (63.4% weight)
        in_first_tier = sum(1 for c in high_claims if 10000 <= c < 20000)
        proportion_first_tier = in_first_tier / len(high_claims)

        # Allow for statistical variation
        assert 0.55 <= proportion_first_tier <= 0.75

    def test_hospital_claim_median_matches_apra(self, test_rng: np.random.Generator):
        """Hospital claim median should be around $2,981 (APRA benchmark)."""
        # Use a fixed seed for reproducibility
        rng = np.random.default_rng(42)
        model = ClaimPropensityModel(rng)

        # Sample many claims
        n_samples = 10000
        amounts = [model.sample_hospital_claim_amount() for _ in range(n_samples)]

        median_amount = np.median(amounts)

        # Median should be around $2,981 (allow for statistical variation)
        # lognormal(8.0, 1.5) has median = exp(8.0) ≈ $2,981
        assert 2500 <= median_amount <= 4000

    def test_hospital_claim_extreme_values_rare(self, test_rng: np.random.Generator):
        """Extremely high claims (>$100k) should be rare."""
        # Use a fixed seed for reproducibility
        rng = np.random.default_rng(42)
        model = ClaimPropensityModel(rng)

        # Sample many claims
        n_samples = 10000
        extreme_count = sum(
            1 for _ in range(n_samples)
            if model.sample_hospital_claim_amount() > 100000
        )

        proportion_extreme = extreme_count / n_samples

        # Should be less than 5% (very high claims are rare)
        assert proportion_extreme < 0.05


class TestDentalSubCategories:
    """Tests for dental sub-category functionality."""

    def test_sample_dental_service_type_returns_enum(
        self, test_rng: np.random.Generator
    ):
        """sample_dental_service_type should return DentalServiceType enum."""
        model = ClaimPropensityModel(test_rng)

        dental_type = model.sample_dental_service_type()

        assert isinstance(dental_type, DentalServiceType)
        assert dental_type in [
            DentalServiceType.PREVENTATIVE,
            DentalServiceType.GENERAL,
            DentalServiceType.MAJOR,
        ]

    def test_dental_type_distribution(self, test_rng: np.random.Generator):
        """Dental types should follow configured frequency distribution."""
        # Use fixed seed
        rng = np.random.default_rng(42)
        model = ClaimPropensityModel(rng)

        # Sample many dental types
        n_samples = 10000
        counts = {
            DentalServiceType.PREVENTATIVE: 0,
            DentalServiceType.GENERAL: 0,
            DentalServiceType.MAJOR: 0,
        }

        for _ in range(n_samples):
            dental_type = model.sample_dental_service_type()
            counts[dental_type] += 1

        # Calculate proportions
        proportions = {k: v / n_samples for k, v in counts.items()}

        # Expected proportions based on frequencies (2.0, 0.5, 0.1) -> (0.77, 0.19, 0.04)
        # Allow for statistical variation
        assert 0.70 <= proportions[DentalServiceType.PREVENTATIVE] <= 0.85
        assert 0.12 <= proportions[DentalServiceType.GENERAL] <= 0.26
        assert 0.02 <= proportions[DentalServiceType.MAJOR] <= 0.08

    def test_dental_claim_amounts_vary_by_type(self, test_rng: np.random.Generator):
        """Different dental types should have different mean claim amounts."""
        # Use fixed seed
        rng = np.random.default_rng(42)
        model = ClaimPropensityModel(rng)

        # Sample many claims of each type
        n_samples = 1000

        preventative_amounts = [
            model.sample_dental_claim_amount(DentalServiceType.PREVENTATIVE)
            for _ in range(n_samples)
        ]
        general_amounts = [
            model.sample_dental_claim_amount(DentalServiceType.GENERAL)
            for _ in range(n_samples)
        ]
        major_amounts = [
            model.sample_dental_claim_amount(DentalServiceType.MAJOR)
            for _ in range(n_samples)
        ]

        # Check means are in expected ranges
        # Preventative: mean=175
        assert 150 <= np.mean(preventative_amounts) <= 200

        # General: mean=280
        assert 230 <= np.mean(general_amounts) <= 330

        # Major: mean=1300
        assert 1100 <= np.mean(major_amounts) <= 1500

    def test_dental_claim_minimum_amounts(self, test_rng: np.random.Generator):
        """Dental claims should respect minimum amounts by type."""
        model = ClaimPropensityModel(test_rng)

        # Sample many claims
        for _ in range(100):
            preventative = model.sample_dental_claim_amount(
                DentalServiceType.PREVENTATIVE
            )
            general = model.sample_dental_claim_amount(DentalServiceType.GENERAL)
            major = model.sample_dental_claim_amount(DentalServiceType.MAJOR)

            assert preventative >= 50
            assert general >= 100
            assert major >= 500


class TestExtrasClaimRate:
    """Tests for extras claim rate calculation."""

    def test_extras_rate_includes_all_services(self, test_rng: np.random.Generator):
        """Extras rate should be sum of all service frequencies."""
        config = ClaimsConfig()
        model = ClaimPropensityModel(test_rng, config=config)

        rate = model.get_extras_claim_rate(age=40)

        # Should include dental (2.0+0.5+0.1), optical (0.8), physio (1.5), chiro (1.2), other (~0.5)
        # Total should be roughly 6.6
        assert 5.0 <= rate <= 8.0

    def test_physio_rate_higher_for_elderly(self, test_rng: np.random.Generator):
        """Physiotherapy rate should be higher for 65+ members."""
        model = ClaimPropensityModel(test_rng)

        rate_40 = model._get_physiotherapy_rate(40)
        rate_70 = model._get_physiotherapy_rate(70)

        # 65+ should have 1.5x multiplier
        assert rate_70 == rate_40 * 1.5


class TestAmbulanceRate:
    """Tests for ambulance claim rate."""

    def test_ambulance_rate_higher_for_elderly(self, test_rng: np.random.Generator):
        """Ambulance rate should be higher for 65+ members."""
        model = ClaimPropensityModel(test_rng)

        rate_40 = model.get_ambulance_claim_rate(40)
        rate_70 = model.get_ambulance_claim_rate(70)

        # 65+ should have 1.5x multiplier
        assert rate_70 == rate_40 * 1.5


class TestConfigIntegration:
    """Tests for configuration integration."""

    def test_uses_custom_config(self, test_rng: np.random.Generator):
        """Model should use custom config values when provided."""
        custom_config = ClaimsConfig(
            hospital_frequency={
                "18-30": 0.1,
                "31-45": 0.2,
                "46-60": 0.3,
                "61-70": 0.4,
                "71+": 0.5,
            }
        )
        model = ClaimPropensityModel(test_rng, config=custom_config)

        assert model.get_hospital_admission_rate(25) == 0.1
        assert model.get_hospital_admission_rate(75) == 0.5

    def test_uses_defaults_without_config(self, test_rng: np.random.Generator):
        """Model should use default APRA values without config."""
        model = ClaimPropensityModel(test_rng)

        # Should use default values
        assert model.get_hospital_admission_rate(25) == 0.3
        assert model.get_hospital_admission_rate(65) == 2.0
