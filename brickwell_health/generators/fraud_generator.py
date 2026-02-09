"""
Fraud claim generator for Brickwell Health Simulator.

Generates fraudulent claims across 9 fraud types, modifying legitimate
claim data to introduce realistic fraud patterns based on published
healthcare fraud research (NHCAA, Health Affairs, APRA).
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, TYPE_CHECKING
from uuid import UUID

import numpy as np

from brickwell_health.config.models import FraudConfig
from brickwell_health.domain.claims import ClaimCreate
from brickwell_health.domain.enums import FraudType, ClaimType
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator

if TYPE_CHECKING:
    from brickwell_health.core.environment import SimulationEnvironment
    from brickwell_health.core.shared_state import SharedState


class FraudGenerator(BaseGenerator[ClaimCreate]):
    """
    Generates fraudulent claim modifications.

    Does NOT generate claims from scratch - modifies existing legitimate
    claim data to inject fraud patterns, or creates standalone fraud
    claims (phantom billing, duplicates).
    """

    # Australian public holidays (month, day) - used for temporal anomaly
    AU_PUBLIC_HOLIDAYS = [
        (1, 1), (1, 26), (4, 25),  # New Year, Australia Day, ANZAC
        (12, 25), (12, 26),          # Christmas, Boxing Day
    ]

    def __init__(
        self,
        rng: np.random.Generator,
        reference: Any,
        id_generator: IDGenerator,
        sim_env: "SimulationEnvironment",
        fraud_config: FraudConfig,
    ):
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.fraud_config = fraud_config
        self._build_fraud_type_weights()

    def _build_fraud_type_weights(self) -> None:
        """Build normalized weights for fraud type selection."""
        self.fraud_types: list[FraudType] = []
        self.fraud_weights: list[float] = []

        type_configs = [
            (FraudType.DRG_UPCODING, self.fraud_config.drg_upcoding),
            (FraudType.EXTRAS_UPCODING, self.fraud_config.extras_upcoding),
            (FraudType.EXACT_DUPLICATE, self.fraud_config.exact_duplicate),
            (FraudType.NEAR_DUPLICATE, self.fraud_config.near_duplicate),
            (FraudType.UNBUNDLING, self.fraud_config.unbundling),
            (FraudType.PHANTOM_BILLING, self.fraud_config.phantom_billing),
            (FraudType.PROVIDER_OUTLIER, self.fraud_config.provider_outlier),
            (FraudType.TEMPORAL_ANOMALY, self.fraud_config.temporal_anomaly),
            (FraudType.GEOGRAPHIC_ANOMALY, self.fraud_config.geographic_anomaly),
        ]

        for fraud_type, config in type_configs:
            if config.enabled:
                self.fraud_types.append(fraud_type)
                self.fraud_weights.append(config.weight)

        # Normalize weights
        total = sum(self.fraud_weights)
        if total > 0:
            self.fraud_weights = [w / total for w in self.fraud_weights]

    def generate(self, **kwargs: Any) -> ClaimCreate:
        """Abstract method implementation - not directly used."""
        raise NotImplementedError("Use specific fraud methods instead")

    def should_apply_fraud(
        self,
        member_id: UUID,
        shared_state: "SharedState",
    ) -> bool:
        """
        Determine if a claim should be fraudulent.

        Base rate is ~6%, boosted 5x for fraud-prone members.
        """
        base_rate = self.fraud_config.fraud_rate

        if shared_state.is_fraud_prone_member(member_id):
            base_rate *= self.fraud_config.fraud_prone_claim_multiplier

        return self.rng.random() < base_rate

    def select_fraud_type(self, claim_type: ClaimType) -> FraudType:
        """
        Select a fraud type, filtered by claim type compatibility.

        DRG upcoding is hospital-only, extras upcoding is extras-only,
        duplicates/phantom/outlier/temporal/geographic are universal.
        """
        compatible = self._get_compatible_fraud_types(claim_type)

        if not compatible:
            return FraudType.PROVIDER_OUTLIER

        types, weights = zip(*compatible)
        weights_arr = np.array(weights, dtype=float)
        weights_arr /= weights_arr.sum()

        idx = self.rng.choice(len(types), p=weights_arr)
        return types[idx]

    def _get_compatible_fraud_types(
        self, claim_type: ClaimType,
    ) -> list[tuple[FraudType, float]]:
        """Get fraud types compatible with a claim type."""
        hospital_only = {FraudType.DRG_UPCODING}
        extras_only = {FraudType.EXTRAS_UPCODING}
        universal = {
            FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
            FraudType.PHANTOM_BILLING, FraudType.PROVIDER_OUTLIER,
            FraudType.TEMPORAL_ANOMALY, FraudType.GEOGRAPHIC_ANOMALY,
        }
        unbundling = {FraudType.UNBUNDLING}

        if claim_type == ClaimType.HOSPITAL:
            allowed = hospital_only | universal | unbundling
        elif claim_type == ClaimType.EXTRAS:
            allowed = extras_only | universal | unbundling
        else:  # AMBULANCE
            allowed = universal

        return [
            (ft, w) for ft, w in zip(self.fraud_types, self.fraud_weights)
            if ft in allowed
        ]

    # =========================================================================
    # Fraud Type Methods
    # =========================================================================

    def apply_drg_upcoding(self, claim: ClaimCreate) -> dict[str, Any]:
        """
        Apply DRG upcoding to a hospital claim.

        Shifts DRG complexity: 40% CC shift (1.3x), 60% MCC shift (1.7x).
        """
        config = self.fraud_config.drg_upcoding
        original_charge = claim.total_charge

        if self.rng.random() < config.cc_shift_probability:
            multiplier = Decimal(str(config.cc_multiplier))
        else:
            multiplier = Decimal(str(config.mcc_multiplier))

        inflated_charge = (original_charge * multiplier).quantize(Decimal("0.01"))
        inflation_amount = inflated_charge - original_charge

        return {
            "is_fraud": True,
            "fraud_type": FraudType.DRG_UPCODING,
            "fraud_original_charge": original_charge,
            "fraud_inflation_amount": inflation_amount,
            "fraud_inflation_ratio": multiplier,
            "total_charge": inflated_charge,
            "total_benefit": (
                (claim.total_benefit * multiplier).quantize(Decimal("0.01"))
                if claim.total_benefit else None
            ),
        }

    def apply_extras_upcoding(self, claim: ClaimCreate) -> dict[str, Any]:
        """
        Apply extras upcoding - inflate charge using lognormal distribution.

        Formula: multiplier = clip(lognormal(mu=0.4, sigma=0.5), 1.2, 2.5)
        """
        config = self.fraud_config.extras_upcoding
        original_charge = claim.total_charge

        raw_multiplier = float(self.rng.lognormal(
            mean=config.inflation_mu, sigma=config.inflation_sigma,
        ))
        multiplier = max(config.inflation_min, min(config.inflation_max, raw_multiplier))
        multiplier_dec = Decimal(str(round(multiplier, 3)))

        inflated_charge = (original_charge * multiplier_dec).quantize(Decimal("0.01"))
        inflation_amount = inflated_charge - original_charge

        return {
            "is_fraud": True,
            "fraud_type": FraudType.EXTRAS_UPCODING,
            "fraud_original_charge": original_charge,
            "fraud_inflation_amount": inflation_amount,
            "fraud_inflation_ratio": multiplier_dec,
            "total_charge": inflated_charge,
            "total_benefit": (
                (claim.total_benefit * multiplier_dec).quantize(Decimal("0.01"))
                if claim.total_benefit else None
            ),
        }

    def generate_exact_duplicate(
        self,
        source_claim: dict[str, Any],
        current_date: date,
    ) -> dict[str, Any]:
        """
        Generate an exact duplicate of a source claim.

        Same amounts, new claim_id, delayed lodgement (7-30 days).
        """
        config = self.fraud_config.exact_duplicate
        delay = int(self.rng.integers(config.delay_days_min, config.delay_days_max + 1))
        lodgement_date = current_date + timedelta(days=delay)

        return {
            "is_fraud": True,
            "fraud_type": FraudType.EXACT_DUPLICATE,
            "fraud_source_claim_id": source_claim["claim_id"],
            "fraud_original_charge": source_claim["total_charge"],
            "fraud_inflation_amount": Decimal("0"),
            "fraud_inflation_ratio": Decimal("1.000"),
            "policy_id": source_claim["policy_id"],
            "member_id": source_claim["member_id"],
            "coverage_id": source_claim["coverage_id"],
            "claim_type": source_claim["claim_type"],
            "service_date": source_claim["service_date"],
            "lodgement_date": lodgement_date,
            "total_charge": source_claim["total_charge"],
            "provider_id": source_claim.get("provider_id"),
            "hospital_id": source_claim.get("hospital_id"),
            "claim_channel": source_claim.get("claim_channel", "Online"),
        }

    def generate_near_duplicate(
        self,
        source_claim: dict[str, Any],
        current_date: date,
    ) -> dict[str, Any]:
        """
        Generate a near-duplicate with slight variations.

        +/-5% amount, +/-7 day service date shift, 15-60 day delay.
        """
        config = self.fraud_config.near_duplicate
        original_charge = Decimal(str(source_claim["total_charge"]))

        variation = float(self.rng.uniform(
            -config.amount_variation_pct, config.amount_variation_pct,
        ))
        modified_charge = (original_charge * Decimal(str(1 + variation))).quantize(
            Decimal("0.01"),
        )

        date_shift = int(self.rng.integers(
            -config.date_shift_days, config.date_shift_days + 1,
        ))
        modified_service_date = source_claim["service_date"] + timedelta(days=date_shift)

        delay = int(self.rng.integers(config.delay_days_min, config.delay_days_max + 1))
        lodgement_date = current_date + timedelta(days=delay)

        inflation_amount = modified_charge - original_charge

        return {
            "is_fraud": True,
            "fraud_type": FraudType.NEAR_DUPLICATE,
            "fraud_source_claim_id": source_claim["claim_id"],
            "fraud_original_charge": original_charge,
            "fraud_inflation_amount": inflation_amount,
            "fraud_inflation_ratio": Decimal(str(round(1 + variation, 3))),
            "policy_id": source_claim["policy_id"],
            "member_id": source_claim["member_id"],
            "coverage_id": source_claim["coverage_id"],
            "claim_type": source_claim["claim_type"],
            "service_date": modified_service_date,
            "lodgement_date": lodgement_date,
            "total_charge": modified_charge,
            "provider_id": source_claim.get("provider_id"),
            "hospital_id": source_claim.get("hospital_id"),
            "claim_channel": source_claim.get("claim_channel", "Online"),
        }

    def apply_phantom_billing(
        self,
        claim: ClaimCreate,
        shared_state: "SharedState",
    ) -> dict[str, Any]:
        """
        Mark a claim as phantom billing (service never rendered).

        30% chance of being part of a fraud ring.
        """
        config = self.fraud_config.phantom_billing
        result: dict[str, Any] = {
            "is_fraud": True,
            "fraud_type": FraudType.PHANTOM_BILLING,
            "fraud_original_charge": Decimal("0"),
            "fraud_inflation_amount": claim.total_charge,
            "fraud_inflation_ratio": None,
        }

        if self.rng.random() < config.fraud_ring_probability:
            existing_rings = [
                ring_id for ring_id, members in shared_state.fraud_rings.items()
                if claim.member_id in members
            ]
            if existing_rings:
                result["fraud_ring_id"] = existing_rings[0]
            else:
                ring_id = self.id_generator.generate_uuid()
                shared_state.fraud_rings[ring_id] = [claim.member_id]
                result["fraud_ring_id"] = ring_id

        return result

    def apply_provider_outlier(
        self,
        claim: ClaimCreate,
    ) -> dict[str, Any]:
        """
        Apply provider outlier inflation.

        Shifts claim amount upward by 1.3x-1.7x.
        """
        config = self.fraud_config.provider_outlier
        original_charge = claim.total_charge

        shift = float(self.rng.uniform(
            config.amount_shift_min, config.amount_shift_max,
        ))
        multiplier = Decimal(str(round(1 + shift, 3)))
        inflated_charge = (original_charge * multiplier).quantize(Decimal("0.01"))

        return {
            "is_fraud": True,
            "fraud_type": FraudType.PROVIDER_OUTLIER,
            "fraud_original_charge": original_charge,
            "fraud_inflation_amount": inflated_charge - original_charge,
            "fraud_inflation_ratio": multiplier,
            "total_charge": inflated_charge,
            "total_benefit": (
                (claim.total_benefit * multiplier).quantize(Decimal("0.01"))
                if claim.total_benefit else None
            ),
        }

    def apply_temporal_anomaly(self, claim: ClaimCreate) -> dict[str, Any]:
        """Shift service date to a weekend or public holiday."""
        service_date = claim.service_date

        if self.rng.random() < 0.7:  # 70% weekend
            days_to_saturday = (5 - service_date.weekday()) % 7
            if days_to_saturday == 0:
                days_to_saturday = 7
            anomaly_date = service_date + timedelta(days=days_to_saturday)
        else:
            month, day = self.choice(self.AU_PUBLIC_HOLIDAYS)
            anomaly_date = date(service_date.year, month, day)

        return {
            "is_fraud": True,
            "fraud_type": FraudType.TEMPORAL_ANOMALY,
            "fraud_original_charge": claim.total_charge,
            "fraud_inflation_amount": Decimal("0"),
            "fraud_inflation_ratio": Decimal("1.000"),
            "service_date": anomaly_date,
        }

    def apply_geographic_anomaly(
        self,
        claim: ClaimCreate,
        member_state: str,
    ) -> dict[str, Any]:
        """Assign a provider from a different state than the member."""
        all_states = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
        other_states = [s for s in all_states if s != member_state]

        if not other_states:
            other_states = ["VIC"]

        target_state = self.choice(other_states)

        providers = self.reference.get_providers_by_type_and_state(
            state=target_state, active_only=True,
        )

        new_provider_id = None
        if providers:
            provider = self.rng.choice(providers)
            new_provider_id = provider.get("provider_id")

        return {
            "is_fraud": True,
            "fraud_type": FraudType.GEOGRAPHIC_ANOMALY,
            "fraud_original_charge": claim.total_charge,
            "fraud_inflation_amount": Decimal("0"),
            "fraud_inflation_ratio": Decimal("1.000"),
            "provider_id": new_provider_id,
        }

    def generate_unbundled_claims(
        self,
        original_charge: Decimal,
        num_fragments: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Split a single service into multiple fragments with inflation.

        Returns list of fragment dicts with fractional charges (sum > original).
        """
        config = self.fraud_config.unbundling

        if num_fragments is None:
            num_fragments = int(self.rng.integers(
                config.fragment_count_min, config.fragment_count_max + 1,
            ))

        inflation_factor = 1.0 + float(self.rng.uniform(0.20, config.inflation_pct))
        total_inflated = (
            original_charge * Decimal(str(inflation_factor))
        ).quantize(Decimal("0.01"))

        raw_splits = self.rng.dirichlet(np.ones(num_fragments))
        fragment_charges = [
            (total_inflated * Decimal(str(round(s, 4)))).quantize(Decimal("0.01"))
            for s in raw_splits
        ]

        # Adjust last fragment to match total exactly
        diff = total_inflated - sum(fragment_charges)
        fragment_charges[-1] += diff

        fragments = []
        for i, charge in enumerate(fragment_charges):
            fragments.append({
                "fragment_index": i,
                "charge_amount": charge,
                "is_fraud": True,
                "fraud_type": FraudType.UNBUNDLING,
                "fraud_original_charge": original_charge,
                "fraud_inflation_amount": total_inflated - original_charge,
                "fraud_inflation_ratio": Decimal(str(round(inflation_factor, 3))),
            })

        return fragments
