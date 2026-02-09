"""
Integration tests for fraud pipeline.

Tests that fraud configuration, domain models, and enum conversions
work correctly end-to-end.
"""

from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from brickwell_health.config.models import (
    FraudConfig,
    ScaleConfig,
    SimulationConfig,
    SimulationTimeConfig,
)
from brickwell_health.domain.claims import ClaimCreate
from brickwell_health.domain.enums import (
    ClaimChannel,
    ClaimStatus,
    ClaimType,
    FraudType,
)


# =============================================================================
# ClaimCreate Fraud Field Tests
# =============================================================================


class TestClaimCreateFraudFields:
    """Test fraud fields on ClaimCreate domain model."""

    def test_default_not_fraud(self):
        """Claims default to is_fraud=False with null fraud fields."""
        claim = ClaimCreate(
            claim_id=uuid4(),
            claim_number="CLM-2024-00000001",
            policy_id=uuid4(),
            member_id=uuid4(),
            coverage_id=uuid4(),
            claim_type=ClaimType.EXTRAS,
            service_date=date(2024, 6, 1),
            lodgement_date=date(2024, 6, 1),
            total_charge=Decimal("100.00"),
            claim_channel=ClaimChannel.ONLINE,
        )
        assert claim.is_fraud is False
        assert claim.fraud_type is None
        assert claim.fraud_original_charge is None
        assert claim.fraud_inflation_amount is None
        assert claim.fraud_inflation_ratio is None
        assert claim.fraud_source_claim_id is None
        assert claim.fraud_ring_id is None

    def test_fraud_claim_creation(self):
        """Can create a claim with all fraud fields populated."""
        source_id = uuid4()
        ring_id = uuid4()
        claim = ClaimCreate(
            claim_id=uuid4(),
            claim_number="CLM-2024-00000002",
            policy_id=uuid4(),
            member_id=uuid4(),
            coverage_id=uuid4(),
            claim_type=ClaimType.HOSPITAL,
            service_date=date(2024, 6, 1),
            lodgement_date=date(2024, 6, 1),
            total_charge=Decimal("6500.00"),
            claim_channel=ClaimChannel.HOSPITAL,
            is_fraud=True,
            fraud_type=FraudType.DRG_UPCODING,
            fraud_original_charge=Decimal("5000.00"),
            fraud_inflation_amount=Decimal("1500.00"),
            fraud_inflation_ratio=Decimal("1.300"),
            fraud_source_claim_id=source_id,
            fraud_ring_id=ring_id,
        )
        assert claim.is_fraud is True
        assert claim.fraud_type == FraudType.DRG_UPCODING
        assert claim.fraud_original_charge == Decimal("5000.00")
        assert claim.fraud_inflation_amount == Decimal("1500.00")
        assert claim.fraud_inflation_ratio == Decimal("1.300")
        assert claim.fraud_source_claim_id == source_id
        assert claim.fraud_ring_id == ring_id


# =============================================================================
# model_dump_db Tests
# =============================================================================


class TestModelDumpDB:
    """Test FraudType enum conversion in model_dump_db."""

    def test_fraud_type_converted_to_string(self):
        """model_dump_db converts FraudType enum to string value."""
        claim = ClaimCreate(
            claim_id=uuid4(),
            claim_number="CLM-2024-00000003",
            policy_id=uuid4(),
            member_id=uuid4(),
            coverage_id=uuid4(),
            claim_type=ClaimType.EXTRAS,
            service_date=date(2024, 6, 1),
            lodgement_date=date(2024, 6, 1),
            total_charge=Decimal("300.00"),
            claim_channel=ClaimChannel.ONLINE,
            is_fraud=True,
            fraud_type=FraudType.EXTRAS_UPCODING,
        )
        db_data = claim.model_dump_db()
        assert db_data["fraud_type"] == "ExtrasUpcoding"
        assert isinstance(db_data["fraud_type"], str)

    def test_null_fraud_type_stays_null(self):
        """model_dump_db preserves None fraud_type."""
        claim = ClaimCreate(
            claim_id=uuid4(),
            claim_number="CLM-2024-00000004",
            policy_id=uuid4(),
            member_id=uuid4(),
            coverage_id=uuid4(),
            claim_type=ClaimType.EXTRAS,
            service_date=date(2024, 6, 1),
            lodgement_date=date(2024, 6, 1),
            total_charge=Decimal("100.00"),
            claim_channel=ClaimChannel.ONLINE,
        )
        db_data = claim.model_dump_db()
        assert db_data["fraud_type"] is None

    def test_legitimate_claim_clean(self):
        """Non-fraud claims have is_fraud=False and null fraud fields in DB dump."""
        claim = ClaimCreate(
            claim_id=uuid4(),
            claim_number="CLM-2024-00000005",
            policy_id=uuid4(),
            member_id=uuid4(),
            coverage_id=uuid4(),
            claim_type=ClaimType.AMBULANCE,
            service_date=date(2024, 6, 1),
            lodgement_date=date(2024, 6, 1),
            total_charge=Decimal("800.00"),
            claim_channel=ClaimChannel.ONLINE,
        )
        db_data = claim.model_dump_db()
        assert db_data["is_fraud"] is False
        assert db_data["fraud_type"] is None
        assert db_data["fraud_original_charge"] is None
        assert db_data["fraud_source_claim_id"] is None
        assert db_data["fraud_ring_id"] is None


# =============================================================================
# FraudConfig in SimulationConfig Tests
# =============================================================================


class TestFraudConfigInSimulation:
    """Test fraud config integrates with SimulationConfig."""

    def test_default_simulation_config_has_fraud(self):
        """SimulationConfig includes FraudConfig by default."""
        config = SimulationConfig(
            simulation=SimulationTimeConfig(
                start_date=date(2024, 1, 1), end_date=date(2024, 12, 31),
            ),
            scale=ScaleConfig(target_member_count=1000),
        )
        assert hasattr(config, "fraud")
        assert isinstance(config.fraud, FraudConfig)

    def test_fraud_disabled_by_default_in_simulation(self):
        """Fraud is disabled by default in SimulationConfig."""
        config = SimulationConfig(
            simulation=SimulationTimeConfig(
                start_date=date(2024, 1, 1), end_date=date(2024, 12, 31),
            ),
            scale=ScaleConfig(target_member_count=1000),
        )
        assert config.fraud.enabled is False

    def test_fraud_type_enum_all_values(self):
        """All FraudType enum values can be round-tripped."""
        for ft in FraudType:
            assert FraudType(ft.value) == ft

    def test_claim_model_copy_with_fraud(self):
        """model_copy can overlay fraud fields on a clean claim."""
        claim = ClaimCreate(
            claim_id=uuid4(),
            claim_number="CLM-2024-00000006",
            policy_id=uuid4(),
            member_id=uuid4(),
            coverage_id=uuid4(),
            claim_type=ClaimType.EXTRAS,
            service_date=date(2024, 6, 1),
            lodgement_date=date(2024, 6, 1),
            total_charge=Decimal("250.00"),
            total_benefit=Decimal("175.00"),
            claim_channel=ClaimChannel.ONLINE,
        )

        fraud_claim = claim.model_copy(update={
            "is_fraud": True,
            "fraud_type": FraudType.UNBUNDLING,
            "total_charge": Decimal("150.00"),
            "fraud_original_charge": Decimal("250.00"),
            "fraud_inflation_amount": Decimal("50.00"),
            "fraud_inflation_ratio": Decimal("1.200"),
        })

        # Original unchanged
        assert claim.is_fraud is False
        assert claim.total_charge == Decimal("250.00")

        # Copy has fraud overlay
        assert fraud_claim.is_fraud is True
        assert fraud_claim.fraud_type == FraudType.UNBUNDLING
        assert fraud_claim.total_charge == Decimal("150.00")
        assert fraud_claim.fraud_original_charge == Decimal("250.00")
        # Unchanged fields preserved
        assert fraud_claim.policy_id == claim.policy_id
        assert fraud_claim.total_benefit == Decimal("175.00")
