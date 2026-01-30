"""
Unit tests for ClaimsGenerator lifecycle transitions.

Tests verify that claims are created with correct initial status for CDC/SCD
support (SUBMITTED for approved claims, REJECTED for deterministic denials).
"""

from datetime import date
from decimal import Decimal

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.domain.enums import ClaimStatus, ClaimType, DenialReason
from brickwell_health.generators.claims_generator import ClaimsGenerator
from brickwell_health.generators.id_generator import IDGenerator


@pytest.fixture
def claims_generator(
    test_rng: np.random.Generator,
    test_reference,
    id_generator: IDGenerator,
    sim_env: SimulationEnvironment,
    test_config,
):
    """Create a claims generator for testing."""
    return ClaimsGenerator(
        rng=test_rng,
        reference=test_reference,
        id_generator=id_generator,
        sim_env=sim_env,
        config=test_config.claims,
    )


@pytest.fixture
def sample_policy():
    """Create a sample policy for testing."""
    from uuid import uuid4
    from brickwell_health.domain.policy import PolicyCreate

    return PolicyCreate(
        policy_id=uuid4(),
        policy_number="POL-2024-000001",
        product_id=1,
        policy_status="Active",
        policy_type="Single",
        effective_date=date(2024, 1, 1),
        payment_frequency="Monthly",
        premium_amount=Decimal("200.00"),
        excess_amount=Decimal("500.00"),
        distribution_channel="Online",
        state_of_residence="NSW",
        original_join_date=date(2024, 1, 1),
    )


@pytest.fixture
def sample_member():
    """Create a sample member for testing."""
    from uuid import uuid4
    from brickwell_health.domain.member import MemberCreate

    return MemberCreate(
        member_id=uuid4(),
        member_number="MEM-2024-000001",
        first_name="John",
        last_name="Smith",
        date_of_birth=date(1985, 6, 15),
        gender="Male",
        state="NSW",
    )


@pytest.fixture
def sample_coverage(sample_policy):
    """Create a sample coverage for testing."""
    from uuid import uuid4
    from brickwell_health.domain.coverage import CoverageCreate

    return CoverageCreate(
        coverage_id=uuid4(),
        policy_id=sample_policy.policy_id,
        coverage_type="Extras",
        product_id=3,
        effective_date=date(2024, 1, 1),
        status="Active",
    )


class TestExtrasClaimLifecycle:
    """Tests for extras claim lifecycle status."""

    def test_extras_claim_created_as_submitted(
        self,
        claims_generator: ClaimsGenerator,
        sample_policy,
        sample_member,
        sample_coverage,
    ):
        """Extras claims should be created with SUBMITTED status for lifecycle transitions."""
        claim, claim_line, extras_claim = claims_generator.generate_extras_claim(
            policy=sample_policy,
            member=sample_member,
            coverage=sample_coverage,
            service_date=date(2024, 6, 15),
        )

        assert claim.claim_status == ClaimStatus.SUBMITTED, (
            f"Expected SUBMITTED, got {claim.claim_status}"
        )

    def test_extras_claim_dates_are_none(
        self,
        claims_generator: ClaimsGenerator,
        sample_policy,
        sample_member,
        sample_coverage,
    ):
        """Extras claims should have None assessment_date and payment_date initially."""
        claim, claim_line, extras_claim = claims_generator.generate_extras_claim(
            policy=sample_policy,
            member=sample_member,
            coverage=sample_coverage,
            service_date=date(2024, 6, 15),
        )

        assert claim.assessment_date is None, (
            f"Expected None assessment_date, got {claim.assessment_date}"
        )
        assert claim.payment_date is None, (
            f"Expected None payment_date, got {claim.payment_date}"
        )

    def test_claim_line_status_is_pending(
        self,
        claims_generator: ClaimsGenerator,
        sample_policy,
        sample_member,
        sample_coverage,
    ):
        """Claim lines should be created with 'Pending' status."""
        claim, claim_line, extras_claim = claims_generator.generate_extras_claim(
            policy=sample_policy,
            member=sample_member,
            coverage=sample_coverage,
            service_date=date(2024, 6, 15),
        )

        assert claim_line.line_status == "Pending", (
            f"Expected 'Pending', got '{claim_line.line_status}'"
        )


class TestHospitalClaimLifecycle:
    """Tests for hospital claim lifecycle status."""

    def test_hospital_claim_created_as_submitted(
        self,
        claims_generator: ClaimsGenerator,
        sample_policy,
        sample_member,
    ):
        """Hospital claims should be created with SUBMITTED status."""
        # Create hospital coverage
        from uuid import uuid4
        from brickwell_health.domain.coverage import CoverageCreate

        hospital_coverage = CoverageCreate(
            coverage_id=uuid4(),
            policy_id=sample_policy.policy_id,
            coverage_type="Hospital",
            product_id=1,
            effective_date=date(2024, 1, 1),
            status="Active",
            tier="Gold",
            excess_amount=Decimal("500.00"),
        )

        claim, claim_lines, admission, prosthesis, medical = claims_generator.generate_hospital_claim(
            policy=sample_policy,
            member=sample_member,
            coverage=hospital_coverage,
            admission_date=date(2024, 6, 15),
            age=39,
            gender="Male",
        )

        assert claim.claim_status == ClaimStatus.SUBMITTED, (
            f"Expected SUBMITTED, got {claim.claim_status}"
        )


class TestAmbulanceClaimLifecycle:
    """Tests for ambulance claim lifecycle status."""

    def test_ambulance_claim_created_as_submitted(
        self,
        claims_generator: ClaimsGenerator,
        sample_policy,
        sample_member,
    ):
        """Ambulance claims should be created with SUBMITTED status."""
        from uuid import uuid4
        from brickwell_health.domain.coverage import CoverageCreate

        ambulance_coverage = CoverageCreate(
            coverage_id=uuid4(),
            policy_id=sample_policy.policy_id,
            coverage_type="Ambulance",
            product_id=4,
            effective_date=date(2024, 1, 1),
            status="Active",
        )

        claim, ambulance = claims_generator.generate_ambulance_claim(
            policy=sample_policy,
            member=sample_member,
            coverage=ambulance_coverage,
            incident_date=date(2024, 6, 15),
        )

        assert claim.claim_status == ClaimStatus.SUBMITTED, (
            f"Expected SUBMITTED, got {claim.claim_status}"
        )


class TestRejectedClaimLifecycle:
    """Tests for deterministic rejection claims."""

    def test_rejected_claim_is_immediate(
        self,
        claims_generator: ClaimsGenerator,
        sample_policy,
        sample_member,
    ):
        """Deterministic rejections should be created with REJECTED status immediately."""
        claim = claims_generator.generate_rejected_claim(
            policy=sample_policy,
            member=sample_member,
            claim_type=ClaimType.EXTRAS,
            service_date=date(2024, 6, 15),
            denial_reason=DenialReason.NO_COVERAGE,
        )

        assert claim.claim_status == ClaimStatus.REJECTED, (
            f"Expected REJECTED, got {claim.claim_status}"
        )
        assert claim.rejection_reason_id is not None, (
            "Expected rejection_reason_id to be set"
        )
        # Deterministic rejections have assessment_date set (assessed immediately)
        assert claim.assessment_date is not None, (
            "Expected assessment_date to be set for deterministic rejections"
        )
