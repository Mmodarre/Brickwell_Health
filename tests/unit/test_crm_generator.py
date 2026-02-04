"""
Unit tests for CRM Domain Generators.

Tests for InteractionGenerator, CaseGenerator, ComplaintGenerator,
EventTriggerEngine, and CRM domain models.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.core.trigger_engine import EventTriggerEngine
from brickwell_health.domain.crm import (
    InteractionCreate,
    CaseCreate,
    ComplaintCreate,
)
from brickwell_health.domain.enums import (
    CasePriority,
    CaseStatus,
    ComplaintSeverity,
    ComplaintSource,
    ComplaintStatus,
    InteractionChannel,
    InteractionDirection,
    TriggerEventType,
)
from brickwell_health.generators.crm_generator import (
    InteractionGenerator,
    CaseGenerator,
    ComplaintGenerator,
)
from brickwell_health.generators.id_generator import IDGenerator


# =============================================================================
# TRIGGER ENGINE TESTS
# =============================================================================


class TestEventTriggerEngine:
    """Tests for EventTriggerEngine."""

    def test_trigger_engine_initialization(self, test_rng):
        """Test trigger engine initializes with default probabilities."""
        engine = EventTriggerEngine(test_rng)

        assert "claim_rejected" in engine.triggers
        assert "payment_failed" in engine.triggers
        assert "claim_submitted" in engine.triggers

    def test_get_triggered_events_claim_rejected(self, test_rng):
        """Test triggered events for claim_rejected with high charge."""
        engine = EventTriggerEngine(test_rng)

        context = {"charge_amount": 1500}  # High value
        triggered = engine.get_triggered_events("claim_rejected", context)

        # At least one of these should be triggered given high probabilities
        assert isinstance(triggered, list)
        # Verify possible outputs
        valid_targets = ["interaction", "case", "complaint", "communication", "nps_survey"]
        for t in triggered:
            assert t in valid_targets

    def test_get_triggered_events_payment_failed(self, test_rng):
        """Test triggered events for payment_failed."""
        engine = EventTriggerEngine(test_rng)

        context = {"attempt_number": 3}  # 3rd attempt
        triggered = engine.get_triggered_events("payment_failed", context)

        # Case should be triggered on 3rd attempt
        assert isinstance(triggered, list)

    def test_get_triggered_events_unknown_type(self, test_rng):
        """Test that unknown event types return empty list."""
        engine = EventTriggerEngine(test_rng)

        triggered = engine.get_triggered_events("unknown_event", {})

        assert triggered == []

    def test_get_interaction_type_for_trigger(self, test_rng):
        """Test interaction type mapping for triggers."""
        engine = EventTriggerEngine(test_rng)

        assert engine.get_interaction_type_for_trigger("claim_rejected") == "CLAIM_DISPUTE"
        assert engine.get_interaction_type_for_trigger("payment_failed") == "BILLING_INQUIRY"
        assert engine.get_interaction_type_for_trigger("unknown") == "GENERAL_INQUIRY"

    def test_get_case_type_for_trigger(self, test_rng):
        """Test case type mapping for triggers."""
        engine = EventTriggerEngine(test_rng)

        assert engine.get_case_type_for_trigger("claim_rejected") == "CLAIM_DISPUTE"
        assert engine.get_case_type_for_trigger("payment_failed") == "PAYMENT_ISSUE"
        assert engine.get_case_type_for_trigger("unknown") == "GENERAL"

    def test_get_complaint_category_for_trigger(self, test_rng):
        """Test complaint category mapping for triggers."""
        engine = EventTriggerEngine(test_rng)

        assert engine.get_complaint_category_for_trigger("claim_rejected") == "CLAIM_DENIAL"
        assert engine.get_complaint_category_for_trigger("payment_failed") == "BILLING_ERROR"
        assert engine.get_complaint_category_for_trigger("unknown") == "OTHER"

    def test_should_trigger_method(self, test_rng):
        """Test the should_trigger convenience method."""
        engine = EventTriggerEngine(test_rng)

        # Test many times to ensure probability works
        results = [
            engine.should_trigger("claim_submitted", "communication", {})
            for _ in range(10)
        ]

        # Communication for claim_submitted has 100% probability, so should always be True
        assert all(results)

    def test_conditional_probability_high_value(self, test_rng):
        """Test conditional probability for high-value claims."""
        engine = EventTriggerEngine(test_rng)

        # Low value - should not trigger case
        low_context = {"charge_amount": 100}
        low_prob = engine._case_on_high_value(low_context)
        assert low_prob == 0.0

        # High value - should have positive probability
        high_context = {"charge_amount": 1000}
        high_prob = engine._case_on_high_value(high_context)
        assert high_prob > 0.0


# =============================================================================
# DOMAIN MODEL TESTS
# =============================================================================


class TestInteractionCreate:
    """Tests for InteractionCreate domain model."""

    def test_interaction_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        interaction = InteractionCreate(
            interaction_id=uuid4(),
            interaction_reference="INT-W0-2024-000001",
            policy_id=uuid4(),
            member_id=uuid4(),
            interaction_type_id=1,
            channel=InteractionChannel.PHONE,
            direction=InteractionDirection.INBOUND,
            start_datetime=datetime.now(),
        )

        data = interaction.model_dump_db()

        assert data["channel"] == "Phone"
        assert data["direction"] == "Inbound"
        assert data["trigger_event_type"] is None

    def test_interaction_with_trigger(self):
        """Test interaction with trigger event."""
        interaction = InteractionCreate(
            interaction_id=uuid4(),
            interaction_reference="INT-W0-2024-000002",
            policy_id=uuid4(),
            member_id=uuid4(),
            interaction_type_id=1,
            channel=InteractionChannel.EMAIL,
            direction=InteractionDirection.OUTBOUND,
            start_datetime=datetime.now(),
            trigger_event_type=TriggerEventType.CLAIM_REJECTED,
            trigger_event_id=uuid4(),
        )

        data = interaction.model_dump_db()

        assert data["channel"] == "Email"
        assert data["direction"] == "Outbound"
        assert data["trigger_event_type"] == "ClaimRejected"


class TestCaseCreate:
    """Tests for CaseCreate domain model."""

    def test_case_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        case = CaseCreate(
            case_id=uuid4(),
            case_number="CASE-W0-2024-000001",
            case_type_id=1,
            policy_id=uuid4(),
            member_id=uuid4(),
            subject="Test Case",
            priority=CasePriority.HIGH,
            status=CaseStatus.OPEN,
        )

        data = case.model_dump_db()

        assert data["priority"] == "High"
        assert data["status"] == "Open"

    def test_case_with_sla(self):
        """Test case with SLA information."""
        due_date = date.today() + timedelta(days=3)
        case = CaseCreate(
            case_id=uuid4(),
            case_number="CASE-W0-2024-000002",
            case_type_id=2,
            policy_id=uuid4(),
            member_id=uuid4(),
            subject="Urgent Case",
            priority=CasePriority.CRITICAL,
            status=CaseStatus.IN_PROGRESS,
            due_date=due_date,
        )

        assert case.due_date == due_date
        assert case.sla_breached is False


class TestComplaintCreate:
    """Tests for ComplaintCreate domain model."""

    def test_complaint_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        complaint = ComplaintCreate(
            complaint_id=uuid4(),
            complaint_number="COMP-W0-2024-000001",
            policy_id=uuid4(),
            member_id=uuid4(),
            complaint_category_id=1,
            subject="Test Complaint",
            severity=ComplaintSeverity.HIGH,
            status=ComplaintStatus.RECEIVED,
            source=ComplaintSource.PHONE,
            received_date=date.today(),
            due_date=date.today() + timedelta(days=21),
        )

        data = complaint.model_dump_db()

        assert data["severity"] == "High"
        assert data["status"] == "Received"
        assert data["source"] == "Phone"

    def test_complaint_with_phio(self):
        """Test complaint with PHIO escalation."""
        complaint = ComplaintCreate(
            complaint_id=uuid4(),
            complaint_number="COMP-W0-2024-000002",
            policy_id=uuid4(),
            member_id=uuid4(),
            complaint_category_id=2,
            subject="PHIO Escalation",
            severity=ComplaintSeverity.CRITICAL,
            status=ComplaintStatus.UNDER_REVIEW,
            source=ComplaintSource.PHIO,
            received_date=date.today(),
            due_date=date.today() + timedelta(days=14),
            phio_escalated=True,
            phio_reference="PHIO-2024-12345",
        )

        assert complaint.phio_escalated is True
        assert complaint.phio_reference == "PHIO-2024-12345"


# =============================================================================
# INTERACTION GENERATOR TESTS
# =============================================================================


class TestInteractionGenerator:
    """Tests for InteractionGenerator."""

    @pytest.fixture
    def interaction_generator(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Create an interaction generator for testing."""
        return InteractionGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

    def test_generate_basic_interaction(self, interaction_generator):
        """Test generating a basic interaction."""
        policy_id = uuid4()
        member_id = uuid4()

        interaction = interaction_generator.generate(
            policy_id=policy_id,
            member_id=member_id,
            interaction_type_code="GENERAL_INQUIRY",
        )

        assert interaction.policy_id == policy_id
        assert interaction.member_id == member_id
        assert interaction.interaction_id is not None
        assert interaction.interaction_reference.startswith("INT-")
        assert interaction.channel in InteractionChannel
        assert interaction.direction == InteractionDirection.INBOUND

    def test_generate_interaction_with_trigger(self, interaction_generator):
        """Test generating an interaction with trigger event."""
        policy_id = uuid4()
        member_id = uuid4()
        claim_id = uuid4()

        interaction = interaction_generator.generate(
            policy_id=policy_id,
            member_id=member_id,
            interaction_type_code="CLAIM_DISPUTE",
            trigger_event_type=TriggerEventType.CLAIM_REJECTED,
            trigger_event_id=claim_id,
            claim_id=claim_id,
        )

        assert interaction.trigger_event_type == TriggerEventType.CLAIM_REJECTED
        assert interaction.trigger_event_id == claim_id
        assert interaction.claim_id == claim_id

    def test_generate_interaction_with_specific_channel(self, interaction_generator):
        """Test generating an interaction with specific channel."""
        interaction = interaction_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            interaction_type_code="BILLING_INQUIRY",
            channel=InteractionChannel.EMAIL,
        )

        assert interaction.channel == InteractionChannel.EMAIL

    def test_interaction_duration_is_positive(self, interaction_generator):
        """Test that interaction duration is positive."""
        interaction = interaction_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            interaction_type_code="GENERAL_INQUIRY",
        )

        assert interaction.duration_seconds > 0
        assert interaction.duration_seconds <= 3600  # Max 1 hour


# =============================================================================
# CASE GENERATOR TESTS
# =============================================================================


class TestCaseGenerator:
    """Tests for CaseGenerator."""

    @pytest.fixture
    def case_generator(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Create a case generator for testing."""
        return CaseGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

    def test_generate_basic_case(self, case_generator):
        """Test generating a basic case."""
        policy_id = uuid4()
        member_id = uuid4()

        case = case_generator.generate(
            policy_id=policy_id,
            member_id=member_id,
            case_type_code="GENERAL",
        )

        assert case.policy_id == policy_id
        assert case.member_id == member_id
        assert case.case_id is not None
        assert case.case_number.startswith("CASE-")
        assert case.status == CaseStatus.OPEN
        assert case.due_date is not None

    def test_generate_case_with_priority_override(self, case_generator):
        """Test generating a case with priority override."""
        case = case_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            case_type_code="CLAIM_DISPUTE",
            priority_override=CasePriority.CRITICAL,
        )

        assert case.priority == CasePriority.CRITICAL

    def test_generate_case_with_related_claim(self, case_generator):
        """Test generating a case with related claim."""
        claim_id = uuid4()

        case = case_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            case_type_code="CLAIM_DISPUTE",
            related_claim_id=claim_id,
        )

        assert case.related_claim_id == claim_id


# =============================================================================
# COMPLAINT GENERATOR TESTS
# =============================================================================


class TestComplaintGenerator:
    """Tests for ComplaintGenerator."""

    @pytest.fixture
    def complaint_generator(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Create a complaint generator for testing."""
        return ComplaintGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

    def test_generate_basic_complaint(self, complaint_generator):
        """Test generating a basic complaint."""
        policy_id = uuid4()
        member_id = uuid4()

        complaint = complaint_generator.generate(
            policy_id=policy_id,
            member_id=member_id,
            category_code="OTHER",
        )

        assert complaint.policy_id == policy_id
        assert complaint.member_id == member_id
        assert complaint.complaint_id is not None
        assert complaint.complaint_number.startswith("COMP-")
        assert complaint.status == ComplaintStatus.RECEIVED
        assert complaint.due_date is not None
        assert complaint.received_date is not None

    def test_high_value_complaint_severity(self, complaint_generator):
        """Test that high value complaints get high severity."""
        complaint = complaint_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            category_code="CLAIM_DENIAL",
            charge_amount=Decimal("5000"),
        )

        # High charge amount should result in high severity
        assert complaint.severity in [ComplaintSeverity.HIGH, ComplaintSeverity.CRITICAL]

    def test_complaint_with_specific_source(self, complaint_generator):
        """Test generating a complaint with specific source."""
        complaint = complaint_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            category_code="OTHER",
            source=ComplaintSource.EMAIL,
        )

        assert complaint.source == ComplaintSource.EMAIL

    def test_privacy_breach_always_high_severity(self, complaint_generator):
        """Test that privacy breach complaints are always high severity."""
        complaint = complaint_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            category_code="PRIVACY_BREACH",
        )

        assert complaint.severity == ComplaintSeverity.HIGH
