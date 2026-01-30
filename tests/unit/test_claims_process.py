"""
Unit tests for ClaimsProcess lifecycle transitions.

Tests verify that claims are properly scheduled for lifecycle transitions
and that transitions occur at the correct times.
"""

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.core.shared_state import SharedState
from brickwell_health.domain.enums import ClaimStatus, ClaimType, DenialReason


class MockBatchWriter:
    """Mock batch writer for testing."""

    def __init__(self):
        self.records = {}
        self.updates = []
        self.flush_count = 0
        self.cdc_flush_log = []  # Track CDC flushes for verification

    def add(self, table_name: str, record: dict) -> None:
        if table_name not in self.records:
            self.records[table_name] = []
        self.records[table_name].append(record)

    def update_record(self, table_name: str, key_field: str, key_value, updates: dict) -> bool:
        self.updates.append({
            "table_name": table_name,
            "key_field": key_field,
            "key_value": key_value,
            "updates": updates,
        })
        return True

    def is_in_buffer(self, table_name: str, key_field: str, key_value) -> bool:
        """Check if a record exists in the buffer."""
        if table_name not in self.records:
            return False
        key_str = str(key_value)
        return any(str(r.get(key_field)) == key_str for r in self.records[table_name])

    def flush_all(self) -> None:
        """Flush all buffers."""
        self.flush_count += 1
        # Clear buffers to simulate flush
        self.records = {}

    def flush_for_cdc(self, table_name: str, key_field: str, key_value) -> bool:
        """Flush if record is in buffer (for CDC visibility)."""
        if self.is_in_buffer(table_name, key_field, key_value):
            self.cdc_flush_log.append({
                "table": table_name,
                "key_field": key_field,
                "key_value": str(key_value),
            })
            self.flush_all()
            return True
        return False


@pytest.fixture
def mock_batch_writer():
    """Create a mock batch writer."""
    return MockBatchWriter()


@pytest.fixture
def shared_state():
    """Create a shared state for testing."""
    return SharedState()


@pytest.fixture
def mock_claims_process(test_rng, test_config, sim_env, shared_state, mock_batch_writer):
    """Create a mock claims process for testing transition logic."""
    from brickwell_health.core.processes.claims import ClaimsProcess
    from unittest.mock import MagicMock

    # Create a minimal process for testing
    # We'll use patches to avoid needing full initialization
    with patch.object(ClaimsProcess, '__init__', lambda self, *args, **kwargs: None):
        process = ClaimsProcess()
        process.rng = test_rng
        process.config = test_config
        process.sim_env = sim_env
        process.batch_writer = mock_batch_writer
        process.pending_claims = shared_state.pending_claims
        process.shared_state = shared_state
        process._stats = {}  # Required by increment_stat

        # Mock the claims generator
        process.claims_gen = MagicMock()
        process.claims_gen.DENIAL_REASON_IDS = {
            DenialReason.NO_COVERAGE: 1,
            DenialReason.LIMITS_EXHAUSTED: 2,
            DenialReason.WAITING_PERIOD: 3,
            DenialReason.POLICY_EXCLUSIONS: 4,
            DenialReason.PRE_EXISTING: 5,
            DenialReason.PROVIDER_ISSUES: 6,
            DenialReason.ADMINISTRATIVE: 7,
        }

        return process


class TestClaimScheduling:
    """Tests for claim scheduling to pending_claims."""

    def test_claim_scheduled_to_pending_claims(
        self, mock_claims_process, shared_state
    ):
        """Claims should be added to pending_claims after scheduling."""
        claim_id = uuid4()
        claim_line_id = uuid4()
        member_data = {"member": MagicMock(member_id=uuid4())}

        # Create a mock claim
        mock_claim = MagicMock()
        mock_claim.claim_id = claim_id

        mock_claims_process._schedule_claim_transitions(
            claim=mock_claim,
            claim_line_ids=[claim_line_id],
            member_data=member_data,
            lodgement_date=date(2024, 6, 15),
            approved=True,
            denial_reason=None,
            benefit_category_id=3,
            benefit_amount=Decimal("150.00"),
        )

        assert claim_id in mock_claims_process.pending_claims
        assert mock_claims_process.pending_claims[claim_id]["status"] == "SUBMITTED"
        assert mock_claims_process.pending_claims[claim_id]["approved"] is True

    def test_transition_dates_computed_from_config(
        self, mock_claims_process
    ):
        """Transition dates should be computed using config delays."""
        claim_id = uuid4()
        lodgement_date = date(2024, 6, 15)
        member_data = {"member": MagicMock(member_id=uuid4())}

        mock_claim = MagicMock()
        mock_claim.claim_id = claim_id

        mock_claims_process._schedule_claim_transitions(
            claim=mock_claim,
            claim_line_ids=[],
            member_data=member_data,
            lodgement_date=lodgement_date,
            approved=True,
        )

        pending = mock_claims_process.pending_claims[claim_id]

        # Assessment date should be lodgement + 1-3 days
        assert pending["assessment_date"] >= lodgement_date + timedelta(days=1)
        assert pending["assessment_date"] <= lodgement_date + timedelta(days=3)

        # Approval date should be assessment + 0-1 days
        assert pending["approval_date"] >= pending["assessment_date"]
        assert pending["approval_date"] <= pending["assessment_date"] + timedelta(days=1)

        # Payment date should be approval + 1-3 days
        assert pending["payment_date"] >= pending["approval_date"] + timedelta(days=1)
        assert pending["payment_date"] <= pending["approval_date"] + timedelta(days=3)


class TestClaimTransitions:
    """Tests for claim state transitions."""

    def test_submitted_to_assessed_transition(
        self, mock_claims_process, mock_batch_writer
    ):
        """Claims should transition from SUBMITTED to ASSESSED when assessment_date reached."""
        claim_id = uuid4()
        assessment_date = date(2024, 6, 17)

        mock_claims_process.pending_claims[claim_id] = {
            "status": "SUBMITTED",
            "assessment_date": assessment_date,
            "approval_date": date(2024, 6, 18),
            "payment_date": date(2024, 6, 20),
            "approved": True,
            "denial_reason": None,
            "claim_line_ids": [],
            "benefit_category_id": None,
            "benefit_amount": None,
            "member_data": {},
        }

        # Process transitions on assessment_date
        mock_claims_process._process_claim_transitions(assessment_date)

        # Should have updated claim status
        assert mock_claims_process.pending_claims[claim_id]["status"] == "ASSESSED"

        # Should have called update_record
        assert len(mock_batch_writer.updates) == 1
        update = mock_batch_writer.updates[0]
        assert update["table_name"] == "claim"
        assert update["updates"]["claim_status"] == ClaimStatus.ASSESSED.value

    def test_assessed_to_approved_transition(
        self, mock_claims_process, mock_batch_writer
    ):
        """Claims should transition from ASSESSED to APPROVED when approval_date reached."""
        claim_id = uuid4()
        approval_date = date(2024, 6, 18)

        mock_claims_process.pending_claims[claim_id] = {
            "status": "ASSESSED",
            "assessment_date": date(2024, 6, 17),
            "approval_date": approval_date,
            "payment_date": date(2024, 6, 20),
            "approved": True,
            "denial_reason": None,
            "claim_line_ids": [],
            "benefit_category_id": None,
            "benefit_amount": None,
            "member_data": {},
        }

        mock_claims_process._process_claim_transitions(approval_date)

        assert mock_claims_process.pending_claims[claim_id]["status"] == "APPROVED"

    def test_assessed_to_rejected_transition(
        self, mock_claims_process, mock_batch_writer
    ):
        """Claims with approved=False should transition from ASSESSED to REJECTED."""
        claim_id = uuid4()
        claim_line_id = uuid4()
        approval_date = date(2024, 6, 18)

        mock_claims_process.pending_claims[claim_id] = {
            "status": "ASSESSED",
            "assessment_date": date(2024, 6, 17),
            "approval_date": approval_date,
            "payment_date": date(2024, 6, 20),
            "approved": False,  # Stochastic rejection
            "denial_reason": DenialReason.POLICY_EXCLUSIONS,
            "claim_line_ids": [claim_line_id],
            "benefit_category_id": 3,
            "benefit_amount": Decimal("150.00"),
            "member_data": {},
        }

        mock_claims_process._process_claim_transitions(approval_date)

        # Claim should be removed from pending (rejected claims are complete)
        assert claim_id not in mock_claims_process.pending_claims

        # Should have 2 updates: claim status and claim_line status
        assert len(mock_batch_writer.updates) == 2

        # Find the claim update
        claim_update = next(u for u in mock_batch_writer.updates if u["table_name"] == "claim")
        assert claim_update["updates"]["claim_status"] == ClaimStatus.REJECTED.value
        assert claim_update["updates"]["rejection_reason_id"] == 4  # POLICY_EXCLUSIONS

    def test_approved_to_paid_transition(
        self, mock_claims_process, mock_batch_writer
    ):
        """Claims should transition from APPROVED to PAID when payment_date reached."""
        claim_id = uuid4()
        claim_line_id = uuid4()
        member_id = uuid4()
        payment_date = date(2024, 6, 20)

        mock_member = MagicMock()
        mock_member.member_id = member_id

        policy_id = uuid4()
        mock_claims_process.pending_claims[claim_id] = {
            "status": "APPROVED",
            "assessment_date": date(2024, 6, 17),
            "approval_date": date(2024, 6, 18),
            "payment_date": payment_date,
            "approved": True,
            "denial_reason": None,
            "claim_line_ids": [claim_line_id],
            "benefit_category_id": 3,
            "benefit_amount": Decimal("150.00"),
            "member_data": {"member": mock_member},
            "policy_id": policy_id,
        }

        # Mock _record_benefit_usage to avoid full implementation
        mock_claims_process._record_benefit_usage = MagicMock()

        mock_claims_process._process_claim_transitions(payment_date)

        # Claim should be removed from pending (paid claims are complete)
        assert claim_id not in mock_claims_process.pending_claims

        # Should have 2 updates: claim status and claim_line status
        assert len(mock_batch_writer.updates) == 2

        # Find the claim update
        claim_update = next(u for u in mock_batch_writer.updates if u["table_name"] == "claim")
        assert claim_update["updates"]["claim_status"] == ClaimStatus.PAID.value

    def test_claim_line_updated_on_paid(
        self, mock_claims_process, mock_batch_writer
    ):
        """Claim lines should be updated to 'Paid' when claim transitions to PAID."""
        claim_id = uuid4()
        claim_line_id = uuid4()
        member_id = uuid4()
        policy_id = uuid4()
        payment_date = date(2024, 6, 20)

        mock_member = MagicMock()
        mock_member.member_id = member_id

        mock_claims_process.pending_claims[claim_id] = {
            "status": "APPROVED",
            "assessment_date": date(2024, 6, 17),
            "approval_date": date(2024, 6, 18),
            "payment_date": payment_date,
            "approved": True,
            "denial_reason": None,
            "claim_line_ids": [claim_line_id],
            "benefit_category_id": 3,
            "benefit_amount": Decimal("150.00"),
            "member_data": {"member": mock_member},
            "policy_id": policy_id,
        }

        mock_claims_process._record_benefit_usage = MagicMock()
        mock_claims_process._process_claim_transitions(payment_date)

        # Find the claim_line update
        line_update = next(u for u in mock_batch_writer.updates if u["table_name"] == "claim_line")
        assert line_update["updates"]["line_status"] == "Paid"

    def test_claim_line_updated_on_rejected(
        self, mock_claims_process, mock_batch_writer
    ):
        """Claim lines should be updated to 'Rejected' on stochastic rejection."""
        claim_id = uuid4()
        claim_line_id = uuid4()
        approval_date = date(2024, 6, 18)

        mock_claims_process.pending_claims[claim_id] = {
            "status": "ASSESSED",
            "assessment_date": date(2024, 6, 17),
            "approval_date": approval_date,
            "payment_date": date(2024, 6, 20),
            "approved": False,
            "denial_reason": DenialReason.ADMINISTRATIVE,
            "claim_line_ids": [claim_line_id],
            "benefit_category_id": 3,
            "benefit_amount": Decimal("150.00"),
            "member_data": {},
        }

        mock_claims_process._process_claim_transitions(approval_date)

        # Find the claim_line update
        line_update = next(u for u in mock_batch_writer.updates if u["table_name"] == "claim_line")
        assert line_update["updates"]["line_status"] == "Rejected"

    def test_benefit_usage_recorded_on_paid(
        self, mock_claims_process, mock_batch_writer
    ):
        """Benefit usage should only be recorded when claim transitions to PAID."""
        claim_id = uuid4()
        claim_line_id = uuid4()
        member_id = uuid4()
        policy_id = uuid4()
        payment_date = date(2024, 6, 20)

        mock_member = MagicMock()
        mock_member.member_id = member_id

        mock_claims_process.pending_claims[claim_id] = {
            "status": "APPROVED",
            "assessment_date": date(2024, 6, 17),
            "approval_date": date(2024, 6, 18),
            "payment_date": payment_date,
            "approved": True,
            "denial_reason": None,
            "claim_line_ids": [claim_line_id],
            "benefit_category_id": 3,
            "benefit_amount": Decimal("150.00"),
            "member_data": {"member": mock_member},
            "policy_id": policy_id,
        }

        mock_claims_process._record_benefit_usage = MagicMock()
        mock_claims_process._process_claim_transitions(payment_date)

        # _record_benefit_usage should have been called
        mock_claims_process._record_benefit_usage.assert_called_once()


class TestCDCFlushBehavior:
    """Tests for CDC flush behavior during claim transitions."""

    def test_flush_for_cdc_called_on_first_transition(
        self, mock_claims_process, mock_batch_writer
    ):
        """flush_for_cdc should be called before SUBMITTED -> ASSESSED transition."""
        claim_id = uuid4()
        assessment_date = date(2024, 6, 17)

        # Add claim to buffer to simulate it being there
        mock_batch_writer.records["claim"] = [{"claim_id": str(claim_id)}]

        mock_claims_process.pending_claims[claim_id] = {
            "status": "SUBMITTED",
            "assessment_date": assessment_date,
            "approval_date": date(2024, 6, 18),
            "payment_date": date(2024, 6, 20),
            "approved": True,
            "denial_reason": None,
            "claim_line_ids": [],
            "benefit_category_id": None,
            "benefit_amount": None,
            "member_data": {},
        }

        mock_claims_process._process_claim_transitions(assessment_date)

        # Should have recorded a CDC flush for the claim
        assert len(mock_batch_writer.cdc_flush_log) >= 1
        claim_flush = next(
            (f for f in mock_batch_writer.cdc_flush_log if f["table"] == "claim"),
            None
        )
        assert claim_flush is not None
        assert claim_flush["key_field"] == "claim_id"
        assert claim_flush["key_value"] == str(claim_id)

    def test_flush_for_cdc_called_for_claim_lines_on_paid(
        self, mock_claims_process, mock_batch_writer
    ):
        """flush_for_cdc should be called for claim_lines before updating to Paid."""
        claim_id = uuid4()
        claim_line_id = uuid4()
        member_id = uuid4()
        policy_id = uuid4()
        payment_date = date(2024, 6, 20)

        # Add claim_line to buffer
        mock_batch_writer.records["claim_line"] = [{"claim_line_id": str(claim_line_id)}]

        mock_member = MagicMock()
        mock_member.member_id = member_id

        mock_claims_process.pending_claims[claim_id] = {
            "status": "APPROVED",
            "assessment_date": date(2024, 6, 17),
            "approval_date": date(2024, 6, 18),
            "payment_date": payment_date,
            "approved": True,
            "denial_reason": None,
            "claim_line_ids": [claim_line_id],
            "benefit_category_id": 3,
            "benefit_amount": Decimal("150.00"),
            "member_data": {"member": mock_member},
            "policy_id": policy_id,
        }

        mock_claims_process._record_benefit_usage = MagicMock()
        mock_claims_process._process_claim_transitions(payment_date)

        # Should have recorded a CDC flush for the claim_line
        line_flush = next(
            (f for f in mock_batch_writer.cdc_flush_log if f["table"] == "claim_line"),
            None
        )
        assert line_flush is not None
        assert line_flush["key_field"] == "claim_line_id"
        assert line_flush["key_value"] == str(claim_line_id)

    def test_no_flush_when_record_not_in_buffer(
        self, mock_claims_process, mock_batch_writer
    ):
        """flush_for_cdc should not trigger flush if record already in DB."""
        claim_id = uuid4()
        assessment_date = date(2024, 6, 17)

        # Buffer is empty - claim already flushed to DB
        mock_batch_writer.records = {}

        mock_claims_process.pending_claims[claim_id] = {
            "status": "SUBMITTED",
            "assessment_date": assessment_date,
            "approval_date": date(2024, 6, 18),
            "payment_date": date(2024, 6, 20),
            "approved": True,
            "denial_reason": None,
            "claim_line_ids": [],
            "benefit_category_id": None,
            "benefit_amount": None,
            "member_data": {},
        }

        mock_claims_process._process_claim_transitions(assessment_date)

        # No CDC flush should have been triggered (record was already in DB)
        assert len(mock_batch_writer.cdc_flush_log) == 0
        assert mock_batch_writer.flush_count == 0
