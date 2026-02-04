"""
Unit tests for checkpoint resume functionality.

Tests serialization/deserialization of checkpoint data and
state reconstruction helpers.
"""

import json
import tempfile
from collections import deque
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from brickwell_health.core.serializers import (
    CheckpointEncoder,
    deserialize_billing_retry_state,
    deserialize_crm_active_journeys,
    deserialize_crm_pending_cases,
    deserialize_crm_pending_complaints,
    deserialize_date,
    deserialize_decimal,
    deserialize_digital_processed_triggers,
    deserialize_event_queue,
    deserialize_member_engagement_levels,
    deserialize_pending_campaign_responses,
    deserialize_pending_claims,
    deserialize_uuid,
    serialize_billing_retry_state,
    serialize_crm_active_journeys,
    serialize_crm_pending_cases,
    serialize_crm_pending_complaints,
    serialize_digital_processed_triggers,
    serialize_event_queue,
    serialize_member_engagement_levels,
    serialize_pending_campaign_responses,
    serialize_pending_claims,
    serialize_to_json,
)
from brickwell_health.core.checkpoint_v2 import (
    CHECKPOINT_VERSION,
    CheckpointCorruptedError,
    CheckpointManagerV2,
    CheckpointNotFoundError,
    get_checkpoint_dates,
    restore_shared_state_from_checkpoint,
)
from brickwell_health.core.shared_state import SharedState


class TestCheckpointEncoder:
    """Tests for CheckpointEncoder JSON serialization."""

    def test_uuid_serialization(self):
        """Test UUID serializes to string."""
        uid = uuid4()
        result = json.dumps({"id": uid}, cls=CheckpointEncoder)
        assert str(uid) in result

    def test_date_serialization(self):
        """Test date serializes to ISO format."""
        d = date(2024, 1, 15)
        result = json.dumps({"date": d}, cls=CheckpointEncoder)
        assert "2024-01-15" in result

    def test_datetime_serialization(self):
        """Test datetime serializes to ISO format."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = json.dumps({"datetime": dt}, cls=CheckpointEncoder)
        assert "2024-01-15T10:30:00" in result

    def test_decimal_serialization(self):
        """Test Decimal serializes to string (preserves precision)."""
        d = Decimal("123.45")
        result = json.dumps({"amount": d}, cls=CheckpointEncoder)
        assert "123.45" in result

    def test_deque_serialization(self):
        """Test deque serializes to list."""
        dq = deque([1, 2, 3])
        result = json.dumps({"queue": dq}, cls=CheckpointEncoder)
        data = json.loads(result)
        assert data["queue"] == [1, 2, 3]

    def test_set_serialization(self):
        """Test set serializes to list."""
        s = {"a", "b", "c"}
        result = json.dumps({"items": s}, cls=CheckpointEncoder)
        data = json.loads(result)
        assert set(data["items"]) == s


class TestBasicDeserializers:
    """Tests for basic type deserializers."""

    def test_deserialize_uuid_valid(self):
        """Test UUID deserialization."""
        uid = uuid4()
        result = deserialize_uuid(str(uid))
        assert result == uid

    def test_deserialize_uuid_none(self):
        """Test UUID deserialization with None."""
        assert deserialize_uuid(None) is None

    def test_deserialize_date_valid(self):
        """Test date deserialization."""
        result = deserialize_date("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_deserialize_date_none(self):
        """Test date deserialization with None."""
        assert deserialize_date(None) is None

    def test_deserialize_decimal_valid(self):
        """Test Decimal deserialization."""
        result = deserialize_decimal("123.45")
        assert result == Decimal("123.45")

    def test_deserialize_decimal_none(self):
        """Test Decimal deserialization with None."""
        assert deserialize_decimal(None) is None


class TestPendingClaimsSerializer:
    """Tests for pending_claims serialization."""

    def test_serialize_empty(self):
        """Test serializing empty dict."""
        result = serialize_pending_claims({})
        assert result == {}

    def test_serialize_basic_claim(self):
        """Test serializing a basic pending claim."""
        claim_id = uuid4()
        policy_id = uuid4()
        claim_line_id = uuid4()

        # Create mock member and policy objects
        mock_member = MagicMock()
        mock_member.member_id = uuid4()
        mock_policy = MagicMock()
        mock_policy.policy_id = policy_id

        pending_claims = {
            claim_id: {
                "status": "SUBMITTED",
                "assessment_date": date(2024, 1, 15),
                "approval_date": date(2024, 1, 17),
                "payment_date": date(2024, 1, 20),
                "approved": True,
                "denial_reason": None,
                "claim_line_ids": [claim_line_id],
                "benefit_category_id": 3,
                "benefit_amount": Decimal("150.00"),
                "policy_id": policy_id,
                "is_auto_adjudicated": True,
                "member_data": {
                    "member": mock_member,
                    "policy": mock_policy,
                    "policy_member_id": uuid4(),
                    "age": 35,
                    "gender": "Female",
                    "hospital_coverage": {"id": "hosp"},
                    "extras_coverage": None,
                    "ambulance_coverage": None,
                },
            }
        }

        result = serialize_pending_claims(pending_claims)

        assert str(claim_id) in result
        serialized = result[str(claim_id)]
        assert serialized["status"] == "SUBMITTED"
        assert serialized["assessment_date"] == "2024-01-15"
        assert serialized["approved"] is True
        assert serialized["benefit_amount"] == "150.00"
        assert serialized["member_data"]["age"] == 35
        assert serialized["member_data"]["has_hospital_coverage"] is True
        assert serialized["member_data"]["has_extras_coverage"] is False

    def test_deserialize_pending_claims(self):
        """Test deserializing pending claims."""
        claim_id_str = str(uuid4())
        policy_id_str = str(uuid4())
        claim_line_id_str = str(uuid4())

        serialized = {
            claim_id_str: {
                "status": "ASSESSED",
                "assessment_date": "2024-01-15",
                "approval_date": "2024-01-17",
                "payment_date": "2024-01-20",
                "approved": False,
                "denial_reason": None,
                "claim_line_ids": [claim_line_id_str],
                "benefit_category_id": 7,
                "benefit_amount": "200.00",
                "policy_id": policy_id_str,
                "is_auto_adjudicated": False,
                "member_data": {"age": 40},
            }
        }

        result = deserialize_pending_claims(serialized)

        assert UUID(claim_id_str) in result
        claim = result[UUID(claim_id_str)]
        assert claim["status"] == "ASSESSED"
        assert claim["assessment_date"] == date(2024, 1, 15)
        assert claim["approved"] is False
        assert claim["benefit_amount"] == Decimal("200.00")
        assert claim["claim_line_ids"][0] == UUID(claim_line_id_str)


class TestCRMSerializers:
    """Tests for CRM state serialization."""

    def test_serialize_pending_cases(self):
        """Test serializing pending cases."""
        case_id = uuid4()
        mock_case = MagicMock()
        mock_case.model_dump.return_value = {"case_id": str(case_id), "subject": "Test"}

        pending_cases = {
            case_id: {
                "case": mock_case,
                "resolution_date": date(2024, 2, 1),
                "sla_breached": False,
            }
        }

        result = serialize_crm_pending_cases(pending_cases)
        assert str(case_id) in result
        assert result[str(case_id)]["resolution_date"] == "2024-02-01"
        assert result[str(case_id)]["sla_breached"] is False

    def test_deserialize_pending_cases(self):
        """Test deserializing pending cases."""
        case_id_str = str(uuid4())
        serialized = {
            case_id_str: {
                "case": {"case_id": case_id_str, "subject": "Test"},
                "resolution_date": "2024-02-01",
                "sla_breached": True,
            }
        }

        result = deserialize_crm_pending_cases(serialized)
        assert UUID(case_id_str) in result
        assert result[UUID(case_id_str)]["resolution_date"] == date(2024, 2, 1)
        assert result[UUID(case_id_str)]["sla_breached"] is True

    def test_serialize_active_journeys(self):
        """Test serializing active journeys."""
        member_id = uuid4()
        case_id = uuid4()

        active_journeys = {
            member_id: {
                "member_id": member_id,
                "trigger_event": {"event_type": "claim_paid"},
                "trigger_type": "claim_paid",
                "start_date": date(2024, 1, 10),
                "escalation_type": "case",
                "highest_level": "case",
                "triggered_actions": ["interaction", "case"],
                "interactions": [uuid4()],
                "case_id": case_id,
                "complaint_id": None,
                "timeout_date": date(2024, 1, 12),
                "first_contact_resolution": True,
                "resolution_outcome": None,
                "case_sla_breached": False,
                "phio_escalated": None,
                "prediction_factors": {"score": 0.75},
                "additional_claims": None,
            }
        }

        result = serialize_crm_active_journeys(active_journeys)
        assert str(member_id) in result
        journey = result[str(member_id)]
        assert journey["start_date"] == "2024-01-10"
        assert journey["case_id"] == str(case_id)
        assert journey["complaint_id"] is None

    def test_roundtrip_active_journeys(self):
        """Test round-trip serialization of active journeys."""
        member_id = uuid4()

        original = {
            member_id: {
                "member_id": member_id,
                "trigger_event": {"event_type": "claim_rejected"},
                "trigger_type": "claim_rejected",
                "start_date": date(2024, 1, 5),
                "escalation_type": None,
                "highest_level": None,
                "triggered_actions": [],
                "interactions": [],
                "case_id": None,
                "complaint_id": None,
                "timeout_date": None,
                "first_contact_resolution": None,
                "resolution_outcome": None,
                "case_sla_breached": None,
                "phio_escalated": None,
                "prediction_factors": {},
                "additional_claims": None,
            }
        }

        serialized = serialize_crm_active_journeys(original)
        deserialized = deserialize_crm_active_journeys(serialized)

        assert member_id in deserialized
        assert deserialized[member_id]["start_date"] == date(2024, 1, 5)
        assert deserialized[member_id]["escalation_type"] is None


class TestBillingRetryStateSerializer:
    """Tests for billing retry state serialization."""

    def test_serialize_billing_retry_state(self):
        """Test serializing billing retry state."""
        invoice_id = uuid4()

        pending_invoices = {
            invoice_id: {
                "attempts": 2,
                "next_attempt_date": date(2024, 1, 20),
                "arrears_created": True,
                "other_field": "ignored",
            }
        }

        result = serialize_billing_retry_state(pending_invoices)
        assert str(invoice_id) in result
        assert result[str(invoice_id)]["attempts"] == 2
        assert result[str(invoice_id)]["next_attempt_date"] == "2024-01-20"
        assert result[str(invoice_id)]["arrears_created"] is True
        assert "other_field" not in result[str(invoice_id)]

    def test_roundtrip_billing_retry_state(self):
        """Test round-trip serialization of billing retry state."""
        invoice_id = uuid4()

        original = {
            invoice_id: {
                "attempts": 3,
                "next_attempt_date": date(2024, 2, 1),
                "arrears_created": False,
            }
        }

        serialized = serialize_billing_retry_state(original)
        deserialized = deserialize_billing_retry_state(serialized)

        assert invoice_id in deserialized
        assert deserialized[invoice_id]["attempts"] == 3
        assert deserialized[invoice_id]["next_attempt_date"] == date(2024, 2, 1)


class TestEventQueueSerializer:
    """Tests for event queue serialization."""

    def test_serialize_event_queue(self):
        """Test serializing event queue."""
        queue = deque([
            {"event_type": "claim_paid", "timestamp": "2024-01-15T10:00:00"},
            {"event_type": "payment_failed", "timestamp": "2024-01-16T11:00:00"},
        ])

        result = serialize_event_queue(queue)
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["event_type"] == "claim_paid"

    def test_deserialize_event_queue(self):
        """Test deserializing event queue."""
        events = [
            {"event_type": "claim_paid"},
            {"event_type": "payment_failed"},
        ]

        result = deserialize_event_queue(events)
        assert isinstance(result, deque)
        assert len(result) == 2
        assert result.popleft()["event_type"] == "claim_paid"


class TestEngagementLevelsSerializer:
    """Tests for member engagement levels serialization."""

    def test_roundtrip_engagement_levels(self):
        """Test round-trip serialization of engagement levels."""
        member1 = uuid4()
        member2 = uuid4()

        original = {
            member1: "high",
            member2: "low",
        }

        serialized = serialize_member_engagement_levels(original)
        deserialized = deserialize_member_engagement_levels(serialized)

        assert member1 in deserialized
        assert deserialized[member1] == "high"
        assert deserialized[member2] == "low"


class TestDigitalTriggersSerializer:
    """Tests for digital processed triggers serialization."""

    def test_roundtrip_processed_triggers(self):
        """Test round-trip serialization of processed triggers."""
        original = {"trigger-1", "trigger-2", "trigger-3"}

        serialized = serialize_digital_processed_triggers(original)
        deserialized = deserialize_digital_processed_triggers(serialized)

        assert deserialized == original


class TestCheckpointManagerV2:
    """Tests for CheckpointManagerV2."""

    def test_save_and_load_checkpoint(self, tmp_path):
        """Test saving and loading a checkpoint."""
        manager = CheckpointManagerV2(tmp_path)
        shared_state = SharedState()

        # Add some test data
        member_id = uuid4()
        shared_state.member_engagement_levels[member_id] = "high"

        # Save checkpoint
        manager.save_full_checkpoint(
            worker_id=0,
            sim_time=100.0,
            checkpoint_date=date(2024, 1, 15),
            original_start_date=date(2023, 1, 1),
            id_counters={"member": 1000, "policy": 500},
            rng_state={"bit_generator": "test"},
            shared_state=shared_state,
        )

        # Load checkpoint
        checkpoint = manager.load_checkpoint(0)

        assert checkpoint is not None
        assert checkpoint["version"] == CHECKPOINT_VERSION
        assert checkpoint["sim_time"] == 100.0
        assert checkpoint["checkpoint_date"] == "2024-01-15"
        assert checkpoint["id_counters"]["member"] == 1000

    def test_load_nonexistent_checkpoint(self, tmp_path):
        """Test loading a nonexistent checkpoint returns None."""
        manager = CheckpointManagerV2(tmp_path)
        result = manager.load_checkpoint(99)
        assert result is None

    def test_load_checkpoint_strict_fails_when_missing(self, tmp_path):
        """Test load_checkpoint_strict raises error when missing."""
        manager = CheckpointManagerV2(tmp_path)
        with pytest.raises(CheckpointNotFoundError):
            manager.load_checkpoint_strict(99)

    def test_delete_checkpoint(self, tmp_path):
        """Test deleting a checkpoint."""
        manager = CheckpointManagerV2(tmp_path)
        shared_state = SharedState()

        manager.save_full_checkpoint(
            worker_id=0,
            sim_time=50.0,
            checkpoint_date=date(2024, 1, 10),
            original_start_date=date(2023, 1, 1),
            id_counters={},
            rng_state={},
            shared_state=shared_state,
        )

        assert manager.has_checkpoint(0) is True
        manager.delete(0)
        assert manager.has_checkpoint(0) is False

    def test_list_checkpoints(self, tmp_path):
        """Test listing checkpoints."""
        manager = CheckpointManagerV2(tmp_path)
        shared_state = SharedState()

        # Save two checkpoints
        for worker_id in [0, 1]:
            manager.save_full_checkpoint(
                worker_id=worker_id,
                sim_time=100.0 + worker_id,
                checkpoint_date=date(2024, 1, 15),
                original_start_date=date(2023, 1, 1),
                id_counters={},
                rng_state={},
                shared_state=shared_state,
            )

        checkpoints = manager.list_checkpoints()
        assert len(checkpoints) == 2

    def test_corrupted_checkpoint_raises_error(self, tmp_path):
        """Test that corrupted checkpoint raises error."""
        manager = CheckpointManagerV2(tmp_path)

        # Write invalid JSON
        checkpoint_path = tmp_path / "checkpoint_v2_0.json"
        checkpoint_path.write_text("{invalid json")

        with pytest.raises(CheckpointCorruptedError):
            manager.load_checkpoint(0)

    def test_version_mismatch_raises_error(self, tmp_path):
        """Test that version mismatch raises error."""
        manager = CheckpointManagerV2(tmp_path)

        # Write checkpoint with wrong version
        checkpoint_path = tmp_path / "checkpoint_v2_0.json"
        checkpoint_path.write_text(json.dumps({"version": "1.0"}))

        with pytest.raises(CheckpointCorruptedError):
            manager.load_checkpoint(0)


class TestRestoreSharedStateFromCheckpoint:
    """Tests for restore_shared_state_from_checkpoint."""

    def test_restore_engagement_levels(self):
        """Test restoring member engagement levels."""
        shared_state = SharedState()
        member_id = uuid4()

        checkpoint = {
            "member_engagement_levels": {str(member_id): "high"},
            "pending_claims": {},
            "crm_event_queue": [],
            "communication_event_queue": [],
            "pending_campaign_responses": {},
            "member_change_events": [],
        }

        restore_shared_state_from_checkpoint(shared_state, checkpoint)

        assert member_id in shared_state.member_engagement_levels
        assert shared_state.member_engagement_levels[member_id] == "high"

    def test_restore_event_queues(self):
        """Test restoring event queues."""
        shared_state = SharedState()

        checkpoint = {
            "member_engagement_levels": {},
            "pending_claims": {},
            "crm_event_queue": [{"event_type": "test1"}],
            "communication_event_queue": [{"event_type": "test2"}],
            "pending_campaign_responses": {},
            "member_change_events": [{"change_type": "ADDRESS_CHANGE"}],
        }

        restore_shared_state_from_checkpoint(shared_state, checkpoint)

        assert len(shared_state.crm_event_queue) == 1
        assert len(shared_state.communication_event_queue) == 1
        assert len(shared_state.member_change_events) == 1


class TestGetCheckpointDates:
    """Tests for get_checkpoint_dates."""

    def test_extracts_dates_correctly(self):
        """Test extracting dates from checkpoint."""
        checkpoint = {
            "checkpoint_date": "2024-06-15",
            "original_start_date": "2023-01-01",
        }

        checkpoint_date, original_start_date = get_checkpoint_dates(checkpoint)

        assert checkpoint_date == date(2024, 6, 15)
        assert original_start_date == date(2023, 1, 1)

    def test_raises_error_on_missing_dates(self):
        """Test raises error when dates missing."""
        checkpoint = {
            "checkpoint_date": None,
            "original_start_date": "2023-01-01",
        }

        with pytest.raises(CheckpointCorruptedError):
            get_checkpoint_dates(checkpoint)
