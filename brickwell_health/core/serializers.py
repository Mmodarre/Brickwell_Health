"""
JSON serialization helpers for checkpoint system.

Provides type-safe serialization and deserialization for complex types
used in simulation checkpoints (UUIDs, dates, Decimals, enums, Pydantic models).
"""

import json
from collections import deque
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class CheckpointEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for checkpoint serialization.

    Handles:
    - UUID -> string
    - date/datetime -> ISO format string
    - Decimal -> string (preserves precision)
    - Enum -> value
    - Pydantic models -> dict (via model_dump)
    - deque -> list
    - set -> list
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, deque):
            return list(obj)
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, BaseModel):
            return obj.model_dump(mode="json")
        if hasattr(obj, "__dict__"):
            # Fallback for other objects with __dict__
            return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
        return super().default(obj)


def serialize_to_json(data: Any, indent: int | None = 2) -> str:
    """
    Serialize data to JSON string using CheckpointEncoder.

    Args:
        data: Data to serialize
        indent: JSON indentation (None for compact)

    Returns:
        JSON string
    """
    return json.dumps(data, cls=CheckpointEncoder, indent=indent)


def deserialize_uuid(value: str | None) -> UUID | None:
    """Convert string to UUID, handling None."""
    if value is None:
        return None
    return UUID(value)


def deserialize_date(value: str | None) -> date | None:
    """Convert ISO string to date, handling None."""
    if value is None:
        return None
    return date.fromisoformat(value)


def deserialize_datetime(value: str | None) -> datetime | None:
    """Convert ISO string to datetime, handling None."""
    if value is None:
        return None
    return datetime.fromisoformat(value)


def deserialize_decimal(value: str | None) -> Decimal | None:
    """Convert string to Decimal, handling None."""
    if value is None:
        return None
    return Decimal(value)


def serialize_pending_claims(pending_claims: dict[UUID, dict]) -> dict[str, dict]:
    """
    Serialize pending_claims dict for checkpoint.

    Simplifies member_data to essential fields only (full data
    can be reconstructed from DB using IDs).

    Args:
        pending_claims: Dict keyed by claim_id

    Returns:
        Serialized dict with string keys
    """
    serialized = {}
    for claim_id, data in pending_claims.items():
        member_data = data.get("member_data", {})

        # Extract essential member info (avoid serializing full Pydantic models)
        simplified_member = {}
        if member_data:
            member = member_data.get("member")
            policy = member_data.get("policy")
            simplified_member = {
                "member_id": str(member.member_id) if member else None,
                "policy_id": str(policy.policy_id) if policy else None,
                "policy_member_id": str(member_data.get("policy_member_id"))
                if member_data.get("policy_member_id")
                else None,
                "age": member_data.get("age"),
                "gender": member_data.get("gender"),
                "has_hospital_coverage": member_data.get("hospital_coverage") is not None,
                "has_extras_coverage": member_data.get("extras_coverage") is not None,
                "has_ambulance_coverage": member_data.get("ambulance_coverage")
                is not None,
            }

        serialized[str(claim_id)] = {
            "status": data.get("status"),
            "assessment_date": data["assessment_date"].isoformat()
            if data.get("assessment_date")
            else None,
            "approval_date": data["approval_date"].isoformat()
            if data.get("approval_date")
            else None,
            "payment_date": data["payment_date"].isoformat()
            if data.get("payment_date")
            else None,
            "approved": data.get("approved"),
            "denial_reason": data["denial_reason"].value
            if data.get("denial_reason")
            else None,
            "claim_line_ids": [str(uid) for uid in data.get("claim_line_ids", [])],
            "benefit_category_id": data.get("benefit_category_id"),
            "benefit_amount": str(data["benefit_amount"])
            if data.get("benefit_amount")
            else None,
            "policy_id": str(data["policy_id"]) if data.get("policy_id") else None,
            "is_auto_adjudicated": data.get("is_auto_adjudicated", False),
            "member_data": simplified_member,
        }

    return serialized


def deserialize_pending_claims(serialized: dict[str, dict]) -> dict[UUID, dict]:
    """
    Deserialize pending_claims from checkpoint.

    Note: member_data will be simplified (IDs only). Full member data
    should be looked up from SharedState.policy_members after reconstruction.

    Args:
        serialized: Serialized dict with string keys

    Returns:
        Dict keyed by UUID
    """
    from brickwell_health.domain.enums import DenialReason

    result = {}
    for claim_id_str, data in serialized.items():
        claim_id = UUID(claim_id_str)

        # Parse denial_reason enum
        denial_reason = None
        if data.get("denial_reason"):
            try:
                denial_reason = DenialReason(data["denial_reason"])
            except ValueError:
                denial_reason = None

        result[claim_id] = {
            "status": data.get("status"),
            "assessment_date": deserialize_date(data.get("assessment_date")),
            "approval_date": deserialize_date(data.get("approval_date")),
            "payment_date": deserialize_date(data.get("payment_date")),
            "approved": data.get("approved"),
            "denial_reason": denial_reason,
            "claim_line_ids": [UUID(uid) for uid in data.get("claim_line_ids", [])],
            "benefit_category_id": data.get("benefit_category_id"),
            "benefit_amount": deserialize_decimal(data.get("benefit_amount")),
            "policy_id": deserialize_uuid(data.get("policy_id")),
            "is_auto_adjudicated": data.get("is_auto_adjudicated", False),
            "member_data": data.get("member_data", {}),  # Simplified, needs lookup
        }

    return result


def serialize_crm_pending_cases(pending_cases: dict[UUID, dict]) -> dict[str, dict]:
    """
    Serialize CRM pending_cases dict for checkpoint.

    Args:
        pending_cases: Dict keyed by case_id

    Returns:
        Serialized dict with string keys
    """
    serialized = {}
    for case_id, data in pending_cases.items():
        case = data.get("case")
        serialized[str(case_id)] = {
            "case": case.model_dump(mode="json") if hasattr(case, "model_dump") else case,
            "resolution_date": data["resolution_date"].isoformat()
            if data.get("resolution_date")
            else None,
            "sla_breached": data.get("sla_breached", False),
        }
    return serialized


def deserialize_crm_pending_cases(serialized: dict[str, dict]) -> dict[UUID, dict]:
    """
    Deserialize CRM pending_cases from checkpoint.

    Note: The 'case' field remains as dict, not CaseCreate model.
    Process should handle this appropriately.

    Args:
        serialized: Serialized dict with string keys

    Returns:
        Dict keyed by UUID
    """
    result = {}
    for case_id_str, data in serialized.items():
        case_id = UUID(case_id_str)
        result[case_id] = {
            "case": data.get("case"),  # Keep as dict
            "resolution_date": deserialize_date(data.get("resolution_date")),
            "sla_breached": data.get("sla_breached", False),
        }
    return result


def serialize_crm_pending_complaints(
    pending_complaints: dict[UUID, dict],
) -> dict[str, dict]:
    """
    Serialize CRM pending_complaints dict for checkpoint.

    Args:
        pending_complaints: Dict keyed by complaint_id

    Returns:
        Serialized dict with string keys
    """
    serialized = {}
    for complaint_id, data in pending_complaints.items():
        complaint = data.get("complaint")
        serialized[str(complaint_id)] = {
            "complaint": complaint.model_dump(mode="json")
            if hasattr(complaint, "model_dump")
            else complaint,
            "resolution_date": data["resolution_date"].isoformat()
            if data.get("resolution_date")
            else None,
            "resolution_outcome": data.get("resolution_outcome"),
            "phio_escalated": data.get("phio_escalated", False),
            "acknowledged": data.get("acknowledged", False),
        }
    return serialized


def deserialize_crm_pending_complaints(
    serialized: dict[str, dict],
) -> dict[UUID, dict]:
    """
    Deserialize CRM pending_complaints from checkpoint.

    Args:
        serialized: Serialized dict with string keys

    Returns:
        Dict keyed by UUID
    """
    result = {}
    for complaint_id_str, data in serialized.items():
        complaint_id = UUID(complaint_id_str)
        result[complaint_id] = {
            "complaint": data.get("complaint"),  # Keep as dict
            "resolution_date": deserialize_date(data.get("resolution_date")),
            "resolution_outcome": data.get("resolution_outcome"),
            "phio_escalated": data.get("phio_escalated", False),
            "acknowledged": data.get("acknowledged", False),
        }
    return result


def serialize_crm_active_journeys(active_journeys: dict[UUID, dict]) -> dict[str, dict]:
    """
    Serialize CRM active_journeys dict for checkpoint.

    Args:
        active_journeys: Dict keyed by member_id

    Returns:
        Serialized dict with string keys
    """
    serialized = {}
    for member_id, journey in active_journeys.items():
        serialized[str(member_id)] = {
            "member_id": str(journey.get("member_id"))
            if journey.get("member_id")
            else None,
            "trigger_event": journey.get("trigger_event"),  # Already dict
            "trigger_type": journey.get("trigger_type"),
            "start_date": journey["start_date"].isoformat()
            if journey.get("start_date")
            else None,
            "escalation_type": journey.get("escalation_type"),
            "highest_level": journey.get("highest_level"),
            "triggered_actions": journey.get("triggered_actions", []),
            "interactions": [str(uid) for uid in journey.get("interactions", [])],
            "case_id": str(journey["case_id"]) if journey.get("case_id") else None,
            "complaint_id": str(journey["complaint_id"])
            if journey.get("complaint_id")
            else None,
            "timeout_date": journey["timeout_date"].isoformat()
            if journey.get("timeout_date")
            else None,
            "first_contact_resolution": journey.get("first_contact_resolution"),
            "resolution_outcome": journey.get("resolution_outcome"),
            "case_sla_breached": journey.get("case_sla_breached"),
            "phio_escalated": journey.get("phio_escalated"),
            "prediction_factors": journey.get("prediction_factors", {}),
            "additional_claims": journey.get("additional_claims"),
        }
    return serialized


def deserialize_crm_active_journeys(serialized: dict[str, dict]) -> dict[UUID, dict]:
    """
    Deserialize CRM active_journeys from checkpoint.

    Args:
        serialized: Serialized dict with string keys

    Returns:
        Dict keyed by UUID (member_id)
    """
    result = {}
    for member_id_str, journey in serialized.items():
        member_id = UUID(member_id_str)
        result[member_id] = {
            "member_id": deserialize_uuid(journey.get("member_id")),
            "trigger_event": journey.get("trigger_event"),
            "trigger_type": journey.get("trigger_type"),
            "start_date": deserialize_date(journey.get("start_date")),
            "escalation_type": journey.get("escalation_type"),
            "highest_level": journey.get("highest_level"),
            "triggered_actions": journey.get("triggered_actions", []),
            "interactions": [UUID(uid) for uid in journey.get("interactions", [])],
            "case_id": deserialize_uuid(journey.get("case_id")),
            "complaint_id": deserialize_uuid(journey.get("complaint_id")),
            "timeout_date": deserialize_date(journey.get("timeout_date")),
            "first_contact_resolution": journey.get("first_contact_resolution"),
            "resolution_outcome": journey.get("resolution_outcome"),
            "case_sla_breached": journey.get("case_sla_breached"),
            "phio_escalated": journey.get("phio_escalated"),
            "prediction_factors": journey.get("prediction_factors", {}),
            "additional_claims": journey.get("additional_claims"),
        }
    return result


def serialize_billing_retry_state(pending_invoices: dict[UUID, dict]) -> dict[str, dict]:
    """
    Serialize billing process retry state for checkpoint.

    Only extracts the non-DB fields (attempts, next_attempt_date, arrears_created).

    Args:
        pending_invoices: Dict keyed by invoice_id

    Returns:
        Serialized dict with string keys containing only retry state
    """
    serialized = {}
    for invoice_id, data in pending_invoices.items():
        serialized[str(invoice_id)] = {
            "attempts": data.get("attempts", 0),
            "next_attempt_date": data["next_attempt_date"].isoformat()
            if data.get("next_attempt_date")
            else None,
            "arrears_created": data.get("arrears_created", False),
        }
    return serialized


def deserialize_billing_retry_state(serialized: dict[str, dict]) -> dict[UUID, dict]:
    """
    Deserialize billing retry state from checkpoint.

    Args:
        serialized: Serialized dict with string keys

    Returns:
        Dict keyed by UUID containing retry state fields
    """
    result = {}
    for invoice_id_str, data in serialized.items():
        invoice_id = UUID(invoice_id_str)
        result[invoice_id] = {
            "attempts": data.get("attempts", 0),
            "next_attempt_date": deserialize_date(data.get("next_attempt_date")),
            "arrears_created": data.get("arrears_created", False),
        }
    return result


def serialize_event_queue(queue: deque[dict]) -> list[dict]:
    """
    Serialize an event queue (CRM or communication events).

    Args:
        queue: Deque of event dicts

    Returns:
        List of serialized event dicts
    """
    return json.loads(serialize_to_json(list(queue)))


def deserialize_event_queue(events: list[dict]) -> deque[dict]:
    """
    Deserialize an event queue.

    Note: UUIDs and dates in events remain as strings.
    Processes should handle string conversion as needed.

    Args:
        events: List of event dicts

    Returns:
        Deque of event dicts
    """
    return deque(events)


def serialize_member_engagement_levels(
    levels: dict[UUID, str],
) -> dict[str, str]:
    """
    Serialize member engagement levels.

    Args:
        levels: Dict mapping member_id to engagement level

    Returns:
        Dict with string keys
    """
    return {str(k): v for k, v in levels.items()}


def deserialize_member_engagement_levels(
    serialized: dict[str, str],
) -> dict[UUID, str]:
    """
    Deserialize member engagement levels.

    Args:
        serialized: Dict with string keys

    Returns:
        Dict mapping UUID to engagement level
    """
    return {UUID(k): v for k, v in serialized.items()}


def serialize_pending_campaign_responses(
    responses: dict[UUID, dict],
) -> dict[str, dict]:
    """
    Serialize pending campaign responses for checkpoint.

    Args:
        responses: Dict keyed by response_id

    Returns:
        Serialized dict with string keys
    """
    return json.loads(serialize_to_json({str(k): v for k, v in responses.items()}))


def deserialize_pending_campaign_responses(
    serialized: dict[str, dict],
) -> dict[UUID, dict]:
    """
    Deserialize pending campaign responses from checkpoint.

    Converts datetime strings back to datetime objects.

    Args:
        serialized: Serialized dict with string keys

    Returns:
        Dict keyed by UUID with proper datetime objects
    """
    result: dict[UUID, dict] = {}
    datetime_fields = [
        "predicted_response_date",
        "predicted_open_date",
        "predicted_click_date",
        "sent_at",
        "opened_at",
        "clicked_at",
    ]
    
    for k, v in serialized.items():
        response_id = UUID(k)
        response_data = dict(v)
        
        # Convert all datetime fields
        for field in datetime_fields:
            if field in response_data and response_data[field]:
                if isinstance(response_data[field], str):
                    response_data[field] = datetime.fromisoformat(response_data[field])
        
        result[response_id] = response_data
    
    return result


def serialize_digital_processed_triggers(triggers: set[str]) -> list[str]:
    """
    Serialize digital process trigger tracking set.

    Args:
        triggers: Set of processed trigger keys

    Returns:
        List of trigger keys
    """
    return list(triggers)


def deserialize_digital_processed_triggers(triggers: list[str]) -> set[str]:
    """
    Deserialize digital process trigger tracking set.

    Args:
        triggers: List of trigger keys

    Returns:
        Set of trigger keys
    """
    return set(triggers)


def serialize_nba_execution_history(
    history: dict[UUID, list[dict]],
) -> dict[str, list[dict]]:
    """
    Serialize NBA execution history for checkpoint.

    Args:
        history: Dict mapping member_id to list of execution records

    Returns:
        Serialized dict with string keys
    """
    serialized = {}
    for member_id, executions in history.items():
        serialized[str(member_id)] = []
        for exec_data in executions:
            serialized_exec = {
                "action_id": str(exec_data["action_id"])
                if exec_data.get("action_id")
                else None,
                "action_code": exec_data.get("action_code"),
                "action_category": exec_data.get("action_category"),
                "executed_at": exec_data["executed_at"].isoformat()
                if exec_data.get("executed_at")
                else None,
                "execution_channel": exec_data.get("execution_channel"),
            }
            serialized[str(member_id)].append(serialized_exec)
    return serialized


def deserialize_nba_execution_history(
    serialized: dict[str, list[dict]],
) -> dict[UUID, list[dict]]:
    """
    Deserialize NBA execution history from checkpoint.

    Args:
        serialized: Serialized dict with string keys

    Returns:
        Dict mapping member_id (UUID) to list of execution records
    """
    result = {}
    for member_id_str, executions in serialized.items():
        member_id = UUID(member_id_str)
        result[member_id] = []
        for exec_data in executions:
            deserialized_exec = {
                "action_id": deserialize_uuid(exec_data.get("action_id")),
                "action_code": exec_data.get("action_code"),
                "action_category": exec_data.get("action_category"),
                "executed_at": deserialize_datetime(exec_data.get("executed_at")),
                "execution_channel": exec_data.get("execution_channel"),
            }
            result[member_id].append(deserialized_exec)
    return result


def serialize_nba_active_effects(
    effects: dict[UUID, list[dict]],
) -> dict[str, list[dict]]:
    """
    Serialize NBA active effects for checkpoint.

    Args:
        effects: Dict mapping policy_id to list of effect records

    Returns:
        Serialized dict with string keys
    """
    serialized = {}
    for policy_id, effect_list in effects.items():
        serialized[str(policy_id)] = []
        for effect in effect_list:
            serialized_effect = {
                "effect_type": effect.get("effect_type"),
                "value": effect.get("value"),
                "expires_at": effect["expires_at"].isoformat()
                if effect.get("expires_at")
                else None,
                "source_action_id": str(effect["source_action_id"])
                if effect.get("source_action_id")
                else None,
                "source_recommendation_id": str(effect["source_recommendation_id"])
                if effect.get("source_recommendation_id")
                else None,
            }
            serialized[str(policy_id)].append(serialized_effect)
    return serialized


def deserialize_nba_active_effects(
    serialized: dict[str, list[dict]],
) -> dict[UUID, list[dict]]:
    """
    Deserialize NBA active effects from checkpoint.

    Args:
        serialized: Serialized dict with string keys

    Returns:
        Dict mapping policy_id (UUID) to list of effect records
    """
    result = {}
    for policy_id_str, effect_list in serialized.items():
        policy_id = UUID(policy_id_str)
        result[policy_id] = []
        for effect in effect_list:
            deserialized_effect = {
                "effect_type": effect.get("effect_type"),
                "value": effect.get("value"),
                "expires_at": deserialize_datetime(effect.get("expires_at")),
                "source_action_id": deserialize_uuid(effect.get("source_action_id")),
                "source_recommendation_id": deserialize_uuid(
                    effect.get("source_recommendation_id")
                ),
            }
            result[policy_id].append(deserialized_effect)
    return result
