"""
CRM Domain Models for Brickwell Health Simulator.

Models for Interaction, Case, and Complaint entities.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    CasePriority,
    CaseStatus,
    ComplaintResolutionOutcome,
    ComplaintSeverity,
    ComplaintSource,
    ComplaintStatus,
    InteractionChannel,
    InteractionDirection,
    TriggerEventType,
)


# ============================================================================
# INTERACTION MODELS
# ============================================================================

class InteractionCreate(BaseModel):
    """Model for creating an interaction record."""

    interaction_id: UUID
    interaction_reference: str = Field(..., max_length=30)

    # Relationships
    policy_id: UUID
    member_id: UUID
    interaction_type_id: int

    # Channel & Direction
    channel: InteractionChannel
    direction: InteractionDirection

    # Timing
    start_datetime: datetime
    end_datetime: Optional[datetime] = None
    duration_seconds: Optional[int] = Field(None, ge=0)

    # Content
    subject: Optional[str] = Field(None, max_length=200)
    summary: Optional[str] = Field(None, max_length=2000)

    # Outcome
    outcome_id: Optional[int] = None
    handled_by: Optional[str] = Field(None, max_length=50)
    queue_name: Optional[str] = Field(None, max_length=50)
    wait_time_seconds: Optional[int] = Field(None, ge=0)

    # Resolution
    first_contact_resolution: bool = False
    satisfaction_score: Optional[int] = Field(None, ge=1, le=5)

    # Trigger Context
    trigger_event_type: Optional[TriggerEventType] = None
    trigger_event_id: Optional[UUID] = None

    # Linked Records
    case_id: Optional[UUID] = None
    claim_id: Optional[UUID] = None
    invoice_id: Optional[UUID] = None

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("channel") is not None:
            data["channel"] = (
                data["channel"].value
                if isinstance(data["channel"], InteractionChannel)
                else data["channel"]
            )
        if data.get("direction") is not None:
            data["direction"] = (
                data["direction"].value
                if isinstance(data["direction"], InteractionDirection)
                else data["direction"]
            )
        if data.get("trigger_event_type") is not None:
            data["trigger_event_type"] = (
                data["trigger_event_type"].value
                if isinstance(data["trigger_event_type"], TriggerEventType)
                else data["trigger_event_type"]
            )
        return data


class Interaction(InteractionCreate):
    """Full interaction model with audit fields."""

    class Config:
        from_attributes = True


# ============================================================================
# CASE MODELS
# ============================================================================

class CaseCreate(BaseModel):
    """Model for creating a service case."""

    case_id: UUID
    case_number: str = Field(..., max_length=30)

    # Type
    case_type_id: int

    # Relationships
    policy_id: UUID
    member_id: UUID

    # Context
    subject: str = Field(..., max_length=200)
    description: Optional[str] = Field(None, max_length=4000)

    # Priority & Status
    priority: CasePriority
    status: CaseStatus = CaseStatus.OPEN

    # Assignment
    assigned_to: Optional[str] = Field(None, max_length=50)
    assigned_team: Optional[str] = Field(None, max_length=50)

    # Source
    source_interaction_id: Optional[UUID] = None

    # Related Entities
    related_claim_id: Optional[UUID] = None
    related_invoice_id: Optional[UUID] = None

    # SLA
    due_date: Optional[date] = None
    resolution_date: Optional[datetime] = None
    resolution_summary: Optional[str] = Field(None, max_length=1000)
    sla_breached: bool = False

    # Metrics
    note_count: int = Field(default=0, ge=0)
    task_count: int = Field(default=0, ge=0)

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("priority") is not None:
            data["priority"] = (
                data["priority"].value
                if isinstance(data["priority"], CasePriority)
                else data["priority"]
            )
        if data.get("status") is not None:
            data["status"] = (
                data["status"].value
                if isinstance(data["status"], CaseStatus)
                else data["status"]
            )
        return data


class Case(CaseCreate):
    """Full case model with audit fields."""

    class Config:
        from_attributes = True


# ============================================================================
# COMPLAINT MODELS
# ============================================================================

class ComplaintCreate(BaseModel):
    """Model for creating a complaint record."""

    complaint_id: UUID
    complaint_number: str = Field(..., max_length=30)

    # Relationships
    case_id: Optional[UUID] = None
    policy_id: UUID
    member_id: UUID

    # Classification
    complaint_category_id: int

    # Content
    subject: str = Field(..., max_length=200)
    description: Optional[str] = Field(None, max_length=4000)

    # Severity & Status
    severity: ComplaintSeverity
    status: ComplaintStatus = ComplaintStatus.RECEIVED
    source: ComplaintSource

    # Timeline
    received_date: date
    acknowledged_date: Optional[date] = None
    due_date: date

    # Assignment
    assigned_to: Optional[str] = Field(None, max_length=50)

    # Resolution
    resolution_date: Optional[date] = None
    resolution_summary: Optional[str] = Field(None, max_length=2000)
    resolution_outcome: Optional[ComplaintResolutionOutcome] = None
    compensation_amount: Optional[Decimal] = Field(None, ge=0)

    # PHIO Escalation
    phio_escalated: bool = False
    phio_reference: Optional[str] = Field(None, max_length=30)
    phio_escalation_date: Optional[date] = None
    phio_decision_outcome: Optional[str] = Field(None, max_length=50)

    # Internal Review
    internal_review_requested: bool = False
    internal_review_outcome: Optional[str] = Field(None, max_length=50)

    # Escalation Tracking
    escalation_count: int = Field(default=0, ge=0)

    # Related Entities
    related_claim_id: Optional[UUID] = None
    related_invoice_id: Optional[UUID] = None

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("severity") is not None:
            data["severity"] = (
                data["severity"].value
                if isinstance(data["severity"], ComplaintSeverity)
                else data["severity"]
            )
        if data.get("status") is not None:
            data["status"] = (
                data["status"].value
                if isinstance(data["status"], ComplaintStatus)
                else data["status"]
            )
        if data.get("source") is not None:
            data["source"] = (
                data["source"].value
                if isinstance(data["source"], ComplaintSource)
                else data["source"]
            )
        if data.get("resolution_outcome") is not None:
            data["resolution_outcome"] = (
                data["resolution_outcome"].value
                if isinstance(data["resolution_outcome"], ComplaintResolutionOutcome)
                else data["resolution_outcome"]
            )
        return data


class Complaint(ComplaintCreate):
    """Full complaint model with audit fields."""

    class Config:
        from_attributes = True
