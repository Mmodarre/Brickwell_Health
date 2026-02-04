"""
Digital Behavior Domain Models for Brickwell Health Simulator.

Models for Web Session and Digital Event entities.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    DeviceType,
    DigitalEventType,
    PageCategory,
    SessionType,
    TriggerEventType,
)


# ============================================================================
# WEB SESSION MODELS
# ============================================================================

class WebSessionCreate(BaseModel):
    """Model for creating a web session."""

    session_id: UUID

    # Relationships (member_id required for authenticated sessions)
    member_id: UUID
    policy_id: Optional[UUID] = None

    # Session Details
    session_start: datetime
    session_end: Optional[datetime] = None
    duration_seconds: Optional[int] = Field(None, ge=0)

    # Engagement Metrics
    page_count: int = Field(default=0, ge=0)
    event_count: int = Field(default=0, ge=0)

    # Device & Browser
    device_type: Optional[DeviceType] = None
    browser: Optional[str] = Field(None, max_length=50)
    operating_system: Optional[str] = Field(None, max_length=50)

    # Entry/Exit
    entry_page: Optional[str] = Field(None, max_length=200)
    exit_page: Optional[str] = Field(None, max_length=200)
    referrer: Optional[str] = Field(None, max_length=200)

    # Authentication
    is_authenticated: bool = True

    # Session Type
    session_type: Optional[SessionType] = None

    # Intent Signals
    viewed_cancel_page: bool = False
    viewed_upgrade_page: bool = False
    viewed_claims_page: bool = False
    viewed_billing_page: bool = False
    viewed_compare_page: bool = False

    # Trigger Context
    trigger_event_type: Optional[TriggerEventType] = None
    trigger_event_id: Optional[UUID] = None

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        for field in ["device_type", "session_type", "trigger_event_type"]:
            if data.get(field) is not None:
                val = data[field]
                data[field] = val.value if hasattr(val, "value") else val
        return data


class WebSession(WebSessionCreate):
    """Full web session model."""

    class Config:
        from_attributes = True


# ============================================================================
# DIGITAL EVENT MODELS
# ============================================================================

class DigitalEventCreate(BaseModel):
    """Model for creating a digital event."""

    event_id: UUID

    # Session Context
    session_id: UUID

    # Member (denormalized for performance)
    member_id: UUID

    # Event Details
    event_timestamp: datetime
    event_type: DigitalEventType

    # Page Context
    page_path: Optional[str] = Field(None, max_length=200)
    page_category: Optional[PageCategory] = None
    page_title: Optional[str] = Field(None, max_length=200)

    # Element Details (for clicks)
    element_id: Optional[str] = Field(None, max_length=100)
    element_text: Optional[str] = Field(None, max_length=200)

    # Search Details
    search_query: Optional[str] = Field(None, max_length=200)
    search_results_count: Optional[int] = Field(None, ge=0)

    # Form Details
    form_name: Optional[str] = Field(None, max_length=100)
    form_field: Optional[str] = Field(None, max_length=100)
    form_completed: Optional[bool] = None

    # Event Sequence
    event_sequence: Optional[int] = Field(None, ge=1)

    # Timing
    time_on_page_seconds: Optional[int] = Field(None, ge=0)

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        for field in ["event_type", "page_category"]:
            if data.get(field) is not None:
                val = data[field]
                data[field] = val.value if hasattr(val, "value") else val
        return data


class DigitalEvent(DigitalEventCreate):
    """Full digital event model."""

    class Config:
        from_attributes = True
