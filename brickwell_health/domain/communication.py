"""
Communication Domain Models for Brickwell Health Simulator.

Models for Communication, Communication Preference, Campaign, and Campaign Response.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    CampaignResponseType,
    CampaignStatus,
    CampaignType,
    CommunicationDeliveryStatus,
    CommunicationType,
    ConversionType,
    PreferenceType,
    TriggerEventType,
)


# ============================================================================
# COMMUNICATION PREFERENCE MODELS
# ============================================================================

class CommunicationPreferenceCreate(BaseModel):
    """Model for creating a communication preference."""

    preference_id: UUID

    # Relationships
    member_id: UUID
    policy_id: UUID

    # Preference Details
    preference_type: PreferenceType
    channel: str = Field(..., max_length=20)

    # Consent Status
    is_opted_in: bool = True
    opt_in_date: Optional[date] = None
    opt_out_date: Optional[date] = None

    # Preferences
    preferred_time: Optional[str] = Field(None, max_length=20)

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("preference_type") is not None:
            data["preference_type"] = (
                data["preference_type"].value
                if isinstance(data["preference_type"], PreferenceType)
                else data["preference_type"]
            )
        return data


class CommunicationPreference(CommunicationPreferenceCreate):
    """Full communication preference model."""

    class Config:
        from_attributes = True


# ============================================================================
# CAMPAIGN MODELS
# ============================================================================

class CampaignCreate(BaseModel):
    """Model for creating a campaign."""

    campaign_id: UUID
    campaign_code: str = Field(..., max_length=30)

    # Details
    campaign_name: str = Field(..., max_length=100)
    campaign_type: CampaignType
    description: Optional[str] = Field(None, max_length=1000)

    # Timeline
    start_date: date
    end_date: Optional[date] = None

    # Status
    status: CampaignStatus = CampaignStatus.ACTIVE

    # Targeting
    target_audience: Optional[str] = Field(None, max_length=500)
    target_segment: Optional[str] = Field(None, max_length=100)

    # Budget
    budget: Optional[Decimal] = Field(None, ge=0)
    actual_spend: Optional[Decimal] = Field(None, ge=0)

    # Performance Metrics
    target_response_rate: Optional[Decimal] = Field(None, ge=0, le=1)
    actual_response_rate: Optional[Decimal] = Field(None, ge=0, le=1)
    target_conversion_rate: Optional[Decimal] = Field(None, ge=0, le=1)
    actual_conversion_rate: Optional[Decimal] = Field(None, ge=0, le=1)

    # Counts
    members_targeted: int = Field(default=0, ge=0)
    communications_sent: int = Field(default=0, ge=0)
    responses_received: int = Field(default=0, ge=0)
    conversions: int = Field(default=0, ge=0)

    # Owner
    owner: Optional[str] = Field(None, max_length=50)

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("campaign_type") is not None:
            data["campaign_type"] = (
                data["campaign_type"].value
                if isinstance(data["campaign_type"], CampaignType)
                else data["campaign_type"]
            )
        if data.get("status") is not None:
            data["status"] = (
                data["status"].value
                if isinstance(data["status"], CampaignStatus)
                else data["status"]
            )
        return data


class Campaign(CampaignCreate):
    """Full campaign model."""

    class Config:
        from_attributes = True


# ============================================================================
# COMMUNICATION MODELS
# ============================================================================

class CommunicationCreate(BaseModel):
    """Model for creating a communication record."""

    communication_id: UUID
    communication_reference: str = Field(..., max_length=30)

    # Relationships
    policy_id: UUID
    member_id: UUID
    campaign_id: Optional[UUID] = None

    # Type & Channel
    communication_type: CommunicationType
    template_code: Optional[str] = Field(None, max_length=50)

    # Content
    subject: Optional[str] = Field(None, max_length=200)

    # Recipient
    recipient_email: Optional[str] = Field(None, max_length=200)
    recipient_phone: Optional[str] = Field(None, max_length=20)

    # Timing
    scheduled_date: Optional[datetime] = None
    sent_date: Optional[datetime] = None

    # Status Tracking
    delivery_status: CommunicationDeliveryStatus = CommunicationDeliveryStatus.PENDING
    delivery_status_date: Optional[datetime] = None

    # Engagement
    opened_date: Optional[datetime] = None
    clicked_date: Optional[datetime] = None

    # Trigger Context
    trigger_event_type: Optional[TriggerEventType] = None
    trigger_event_id: Optional[UUID] = None

    # Related Entities
    claim_id: Optional[UUID] = None
    invoice_id: Optional[UUID] = None
    interaction_id: Optional[UUID] = None

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("communication_type") is not None:
            data["communication_type"] = (
                data["communication_type"].value
                if isinstance(data["communication_type"], CommunicationType)
                else data["communication_type"]
            )
        if data.get("delivery_status") is not None:
            data["delivery_status"] = (
                data["delivery_status"].value
                if isinstance(data["delivery_status"], CommunicationDeliveryStatus)
                else data["delivery_status"]
            )
        if data.get("trigger_event_type") is not None:
            data["trigger_event_type"] = (
                data["trigger_event_type"].value
                if isinstance(data["trigger_event_type"], TriggerEventType)
                else data["trigger_event_type"]
            )
        return data


class Communication(CommunicationCreate):
    """Full communication model."""

    class Config:
        from_attributes = True


# ============================================================================
# CAMPAIGN RESPONSE MODELS
# ============================================================================

class CampaignResponseCreate(BaseModel):
    """Model for creating a campaign response."""

    response_id: UUID

    # Relationships
    campaign_id: UUID
    member_id: UUID
    policy_id: UUID
    communication_id: Optional[UUID] = None

    # Response Details
    response_type: CampaignResponseType
    response_date: datetime

    # Conversion Details
    conversion_type: Optional[ConversionType] = None
    conversion_value: Optional[Decimal] = Field(None, ge=0)

    # Channel
    response_channel: Optional[str] = Field(None, max_length=20)

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("response_type") is not None:
            data["response_type"] = (
                data["response_type"].value
                if isinstance(data["response_type"], CampaignResponseType)
                else data["response_type"]
            )
        if data.get("conversion_type") is not None:
            data["conversion_type"] = (
                data["conversion_type"].value
                if isinstance(data["conversion_type"], ConversionType)
                else data["conversion_type"]
            )
        return data


class CampaignResponse(CampaignResponseCreate):
    """Full campaign response model."""

    class Config:
        from_attributes = True
