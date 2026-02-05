"""
NBA (Next Best Action) Domain Models for Brickwell Health Simulator.

Defines Pydantic models for NBA action catalog, recommendations, and executions.
"""

import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


# ============================================================================
# NBA ENUMS
# ============================================================================


class BusinessIssue(str, Enum):
    """Business issue categories for NBA actions."""

    ACQUIRE = "Acquire"
    GROW = "Grow"
    RETAIN = "Retain"
    SERVICE = "Service"


class ActionCategory(str, Enum):
    """Action categories for NBA."""

    RETENTION = "Retention"
    UPSELL = "Upsell"
    CROSS_SELL = "CrossSell"
    SERVICE = "Service"
    WELLNESS = "Wellness"


class NBAChannel(str, Enum):
    """Channels for NBA action execution."""

    EMAIL = "Email"
    SMS = "SMS"
    PHONE = "Phone"
    IN_APP = "InApp"
    LETTER = "Letter"
    WEB = "Web"


class RecommendationStatus(str, Enum):
    """Status of NBA recommendations."""

    PENDING = "pending"
    SCHEDULED = "scheduled"
    EXECUTED = "executed"
    EXPIRED = "expired"
    SUPPRESSED = "suppressed"


class ImmediateResponse(str, Enum):
    """Immediate response types for action execution."""

    DELIVERED = "Delivered"
    OPENED = "Opened"
    CLICKED = "Clicked"
    IGNORED = "Ignored"
    FAILED = "Failed"
    ANSWERED = "Answered"
    VOICEMAIL = "Voicemail"
    NO_ANSWER = "NoAnswer"


class ExecutionMethod(str, Enum):
    """Methods of action execution."""

    AUTOMATED = "Automated"
    AGENT_ASSISTED = "AgentAssisted"
    SELF_SERVICE = "SelfService"


# ============================================================================
# ACTION CATALOG MODELS
# ============================================================================


class NBAActionCatalogCreate(BaseModel):
    """Model for creating an NBA action catalog entry."""

    action_id: UUID
    action_code: str = Field(..., max_length=50)
    action_name: str = Field(..., max_length=200)
    business_issue: BusinessIssue
    action_category: ActionCategory
    channel: NBAChannel
    description: Optional[str] = None
    eligibility_rules: Optional[dict[str, Any]] = None
    suitability_rules: Optional[dict[str, Any]] = None
    base_business_value: Optional[Decimal] = None
    probability_multiplier: Decimal = Decimal("1.0")
    cooldown_days: int = Field(default=30, ge=0)
    max_attempts: int = Field(default=3, ge=1)
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="NBA_ENGINE", max_length=50)

    def model_dump_db(self) -> dict[str, Any]:
        """Convert to database-ready dictionary."""
        data = self.model_dump()
        data["business_issue"] = self.business_issue.value
        data["action_category"] = self.action_category.value
        data["channel"] = self.channel.value
        # Convert dict to JSON string for JSONB columns
        if data.get("eligibility_rules") is not None:
            data["eligibility_rules"] = json.dumps(data["eligibility_rules"])
        if data.get("suitability_rules") is not None:
            data["suitability_rules"] = json.dumps(data["suitability_rules"])
        return data


class NBAActionCatalog(NBAActionCatalogCreate):
    """Full NBA action catalog model."""

    class Config:
        from_attributes = True


# ============================================================================
# RECOMMENDATION MODELS
# ============================================================================


class NBARecommendationCreate(BaseModel):
    """Model for creating an NBA recommendation."""

    recommendation_id: UUID
    batch_id: UUID
    batch_date: date
    member_id: UUID
    policy_id: Optional[UUID] = None
    action_id: UUID
    propensity_score: Optional[Decimal] = Field(None, ge=0, le=1)
    urgency_score: Optional[Decimal] = Field(None, ge=0, le=1)
    business_value_score: Optional[Decimal] = None
    priority_rank: Optional[int] = Field(None, ge=1)
    final_score: Optional[Decimal] = None
    trigger_reason: Optional[str] = Field(None, max_length=100)
    trigger_signals: Optional[dict[str, Any]] = None
    model_version: Optional[str] = Field(None, max_length=50)
    valid_from: date
    valid_until: date
    status: RecommendationStatus = RecommendationStatus.PENDING
    suppression_reason: Optional[str] = Field(None, max_length=100)
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: Optional[datetime] = None

    def model_dump_db(self) -> dict[str, Any]:
        """Convert to database-ready dictionary."""
        data = self.model_dump()
        data["status"] = self.status.value
        # Convert dict to JSON string for JSONB columns
        if data.get("trigger_signals") is not None:
            data["trigger_signals"] = json.dumps(data["trigger_signals"])
        return data


class NBARecommendation(NBARecommendationCreate):
    """Full NBA recommendation model."""

    class Config:
        from_attributes = True


# ============================================================================
# EXECUTION MODELS
# ============================================================================


class NBAExecutionCreate(BaseModel):
    """Model for recording an NBA action execution."""

    execution_id: UUID
    recommendation_id: Optional[UUID] = None
    action_id: UUID
    member_id: UUID
    policy_id: Optional[UUID] = None
    executed_at: datetime
    execution_channel: NBAChannel
    execution_method: Optional[ExecutionMethod] = None
    communication_id: Optional[UUID] = None
    interaction_id: Optional[UUID] = None
    campaign_response_id: Optional[UUID] = None
    immediate_response: Optional[ImmediateResponse] = None
    response_at: Optional[datetime] = None
    worker_id: Optional[int] = None
    simulation_date: Optional[date] = None
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict[str, Any]:
        """Convert to database-ready dictionary."""
        data = self.model_dump()
        data["execution_channel"] = self.execution_channel.value
        if self.execution_method:
            data["execution_method"] = self.execution_method.value
        if self.immediate_response:
            data["immediate_response"] = self.immediate_response.value
        return data


class NBAExecution(NBAExecutionCreate):
    """Full NBA execution model."""

    class Config:
        from_attributes = True


# ============================================================================
# AGGREGATED MODELS FOR SIMULATION
# ============================================================================


class NBAActionWithRecommendation(BaseModel):
    """
    Combined model for simulation use.

    Contains action catalog info plus recommendation details.
    Used when loading recommendations from database for processing.
    """

    # From recommendation
    recommendation_id: UUID
    member_id: UUID
    policy_id: Optional[UUID] = None
    propensity_score: Optional[Decimal] = None
    urgency_score: Optional[Decimal] = None
    final_score: Optional[Decimal] = None
    priority_rank: Optional[int] = None
    trigger_reason: Optional[str] = None
    trigger_signals: Optional[dict[str, Any]] = None
    valid_from: date
    valid_until: date

    # From action catalog
    action_id: UUID
    action_code: str
    action_name: str
    action_category: ActionCategory
    channel: NBAChannel
    probability_multiplier: Decimal = Decimal("1.0")
    cooldown_days: int = 30
    max_attempts: int = 3

    @property
    def is_retention(self) -> bool:
        """Check if this is a retention action."""
        return self.action_category == ActionCategory.RETENTION

    @property
    def is_upsell(self) -> bool:
        """Check if this is an upsell action."""
        return self.action_category == ActionCategory.UPSELL

    @property
    def is_cross_sell(self) -> bool:
        """Check if this is a cross-sell action."""
        return self.action_category == ActionCategory.CROSS_SELL

    @property
    def is_service(self) -> bool:
        """Check if this is a service action."""
        return self.action_category == ActionCategory.SERVICE

    @property
    def is_wellness(self) -> bool:
        """Check if this is a wellness action."""
        return self.action_category == ActionCategory.WELLNESS

    class Config:
        from_attributes = True
