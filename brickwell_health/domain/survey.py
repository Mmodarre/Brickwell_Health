"""
Survey Domain Models for Brickwell Health Simulator.

Models for NPS and CSAT surveys with pending tables for deferred LLM processing.
"""

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    CSATLabel,
    NPSCategory,
    ProcessingStatus,
    SentimentLabel,
    SurveyChannel,
    SurveyType,
)


# ============================================================================
# NPS SURVEY PENDING MODELS
# ============================================================================


class NPSSurveyPendingCreate(BaseModel):
    """Model for creating a pending NPS survey."""

    pending_id: UUID
    survey_reference: str = Field(..., max_length=30)

    # Relationships
    member_id: UUID
    policy_id: UUID

    # Survey Context
    survey_type: SurveyType
    trigger_event: Optional[str] = Field(None, max_length=50)
    trigger_entity_id: Optional[UUID] = None
    claim_id: Optional[UUID] = None
    interaction_id: Optional[UUID] = None

    # Timing
    simulation_date: date
    sent_datetime: datetime

    # Response Prediction (pre-calculated during simulation)
    will_respond: bool
    response_probability: Optional[Decimal] = Field(None, ge=0, le=1)
    completed_datetime: Optional[datetime] = None
    response_time_minutes: Optional[int] = Field(None, ge=0)

    # LLM Context
    llm_context: dict[str, Any]

    # Prior Survey Context
    prior_surveys_context: Optional[dict[str, Any]] = None

    # Processing Status
    processing_status: ProcessingStatus = ProcessingStatus.PENDING
    processing_order: Optional[int] = None
    processed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = Field(default=0, ge=0)

    # Final Survey ID
    final_survey_id: Optional[UUID] = None

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("survey_type"):
            data["survey_type"] = (
                data["survey_type"].value
                if hasattr(data["survey_type"], "value")
                else data["survey_type"]
            )
        if data.get("processing_status"):
            data["processing_status"] = (
                data["processing_status"].value
                if hasattr(data["processing_status"], "value")
                else data["processing_status"]
            )
        # Convert dict to JSON string for JSONB columns
        if data.get("llm_context") is not None:
            data["llm_context"] = json.dumps(data["llm_context"])
        if data.get("prior_surveys_context") is not None:
            data["prior_surveys_context"] = json.dumps(data["prior_surveys_context"])
        return data


class NPSSurveyPending(NPSSurveyPendingCreate):
    """Full NPS survey pending model."""

    pass


# ============================================================================
# NPS SURVEY MODELS
# ============================================================================


class NPSSurveyCreate(BaseModel):
    """Model for creating an NPS survey response."""

    survey_id: UUID
    survey_reference: str = Field(..., max_length=30)

    # Relationships
    member_id: UUID
    policy_id: UUID

    # Survey Context
    survey_type: SurveyType
    trigger_event: Optional[str] = Field(None, max_length=50)
    trigger_entity_id: Optional[UUID] = None
    claim_id: Optional[UUID] = None
    interaction_id: Optional[UUID] = None

    # Survey Lifecycle
    sent_date: datetime
    completed_date: Optional[datetime] = None

    # Q1: Core NPS Score
    nps_score: Optional[int] = Field(None, ge=0, le=10)
    nps_category: Optional[NPSCategory] = None

    # Q2: Verbatim Feedback
    feedback_text: Optional[str] = Field(None, max_length=2000)
    feedback_improvement: Optional[str] = Field(None, max_length=1000)

    # Q3-7: Driver Scores
    driver_claims_processing: Optional[int] = Field(None, ge=0, le=10)
    driver_customer_service: Optional[int] = Field(None, ge=0, le=10)
    driver_value_for_money: Optional[int] = Field(None, ge=0, le=10)
    driver_coverage_clarity: Optional[int] = Field(None, ge=0, le=10)
    driver_digital_experience: Optional[int] = Field(None, ge=0, le=10)

    # Sentiment Analysis
    sentiment_score: Optional[Decimal] = Field(None, ge=-1, le=1)
    sentiment_label: Optional[SentimentLabel] = None
    feedback_themes: Optional[str] = Field(None, max_length=500)

    # Survey Metadata
    survey_channel: Optional[SurveyChannel] = None
    response_time_minutes: Optional[int] = Field(None, ge=0)
    follow_up_consent: Optional[bool] = None

    # Processing Info
    pending_id: Optional[UUID] = None

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        for field in [
            "survey_type",
            "nps_category",
            "sentiment_label",
            "survey_channel",
        ]:
            if data.get(field):
                data[field] = (
                    data[field].value
                    if hasattr(data[field], "value")
                    else data[field]
                )
        return data


class NPSSurvey(NPSSurveyCreate):
    """Full NPS survey model."""

    pass


# ============================================================================
# CSAT SURVEY PENDING MODELS
# ============================================================================


class CSATSurveyPendingCreate(BaseModel):
    """Model for creating a pending CSAT survey."""

    pending_id: UUID
    survey_reference: str = Field(..., max_length=30)

    # Relationships
    member_id: UUID
    policy_id: UUID

    # Survey Context
    survey_type: SurveyType
    interaction_id: Optional[UUID] = None
    case_id: Optional[UUID] = None

    # Timing
    simulation_date: date
    sent_datetime: datetime

    # Response Prediction (pre-calculated during simulation)
    will_respond: bool
    response_probability: Optional[Decimal] = Field(None, ge=0, le=1)
    completed_datetime: Optional[datetime] = None
    response_time_minutes: Optional[int] = Field(None, ge=0)

    # LLM Context
    llm_context: dict[str, Any]

    # Processing Status
    processing_status: ProcessingStatus = ProcessingStatus.PENDING
    processing_order: Optional[int] = None
    processed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = Field(default=0, ge=0)

    # Final Survey ID
    final_survey_id: Optional[UUID] = None

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        if data.get("survey_type"):
            data["survey_type"] = (
                data["survey_type"].value
                if hasattr(data["survey_type"], "value")
                else data["survey_type"]
            )
        if data.get("processing_status"):
            data["processing_status"] = (
                data["processing_status"].value
                if hasattr(data["processing_status"], "value")
                else data["processing_status"]
            )
        # Convert dict to JSON string for JSONB columns
        if data.get("llm_context") is not None:
            data["llm_context"] = json.dumps(data["llm_context"])
        return data


class CSATSurveyPending(CSATSurveyPendingCreate):
    """Full CSAT survey pending model."""

    pass


# ============================================================================
# CSAT SURVEY MODELS
# ============================================================================


class CSATSurveyCreate(BaseModel):
    """Model for creating a CSAT survey response."""

    survey_id: UUID
    survey_reference: str = Field(..., max_length=30)

    # Relationships
    member_id: UUID
    policy_id: UUID

    # Survey Context
    survey_type: SurveyType
    interaction_id: Optional[UUID] = None
    case_id: Optional[UUID] = None

    # Survey Lifecycle
    sent_date: datetime
    completed_date: Optional[datetime] = None

    # CSAT Response
    csat_score: Optional[int] = Field(None, ge=1, le=5)
    csat_label: Optional[CSATLabel] = None

    # Additional Questions
    effort_score: Optional[int] = Field(None, ge=1, le=5)
    recommend_agent: Optional[bool] = None

    # Verbatim
    feedback_text: Optional[str] = Field(None, max_length=1000)

    # Sentiment
    sentiment_label: Optional[SentimentLabel] = None

    # Survey Channel
    survey_channel: Optional[SurveyChannel] = None
    response_time_minutes: Optional[int] = Field(None, ge=0)

    # Processing Info
    pending_id: Optional[UUID] = None

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to their values
        for field in ["survey_type", "csat_label", "sentiment_label", "survey_channel"]:
            if data.get(field):
                data[field] = (
                    data[field].value
                    if hasattr(data[field], "value")
                    else data[field]
                )
        return data


class CSATSurvey(CSATSurveyCreate):
    """Full CSAT survey model."""

    pass


# ============================================================================
# LLM RESPONSE MODELS
# ============================================================================


class NPSSurveyLLMResponse(BaseModel):
    """Schema for LLM-generated NPS survey response."""

    nps_score: int = Field(..., ge=0, le=10)
    driver_claims_processing: int = Field(..., ge=0, le=10)
    driver_customer_service: int = Field(..., ge=0, le=10)
    driver_value_for_money: int = Field(..., ge=0, le=10)
    driver_coverage_clarity: int = Field(..., ge=0, le=10)
    driver_digital_experience: int = Field(..., ge=0, le=10)

    feedback_text: str = Field(..., max_length=2000)
    feedback_improvement: Optional[str] = Field(None, max_length=1000)

    sentiment_score: float = Field(..., ge=-1, le=1)
    sentiment_label: str
    feedback_themes: list[str]

    follow_up_consent: bool


class CSATSurveyLLMResponse(BaseModel):
    """Schema for LLM-generated CSAT survey response."""

    csat_score: int = Field(..., ge=1, le=5)
    effort_score: int = Field(..., ge=1, le=5)
    recommend_agent: bool

    feedback_text: str = Field(..., max_length=1000)
    sentiment_label: str
