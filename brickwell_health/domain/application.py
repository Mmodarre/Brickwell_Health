"""
Application domain models for Brickwell Health Simulator.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    ApplicationStatus,
    ApplicationType,
    PolicyType,
    DistributionChannel,
    MemberRole,
    RelationshipType,
    Gender,
)


class ApplicationCreate(BaseModel):
    """Model for creating a new application."""

    application_id: UUID
    application_number: str = Field(..., max_length=25)
    application_type: ApplicationType = ApplicationType.NEW
    application_status: ApplicationStatus = ApplicationStatus.PENDING
    product_id: int
    requested_policy_type: PolicyType
    requested_excess: Optional[Decimal] = None
    requested_start_date: date
    channel: DistributionChannel
    previous_fund_code: Optional[str] = None
    transfer_certificate_received: bool = False
    submission_date: datetime
    decision_date: Optional[datetime] = None
    decision_by: Optional[str] = None
    decline_reason: Optional[str] = None
    state: str = Field(..., max_length=3)

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["application_type"] = data["application_type"].value if isinstance(data["application_type"], ApplicationType) else data["application_type"]
        data["application_status"] = data["application_status"].value if isinstance(data["application_status"], ApplicationStatus) else data["application_status"]
        data["requested_policy_type"] = data["requested_policy_type"].value if isinstance(data["requested_policy_type"], PolicyType) else data["requested_policy_type"]
        data["channel"] = data["channel"].value if isinstance(data["channel"], DistributionChannel) else data["channel"]
        return data


class Application(ApplicationCreate):
    """Full application model with audit fields."""

    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    class Config:
        from_attributes = True


class ApplicationMemberCreate(BaseModel):
    """Model for members on an application."""

    application_member_id: UUID
    application_id: UUID
    member_role: MemberRole
    title: Optional[str] = Field(None, max_length=10)
    first_name: str = Field(..., max_length=100)
    middle_name: Optional[str] = Field(None, max_length=100)
    last_name: str = Field(..., max_length=100)
    date_of_birth: date
    gender: Gender
    relationship_to_primary: Optional[RelationshipType] = None
    medicare_number: Optional[str] = Field(None, max_length=12)
    medicare_irn: Optional[str] = Field(None, max_length=1)
    email: Optional[str] = Field(None, max_length=200)
    mobile_phone: Optional[str] = Field(None, max_length=20)
    existing_member_id: Optional[UUID] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["member_role"] = data["member_role"].value if isinstance(data["member_role"], MemberRole) else data["member_role"]
        data["gender"] = data["gender"].value if isinstance(data["gender"], Gender) else data["gender"]
        if data["relationship_to_primary"]:
            data["relationship_to_primary"] = data["relationship_to_primary"].value if isinstance(data["relationship_to_primary"], RelationshipType) else data["relationship_to_primary"]
        return data


class HealthDeclarationCreate(BaseModel):
    """Model for health declaration responses."""

    health_declaration_id: UUID
    application_member_id: UUID
    application_id: UUID
    question_code: str = Field(..., max_length=20)
    question_text: str = Field(..., max_length=500)
    response: str = Field(..., max_length=10)  # Yes/No
    response_details: Optional[str] = Field(None, max_length=2000)
    declaration_date: datetime
    declaration_acknowledged: bool = True

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
