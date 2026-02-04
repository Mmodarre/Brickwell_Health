"""
Policy domain models for Brickwell Health Simulator.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    PolicyStatus,
    PolicyType,
    MemberRole,
    RelationshipType,
    DistributionChannel,
    RebateTier,
)


class PolicyCreate(BaseModel):
    """Model for creating a new policy."""

    policy_id: UUID
    policy_number: str = Field(..., max_length=25)
    application_id: Optional[UUID] = None
    product_id: int

    policy_status: PolicyStatus = PolicyStatus.ACTIVE
    policy_type: PolicyType

    effective_date: date
    end_date: Optional[date] = None
    cancellation_reason: Optional[str] = None

    payment_frequency: str = Field(default="Monthly", max_length=20)
    premium_amount: Decimal = Field(..., ge=0)
    excess_amount: Optional[Decimal] = None

    government_rebate_tier: Optional[str] = None
    rebate_claimed_as: Optional[str] = None  # ReducedPremium/TaxReturn

    distribution_channel: DistributionChannel
    state_of_residence: str = Field(..., max_length=3)

    original_join_date: date
    previous_fund_code: Optional[str] = None
    transfer_certificate_date: Optional[date] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        # Convert enums to string values
        data["policy_status"] = data["policy_status"].value if isinstance(data["policy_status"], PolicyStatus) else data["policy_status"]
        data["policy_type"] = data["policy_type"].value if isinstance(data["policy_type"], PolicyType) else data["policy_type"]
        data["distribution_channel"] = data["distribution_channel"].value if isinstance(data["distribution_channel"], DistributionChannel) else data["distribution_channel"]
        return data


class Policy(PolicyCreate):
    """Full policy model with audit fields."""

    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    class Config:
        from_attributes = True


class PolicyMemberCreate(BaseModel):
    """Model for linking a member to a policy."""

    policy_member_id: UUID
    policy_id: UUID
    member_id: UUID

    member_role: MemberRole
    relationship_to_primary: RelationshipType

    effective_date: date
    end_date: Optional[date] = None
    is_active: bool = True

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["member_role"] = data["member_role"].value if isinstance(data["member_role"], MemberRole) else data["member_role"]
        data["relationship_to_primary"] = data["relationship_to_primary"].value if isinstance(data["relationship_to_primary"], RelationshipType) else data["relationship_to_primary"]
        return data


class SuspensionCreate(BaseModel):
    """Model for creating a policy suspension."""

    suspension_id: UUID
    policy_id: UUID
    suspension_type: str = Field(..., max_length=30)
    start_date: date
    expected_end_date: Optional[date] = None
    actual_end_date: Optional[date] = None
    reason: Optional[str] = None
    status: str = Field(default="Active", max_length=20)
    max_suspension_days: int = Field(default=730)
    days_used: int = Field(default=0, ge=0)
    waiting_period_impact: bool = False

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class UpgradeRequestCreate(BaseModel):
    """Model for creating an upgrade/downgrade request."""

    upgrade_request_id: UUID
    policy_id: UUID
    request_type: str = Field(..., max_length=20)  # Upgrade/Downgrade/ChangeExcess
    current_product_id: int
    requested_product_id: int
    current_excess: Optional[Decimal] = None
    requested_excess: Optional[Decimal] = None
    requested_effective_date: date
    request_reason: Optional[str] = None
    request_status: str = Field(default="Approved", max_length=20)
    submission_date: datetime
    decision_date: Optional[datetime] = None
    decision_by: Optional[str] = None
    requires_waiting_period: Optional[bool] = None
    waiting_period_details: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
