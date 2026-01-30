"""
Member domain models for Brickwell Health Simulator.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from brickwell_health.domain.enums import Gender, MaritalStatus, MemberChangeType


class MemberCreate(BaseModel):
    """Model for creating a new member."""

    member_id: UUID
    member_number: str = Field(..., max_length=20)
    title: Optional[str] = Field(None, max_length=10)
    first_name: str = Field(..., max_length=100)
    middle_name: Optional[str] = Field(None, max_length=100)
    last_name: str = Field(..., max_length=100)
    preferred_name: Optional[str] = Field(None, max_length=100)
    date_of_birth: date
    gender: Gender

    # Medicare
    medicare_number: Optional[str] = Field(None, max_length=12)
    medicare_irn: Optional[str] = Field(None, max_length=1)
    medicare_expiry_date: Optional[date] = None

    # Address (merged from ADDRESS table)
    address_line_1: Optional[str] = Field(None, max_length=200)
    address_line_2: Optional[str] = Field(None, max_length=200)
    suburb: Optional[str] = Field(None, max_length=100)
    state: str = Field(..., max_length=3)
    postcode: Optional[str] = Field(None, max_length=10)
    country: str = Field(default="AUS", max_length=3)

    # Contact (merged from CONTACT table)
    email: Optional[str] = Field(None, max_length=200)
    mobile_phone: Optional[str] = Field(None, max_length=20)
    home_phone: Optional[str] = Field(None, max_length=20)

    # Regulatory
    australian_resident: bool = True
    tax_file_number_provided: bool = False
    lhc_applicable: bool = False

    # Demographics
    marital_status: MaritalStatus = MaritalStatus.SINGLE

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: str) -> str:
        """Validate Australian state code."""
        valid_states = {"NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"}
        if v.upper() not in valid_states:
            raise ValueError(f"Invalid state: {v}. Must be one of {valid_states}")
        return v.upper()

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["gender"] = data["gender"].value if isinstance(data["gender"], Gender) else data["gender"]
        data["marital_status"] = data["marital_status"].value if isinstance(data["marital_status"], MaritalStatus) else data["marital_status"]
        return data


class Member(MemberCreate):
    """Full member model with audit fields."""

    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None
    deceased_flag: bool = False
    deceased_date: Optional[date] = None

    class Config:
        from_attributes = True


class LHCLoadingCreate(BaseModel):
    """Model for creating LHC loading record."""

    lhc_loading_id: UUID
    member_id: UUID
    policy_id: UUID
    certified_age_of_entry: int = Field(..., ge=0, le=100)
    base_day: date
    loading_percentage: Decimal = Field(..., ge=0, le=70)
    loading_start_date: date
    loading_removal_date: Optional[date] = None
    continuous_cover_start: Optional[date] = None
    years_without_cover: int = Field(default=0, ge=0)
    is_loading_active: bool = True
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class AgeBasedDiscountCreate(BaseModel):
    """Model for creating age-based discount record."""

    age_discount_id: UUID
    member_id: UUID
    policy_id: UUID
    age_at_eligibility: int = Field(..., ge=18, le=29)
    discount_percentage: Decimal = Field(..., ge=0, le=10)
    eligibility_date: date
    phase_out_start_date: date
    phase_out_end_date: date
    current_discount_pct: Decimal = Field(..., ge=0, le=10)
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class PHIRebateEntitlementCreate(BaseModel):
    """Model for creating PHI rebate entitlement record."""

    rebate_entitlement_id: UUID
    policy_id: UUID
    financial_year: str = Field(..., max_length=9)  # e.g., "2024-2025"
    income_tier: str = Field(..., max_length=10)  # Tier 0/1/2/3
    oldest_member_age_bracket: str = Field(..., max_length=20)
    rebate_percentage: Decimal = Field(..., ge=0, le=1)
    income_declaration_date: Optional[date] = None
    declared_income_range: Optional[str] = None
    single_or_family: str = Field(..., max_length=10)
    mls_liable: bool = False
    effective_date: date
    end_date: Optional[date] = None
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class MemberUpdate(BaseModel):
    """
    Model for member update/change event.

    Tracks all changes to member data for audit trail and downstream processing.
    """

    member_update_id: UUID
    member_id: UUID
    change_type: MemberChangeType
    change_date: date

    # Previous and new values (stored as JSON for flexibility)
    previous_values: dict[str, Any] = Field(default_factory=dict)
    new_values: dict[str, Any] = Field(default_factory=dict)

    # Change context
    reason: Optional[str] = Field(None, max_length=200)
    triggered_by: Optional[str] = Field(None, max_length=50)  # "SIMULATION", "POLICY_EVENT", etc.

    # Audit
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        import json
        data = self.model_dump()
        data["change_type"] = data["change_type"].value if isinstance(data["change_type"], MemberChangeType) else data["change_type"]
        # Convert dicts to JSON strings for JSONB columns
        data["previous_values"] = json.dumps(data["previous_values"])
        data["new_values"] = json.dumps(data["new_values"])
        return data
