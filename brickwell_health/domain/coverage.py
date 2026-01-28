"""
Coverage domain models for Brickwell Health Simulator.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    CoverageType,
    CoverageTier,
    WaitingPeriodType,
    WaitingPeriodStatus,
)


class CoverageCreate(BaseModel):
    """Model for creating a coverage record."""

    coverage_id: UUID
    policy_id: UUID

    coverage_type: CoverageType
    product_id: int

    effective_date: date
    end_date: Optional[date] = None
    status: str = Field(default="Active", max_length=20)

    tier: Optional[CoverageTier] = None  # Hospital only
    excess_amount: Optional[Decimal] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["coverage_type"] = data["coverage_type"].value if isinstance(data["coverage_type"], CoverageType) else data["coverage_type"]
        if data["tier"]:
            data["tier"] = data["tier"].value if isinstance(data["tier"], CoverageTier) else data["tier"]
        return data


class Coverage(CoverageCreate):
    """Full coverage model with audit fields."""

    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    class Config:
        from_attributes = True


class WaitingPeriodCreate(BaseModel):
    """Model for creating a waiting period record."""

    waiting_period_id: UUID
    policy_member_id: UUID
    coverage_id: UUID

    waiting_period_type: WaitingPeriodType
    benefit_category_id: Optional[int] = None
    clinical_category_id: Optional[int] = None

    start_date: date
    end_date: date
    duration_months: int = Field(..., ge=0, le=24)

    status: WaitingPeriodStatus = WaitingPeriodStatus.IN_PROGRESS
    waiver_reason: Optional[str] = None

    # Merged from WAITING_PERIOD_EXEMPTION
    exemption_granted: bool = False
    exemption_type: Optional[str] = None
    exemption_reason: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["waiting_period_type"] = data["waiting_period_type"].value if isinstance(data["waiting_period_type"], WaitingPeriodType) else data["waiting_period_type"]
        data["status"] = data["status"].value if isinstance(data["status"], WaitingPeriodStatus) else data["status"]
        return data


class BenefitUsageCreate(BaseModel):
    """Model for tracking benefit usage."""

    benefit_usage_id: UUID
    policy_id: UUID
    member_id: UUID
    claim_id: Optional[UUID] = None

    benefit_category_id: int
    benefit_year: str  # Australian financial year (e.g., "2024-2025")

    usage_date: date
    usage_amount: Decimal = Field(..., ge=0)
    usage_count: int = Field(default=1, ge=1)

    annual_limit: Optional[Decimal] = None
    remaining_limit: Optional[Decimal] = None
    limit_type: Optional[str] = None  # Dollar/Service/Days

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
