"""
Commission / acquisition cost domain models for IFRS 17 DAC tracking.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class AcquisitionCostCreate(BaseModel):
    """Model for creating an acquisition cost (commission/DAC) record."""

    acquisition_cost_id: UUID
    policy_id: UUID

    commission_type: str = Field(default="Upfront", max_length=30)
    distribution_channel: str = Field(..., max_length=20)

    gross_written_premium: Decimal = Field(..., ge=0)
    commission_rate: Decimal = Field(..., ge=0)
    commission_amount: Decimal = Field(..., ge=0)

    incurred_date: date
    amortisation_start_date: date
    amortisation_end_date: date

    status: str = Field(default="Active", max_length=20)
    clawback_date: Optional[date] = None
    clawback_amount: Optional[Decimal] = None

    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        return self.model_dump()
