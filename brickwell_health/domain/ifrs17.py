"""
IFRS 17 / PAA LRC domain models.

Covers cohort dimension and the three fact tables produced by the post-simulation
engine: monthly balances (point-in-time state), monthly movements (P&L / roll-
forward flows), and onerous assessments (combined-ratio evaluation output).
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class IFRS17Cohort(BaseModel):
    """Cohort dimension row (portfolio x Australian Financial Year)."""

    cohort_id: str = Field(..., max_length=30)
    portfolio: str = Field(..., max_length=20)
    afy_label: str = Field(..., max_length=10)
    afy_start_date: date
    afy_end_date: date
    is_onerous_at_inception: bool = False
    onerous_first_detected_month: Optional[date] = None
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        return self.model_dump()


class IFRS17MonthlyBalance(BaseModel):
    """Point-in-time LRC / LIC / DAC balance per (cohort, reporting_month)."""

    monthly_balance_id: UUID
    cohort_id: str
    reporting_month: date

    policy_count: int = 0
    in_force_premium: Decimal = Decimal("0")

    lrc_excl_loss_component: Decimal = Decimal("0")
    loss_component: Decimal = Decimal("0")
    lrc_total: Decimal = Decimal("0")

    lic_best_estimate: Decimal = Decimal("0")
    lic_risk_adjustment: Decimal = Decimal("0")
    lic_ibnr: Decimal = Decimal("0")
    lic_total: Decimal = Decimal("0")

    deferred_acquisition_cost: Decimal = Decimal("0")

    is_onerous: bool = False
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        return self.model_dump()


class IFRS17MonthlyMovement(BaseModel):
    """P&L / roll-forward movements per (cohort, reporting_month)."""

    monthly_movement_id: UUID
    cohort_id: str
    reporting_month: date

    opening_lrc: Decimal = Decimal("0")
    premiums_received: Decimal = Decimal("0")
    insurance_revenue: Decimal = Decimal("0")
    insurance_service_expense: Decimal = Decimal("0")
    claims_incurred: Decimal = Decimal("0")
    acquisition_cost_amortised: Decimal = Decimal("0")
    loss_component_recognised: Decimal = Decimal("0")
    loss_component_reversed: Decimal = Decimal("0")
    closing_lrc: Decimal = Decimal("0")
    insurance_service_result: Decimal = Decimal("0")

    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        return self.model_dump()


class OnerousAssessment(BaseModel):
    """Combined-ratio evaluation + loss component change per (cohort, month)."""

    assessment_id: UUID
    cohort_id: str
    reporting_month: date

    expected_remaining_premium: Decimal = Decimal("0")
    expected_remaining_claims: Decimal = Decimal("0")
    expected_remaining_expenses: Decimal = Decimal("0")
    expected_combined_ratio: Optional[Decimal] = None

    onerous_threshold_crossed: bool = False
    loss_component_change: Decimal = Decimal("0")
    notes: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        return self.model_dump()


class IFRS17JournalLineCreate(BaseModel):
    """Double-entry IFRS 17 posting keyed to GL dims (one side per row)."""

    journal_line_id: UUID
    cohort_id: str = Field(..., max_length=30)
    reporting_month: date
    gl_period_id: int
    gl_account_id: int
    cost_centre_id: Optional[int] = None
    movement_bucket: str = Field(..., max_length=40)
    debit_amount: Decimal = Decimal("0")
    credit_amount: Decimal = Decimal("0")
    journal_source: str = Field(default="IFRS17_ENGINE", max_length=20)
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        return self.model_dump()
