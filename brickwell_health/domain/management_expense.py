"""Management expense domain models."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class FinanceJournalLineCreate(BaseModel):
    """Double-entry management expense GL posting (one side per row)."""

    journal_line_id: UUID
    reporting_month: date
    gl_period_id: int
    gl_account_id: int
    cost_centre_id: Optional[int] = None
    expense_category: str = Field(..., max_length=60)
    debit_amount: Decimal = Decimal("0")
    credit_amount: Decimal = Decimal("0")
    journal_source: str = Field(default="MGMT_EXPENSE_ENGINE", max_length=30)
    description: Optional[str] = Field(default=None, max_length=200)
    created_at: datetime = Field(default_factory=datetime.now)

    def model_dump_db(self) -> dict:
        """Serialise for BatchWriter / database insertion."""
        d = {}
        for k, v in self.model_dump().items():
            if isinstance(v, UUID):
                d[k] = str(v)
            elif isinstance(v, Decimal):
                d[k] = float(v)
            else:
                d[k] = v
        return d
