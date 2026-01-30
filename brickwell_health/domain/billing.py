"""
Billing domain models for Brickwell Health Simulator.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    InvoiceStatus,
    PaymentMethod,
    PaymentStatus,
)


class InvoiceCreate(BaseModel):
    """Model for creating an invoice."""

    invoice_id: UUID
    invoice_number: str = Field(..., max_length=20)

    policy_id: UUID

    invoice_date: date
    due_date: date
    period_start: date
    period_end: date

    invoice_status: InvoiceStatus = InvoiceStatus.ISSUED

    # Amounts
    gross_premium: Decimal = Field(..., ge=0)
    lhc_loading_amount: Decimal = Field(default=Decimal("0"))
    age_discount_amount: Decimal = Field(default=Decimal("0"))
    rebate_amount: Decimal = Field(default=Decimal("0"))
    other_adjustments: Decimal = Field(default=Decimal("0"))
    net_amount: Decimal = Field(..., ge=0)

    gst_amount: Decimal = Field(default=Decimal("0"))
    total_amount: Decimal = Field(..., ge=0)

    paid_amount: Decimal = Field(default=Decimal("0"))
    balance_due: Optional[Decimal] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["invoice_status"] = data["invoice_status"].value if isinstance(data["invoice_status"], InvoiceStatus) else data["invoice_status"]
        return data


class Invoice(InvoiceCreate):
    """Full invoice model with audit fields."""

    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    class Config:
        from_attributes = True


class PaymentCreate(BaseModel):
    """Model for creating a payment record."""

    payment_id: UUID
    payment_number: str = Field(..., max_length=20)

    policy_id: UUID
    invoice_id: Optional[UUID] = None

    payment_date: date
    payment_amount: Decimal = Field(..., ge=0)

    payment_method: PaymentMethod
    payment_status: PaymentStatus = PaymentStatus.PENDING  # Changed from COMPLETED for lifecycle transitions

    bank_reference: Optional[str] = Field(None, max_length=50)

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["payment_method"] = data["payment_method"].value if isinstance(data["payment_method"], PaymentMethod) else data["payment_method"]
        data["payment_status"] = data["payment_status"].value if isinstance(data["payment_status"], PaymentStatus) else data["payment_status"]
        return data


class DirectDebitMandateCreate(BaseModel):
    """Model for creating a direct debit mandate."""

    direct_debit_id: UUID
    policy_id: UUID
    bank_account_id: UUID

    debit_day: int = Field(..., ge=1, le=28)
    frequency: str = Field(default="Monthly", max_length=20)
    max_debit_amount: Optional[Decimal] = None

    mandate_reference: str = Field(..., max_length=50)
    authorization_date: date
    authorization_method: str = Field(..., max_length=30)

    status: str = Field(default="Active", max_length=20)
    cancellation_date: Optional[date] = None
    cancellation_reason: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class DirectDebitResultCreate(BaseModel):
    """Model for creating a direct debit result."""

    result_id: UUID
    direct_debit_id: UUID
    invoice_id: Optional[UUID] = None

    attempt_date: date
    attempt_number: int = Field(default=1, ge=1)

    requested_amount: Decimal = Field(..., ge=0)
    result_status: str = Field(..., max_length=20)  # Success/Dishonoured/InsufficientFunds
    result_code: Optional[str] = Field(None, max_length=10)
    result_description: Optional[str] = Field(None, max_length=200)

    settlement_date: Optional[date] = None
    payment_id: Optional[UUID] = None

    retry_scheduled: bool = False
    retry_date: Optional[date] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class BankAccountCreate(BaseModel):
    """Model for creating a bank account record."""

    bank_account_id: UUID
    member_id: UUID
    policy_id: Optional[UUID] = None

    account_name: str = Field(..., max_length=100)
    bsb: str = Field(..., max_length=7)
    account_number_masked: str = Field(..., max_length=20)
    bank_name: Optional[str] = Field(None, max_length=100)
    account_type: str = Field(..., max_length=20)  # Savings/Cheque
    purpose: str = Field(..., max_length=30)  # PremiumDebit/ClaimRefund/Both

    is_active: bool = True
    is_verified: bool = False
    verification_date: Optional[date] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class ArrearsCreate(BaseModel):
    """Model for creating an arrears record."""

    arrears_id: UUID
    policy_id: UUID
    invoice_id: UUID

    arrears_date: date
    arrears_amount: Decimal = Field(..., ge=0)
    days_overdue: int = Field(..., ge=1)

    arrears_status: str = Field(..., max_length=20)  # Current/Resolved/WrittenOff
    resolution_date: Optional[date] = None
    resolution_method: Optional[str] = None

    reminder_sent: bool = False
    reminder_date: Optional[date] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class RefundCreate(BaseModel):
    """Model for creating a refund record."""

    refund_id: UUID
    refund_reference: str = Field(..., max_length=30)
    policy_id: UUID
    member_id: Optional[UUID] = None

    refund_date: date
    refund_amount: Decimal = Field(..., ge=0)
    refund_reason: str = Field(..., max_length=200)
    refund_type: str = Field(..., max_length=30)  # Cancellation/Overpayment/Adjustment

    payment_method: str = Field(..., max_length=30)
    bank_account_id: Optional[UUID] = None

    status: str = Field(default="Pending", max_length=20)
    processed_date: Optional[date] = None
    bank_reference: Optional[str] = Field(None, max_length=50)

    approved_by: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class PremiumDiscountCreate(BaseModel):
    """Model for creating a premium discount record."""

    premium_discount_id: UUID
    policy_id: UUID

    discount_type: str = Field(..., max_length=30)  # AgeBased/Corporate/MultiPolicy/Loyalty
    discount_percentage: Decimal = Field(..., ge=0, le=100)
    discount_amount: Optional[Decimal] = None

    effective_date: date
    end_date: Optional[date] = None

    reason: Optional[str] = Field(None, max_length=200)
    corporate_account_id: Optional[int] = None

    is_active: bool = True

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
