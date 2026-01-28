"""
Billing generator for Brickwell Health Simulator.

Generates invoices, payments, and direct debit records.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from brickwell_health.domain.billing import (
    InvoiceCreate,
    PaymentCreate,
    DirectDebitMandateCreate,
    DirectDebitResultCreate,
    BankAccountCreate,
    ArrearsCreate,
    RefundCreate,
)
from brickwell_health.domain.enums import (
    InvoiceStatus,
    PaymentMethod,
    PaymentStatus,
)
from brickwell_health.domain.member import MemberCreate
from brickwell_health.domain.policy import PolicyCreate
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.utils.time_conversion import first_of_month, add_months, last_of_month


class BillingGenerator(BaseGenerator[InvoiceCreate]):
    """
    Generates billing records (invoices, payments, direct debits).
    """

    def __init__(self, rng, reference, id_generator: IDGenerator):
        """
        Initialize the billing generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
        """
        super().__init__(rng, reference)
        self.id_generator = id_generator

    def generate(self, **kwargs: Any) -> InvoiceCreate:
        """
        Generate an invoice (delegates to generate_invoice).

        This is the abstract method implementation required by BaseGenerator.

        Args:
            **kwargs: Arguments passed to generate_invoice

        Returns:
            InvoiceCreate instance
        """
        return self.generate_invoice(**kwargs)

    def generate_invoice(
        self,
        policy: PolicyCreate,
        period_start: date,
        lhc_loading_pct: Decimal = Decimal("0"),
        age_discount_pct: Decimal = Decimal("0"),
        rebate_pct: Decimal = Decimal("0"),
        invoice_id: UUID | None = None,
        **kwargs: Any,
    ) -> InvoiceCreate:
        """
        Generate a monthly invoice.

        Args:
            policy: Policy to invoice
            period_start: Start of billing period
            lhc_loading_pct: LHC loading percentage
            age_discount_pct: Age-based discount percentage
            rebate_pct: Government rebate percentage
            invoice_id: Optional pre-generated UUID

        Returns:
            InvoiceCreate instance
        """
        if invoice_id is None:
            invoice_id = self.id_generator.generate_uuid()

        # Invoice date is the billing anniversary day (policy start day of month)
        invoice_date = period_start
        # Period covers from invoice date to same day next month (or end of month if shorter)
        period_end = add_months(period_start, 1)
        # Adjust period_end to be day before next billing date
        from datetime import timedelta
        period_end = period_end - timedelta(days=1)
        # Due date is 15 days after invoice issue
        due_date = period_start + timedelta(days=15)

        # Calculate amounts
        gross_premium = policy.premium_amount

        # LHC loading (increases premium)
        lhc_loading = (gross_premium * lhc_loading_pct / Decimal("100")).quantize(
            Decimal("0.01")
        )

        # Age-based discount (decreases premium)
        age_discount = (gross_premium * age_discount_pct / Decimal("100")).quantize(
            Decimal("0.01")
        )

        # Government rebate (decreases premium)
        rebate_amount = (gross_premium * rebate_pct).quantize(Decimal("0.01"))

        # Net amount
        net_amount = gross_premium + lhc_loading - age_discount - rebate_amount
        total_amount = net_amount  # No GST on health insurance

        return InvoiceCreate(
            invoice_id=invoice_id,
            invoice_number=self.id_generator.generate_invoice_number(),
            policy_id=policy.policy_id,
            invoice_date=invoice_date,
            due_date=due_date,
            period_start=period_start,
            period_end=period_end,
            invoice_status=InvoiceStatus.ISSUED,
            gross_premium=gross_premium,
            lhc_loading_amount=lhc_loading,
            age_discount_amount=age_discount,
            rebate_amount=rebate_amount,
            other_adjustments=Decimal("0"),
            net_amount=net_amount,
            gst_amount=Decimal("0"),
            total_amount=total_amount,
            paid_amount=Decimal("0"),
            balance_due=total_amount,
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

    def generate_payment(
        self,
        policy: PolicyCreate,
        invoice: InvoiceCreate,
        payment_date: date,
        payment_method: PaymentMethod = PaymentMethod.DIRECT_DEBIT,
        payment_id: UUID | None = None,
        **kwargs: Any,
    ) -> PaymentCreate:
        """
        Generate a payment for an invoice.

        Args:
            policy: Policy
            invoice: Invoice being paid
            payment_date: Date of payment
            payment_method: How payment was made
            payment_id: Optional pre-generated UUID

        Returns:
            PaymentCreate instance
        """
        if payment_id is None:
            payment_id = self.id_generator.generate_uuid()

        return PaymentCreate(
            payment_id=payment_id,
            payment_number=self.id_generator.generate_payment_number(),
            policy_id=policy.policy_id,
            invoice_id=invoice.invoice_id,
            payment_date=payment_date,
            payment_amount=invoice.total_amount,
            payment_method=payment_method,
            payment_status=PaymentStatus.COMPLETED,
            bank_reference=f"DD{self.uniform_int(100000000, 999999999)}",
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

    def generate_bank_account(
        self,
        member: MemberCreate,
        policy: PolicyCreate | None = None,
        bank_account_id: UUID | None = None,
        **kwargs: Any,
    ) -> BankAccountCreate:
        """
        Generate a bank account record.

        Args:
            member: Account owner
            policy: Optional associated policy
            bank_account_id: Optional pre-generated UUID

        Returns:
            BankAccountCreate instance
        """
        if bank_account_id is None:
            bank_account_id = self.id_generator.generate_uuid()

        # Generate BSB and masked account
        bsb = self.id_generator.generate_bsb()
        account_masked = self.id_generator.generate_masked_account_number()

        bank_names = ["Commonwealth Bank", "Westpac", "ANZ", "NAB", "St George", "ING"]

        return BankAccountCreate(
            bank_account_id=bank_account_id,
            member_id=member.member_id,
            policy_id=policy.policy_id if policy else None,
            account_name=f"{member.first_name} {member.last_name}",
            bsb=bsb,
            account_number_masked=account_masked,
            bank_name=self.choice(bank_names),
            account_type=self.choice(["Savings", "Cheque"], [0.8, 0.2]),
            purpose="PremiumDebit",
            is_active=True,
            is_verified=True,
            verification_date=date.today(),
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

    def generate_direct_debit_mandate(
        self,
        policy: PolicyCreate,
        bank_account: BankAccountCreate,
        authorization_date: date,
        direct_debit_id: UUID | None = None,
        **kwargs: Any,
    ) -> DirectDebitMandateCreate:
        """
        Generate a direct debit mandate.

        Args:
            policy: Policy for debits
            bank_account: Bank account to debit
            authorization_date: Date mandate was authorized
            direct_debit_id: Optional pre-generated UUID

        Returns:
            DirectDebitMandateCreate instance
        """
        if direct_debit_id is None:
            direct_debit_id = self.id_generator.generate_uuid()

        # Debit day - typically 1st, 15th, or end of month
        debit_day = self.choice([1, 15, 28], [0.4, 0.4, 0.2])

        return DirectDebitMandateCreate(
            direct_debit_id=direct_debit_id,
            policy_id=policy.policy_id,
            bank_account_id=bank_account.bank_account_id,
            debit_day=debit_day,
            frequency="Monthly",
            max_debit_amount=policy.premium_amount * Decimal("1.5"),
            mandate_reference=self.id_generator.generate_mandate_reference(),
            authorization_date=authorization_date,
            authorization_method=self.choice(["Online", "Phone", "PaperForm"], [0.7, 0.2, 0.1]),
            status="Active",
            cancellation_date=None,
            cancellation_reason=None,
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

    def generate_direct_debit_result(
        self,
        mandate: DirectDebitMandateCreate,
        invoice: InvoiceCreate,
        attempt_date: date,
        attempt_number: int = 1,
        success: bool = True,
        payment: PaymentCreate | None = None,
        retry_scheduled: bool = False,
        retry_date: date | None = None,
        result_id: UUID | None = None,
        **kwargs: Any,
    ) -> DirectDebitResultCreate:
        """
        Generate a direct debit result.

        Args:
            mandate: Direct debit mandate
            invoice: Invoice being debited
            attempt_date: Date of attempt
            attempt_number: Which attempt this is (1 = initial, 2+ = retries)
            success: Whether debit succeeded
            payment: Payment created if successful
            retry_scheduled: Whether another retry is scheduled
            retry_date: Date of next retry (if scheduled)
            result_id: Optional pre-generated UUID

        Returns:
            DirectDebitResultCreate instance
        """
        if result_id is None:
            result_id = self.id_generator.generate_uuid()

        if success:
            status = "Success"
            result_code = "00"
            description = "Transaction successful"
            settlement_date = attempt_date
        else:
            fail_reasons = [
                ("01", "Insufficient funds"),
                ("02", "Account closed"),
                ("03", "Invalid account"),
                ("04", "Dishonoured"),
            ]
            result_code, description = self.choice(fail_reasons)
            status = description.replace(" ", "")
            settlement_date = None

        return DirectDebitResultCreate(
            result_id=result_id,
            direct_debit_id=mandate.direct_debit_id,
            invoice_id=invoice.invoice_id,
            attempt_date=attempt_date,
            attempt_number=attempt_number,
            requested_amount=invoice.total_amount,
            result_status=status,
            result_code=result_code,
            result_description=description,
            settlement_date=settlement_date,
            payment_id=payment.payment_id if payment else None,
            retry_scheduled=retry_scheduled,
            retry_date=retry_date,
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

    def generate_arrears(
        self,
        policy: PolicyCreate,
        invoice: InvoiceCreate,
        arrears_date: date,
        days_overdue: int,
        arrears_id: UUID | None = None,
        **kwargs: Any,
    ) -> ArrearsCreate:
        """
        Generate an arrears record.

        Args:
            policy: Policy in arrears
            invoice: Overdue invoice
            arrears_date: Date arrears recorded
            days_overdue: Days past due
            arrears_id: Optional pre-generated UUID

        Returns:
            ArrearsCreate instance
        """
        if arrears_id is None:
            arrears_id = self.id_generator.generate_uuid()

        return ArrearsCreate(
            arrears_id=arrears_id,
            policy_id=policy.policy_id,
            invoice_id=invoice.invoice_id,
            arrears_date=arrears_date,
            arrears_amount=invoice.balance_due or invoice.total_amount,
            days_overdue=days_overdue,
            arrears_status="Current",
            resolution_date=None,
            resolution_method=None,
            reminder_sent=False,
            reminder_date=None,
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

    def generate_refund(
        self,
        policy: PolicyCreate,
        refund_date: date,
        refund_amount: Decimal,
        refund_reason: str,
        refund_type: str,  # "Cancellation" | "Suspension" | "Overpayment"
        member: MemberCreate | None = None,
        bank_account_id: UUID | None = None,
        refund_id: UUID | None = None,
        **kwargs: Any,
    ) -> RefundCreate:
        """
        Generate a refund record for prorated premium return.

        Args:
            policy: Policy being refunded
            refund_date: Date of refund
            refund_amount: Amount to refund
            refund_reason: Reason for refund (e.g., "Prorated refund for policy cancellation")
            refund_type: Type of refund (Cancellation/Suspension/Overpayment)
            member: Optional member to refund to
            bank_account_id: Optional bank account for EFT refund
            refund_id: Optional pre-generated UUID

        Returns:
            RefundCreate instance
        """
        if refund_id is None:
            refund_id = self.id_generator.generate_uuid()

        return RefundCreate(
            refund_id=refund_id,
            refund_reference=self.id_generator.generate_refund_reference(),
            policy_id=policy.policy_id,
            member_id=member.member_id if member else None,
            refund_date=refund_date,
            refund_amount=refund_amount.quantize(Decimal("0.01")),
            refund_reason=refund_reason,
            refund_type=refund_type,
            payment_method="EFT",
            bank_account_id=bank_account_id,
            status="Processed",
            processed_date=refund_date,
            bank_reference=f"REF{self.uniform_int(100000000, 999999999)}",
            approved_by="SIMULATION",
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

    def mark_invoice_paid(
        self,
        invoice: InvoiceCreate,
        payment: PaymentCreate,
    ) -> InvoiceCreate:
        """
        Update invoice as paid.

        Args:
            invoice: Invoice to update
            payment: Payment received

        Returns:
            Updated InvoiceCreate
        """
        invoice.paid_amount = payment.payment_amount
        invoice.balance_due = invoice.total_amount - payment.payment_amount
        if invoice.balance_due <= Decimal("0"):
            invoice.invoice_status = InvoiceStatus.PAID
        else:
            invoice.invoice_status = InvoiceStatus.PARTIALLY_PAID
        return invoice
