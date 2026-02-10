"""
Billing process for Brickwell Health Simulator.

Handles invoicing, direct debit processing, and arrears management.
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import Generator, Any
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.enums import InvoiceStatus, PaymentMethod, PaymentStatus
from brickwell_health.generators.billing_generator import BillingGenerator


logger = structlog.get_logger()


def _get_primary_member_id(policy_data: dict) -> UUID | None:
    """Get the primary member ID from policy data."""
    members = policy_data.get("members", [])
    if members:
        for member in members:
            if hasattr(member, "member_id"):
                return member.member_id
    return None


class BillingProcess(BaseProcess):
    """
    SimPy process for billing operations.

    Handles:
    - Monthly invoice generation
    - Direct debit processing with retry logic
    - Payment recording
    - Arrears tracking

    Billing cycle (per policy):
    1. Invoice generated on policy anniversary day (same day of month as effective_date)
    2. Due date is 15 days after invoice issue
    3. Direct debit attempted on due date
    4. If failed, retry up to max_debit_retries times, retry_interval_days apart
    5. After all retries exhausted, invoice goes to arrears (days_to_arrears after due date)
    6. 60 days past due: Policy suspension consideration
    """

    def __init__(
        self,
        *args: Any,
        active_policies: dict[UUID, dict] | None = None,
        pending_invoices: dict[UUID, dict] | None = None,
        shared_state: Any = None,
        **kwargs: Any,
    ):
        """
        Initialize the billing process.

        Args:
            active_policies: Dictionary of active policies
            pending_invoices: Dictionary of unpaid invoices
            shared_state: SharedState instance for cross-process cleanup
        """
        super().__init__(*args, **kwargs)

        # Track policies and invoices
        # Use explicit None check to preserve empty dict references for shared state
        self.active_policies = active_policies if active_policies is not None else {}
        self.pending_invoices = pending_invoices if pending_invoices is not None else {}
        self.shared_state = shared_state

        # Initialize generator
        self.billing_gen = BillingGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)

        # Configuration
        self.max_debit_retries = self.config.billing.max_debit_retries
        self.retry_interval_days = self.config.billing.retry_interval_days
        self.days_to_arrears = self.config.billing.days_to_arrears
        self.days_to_suspension = self.config.billing.days_to_suspension
        self.days_to_lapse = self.config.billing.days_to_lapse

        # Calculate per-attempt success rate from final success rate
        # Formula: per_attempt_failure^(total_attempts) = final_failure
        # So: per_attempt_success = 1 - (1 - final_success)^(1/total_attempts)
        total_attempts = 1 + self.max_debit_retries
        final_success_rate = self.config.billing.final_payment_success_rate
        final_failure_rate = 1 - final_success_rate
        per_attempt_failure = final_failure_rate ** (1 / total_attempts)
        self.payment_success_rate = 1 - per_attempt_failure

    def _emit_crm_event(
        self,
        event_type: str,
        policy_id: UUID,
        member_id: UUID | None,
        **kwargs,
    ) -> None:
        """
        Emit a CRM trigger event for processing by CRMProcess.

        Args:
            event_type: Type of event (payment_failed, arrears_created, policy_suspended)
            policy_id: The policy UUID
            member_id: Primary member UUID
            **kwargs: Additional event data
        """
        if not self.shared_state:
            return

        self.shared_state.add_crm_event({
            "event_type": event_type,
            "policy_id": policy_id,
            "member_id": member_id,
            "timestamp": self.sim_env.current_datetime,
            **kwargs,
        })

    def run(self) -> Generator:
        """
        Main billing process loop.

        Runs daily, handling billing events.
        """
        logger.info(
            "billing_process_started",
            worker_id=self.worker_id,
            final_success_rate=f"{self.config.billing.final_payment_success_rate:.0%}",
            per_attempt_success_rate=f"{self.payment_success_rate:.1%}",
            max_retries=self.max_debit_retries,
            retry_interval_days=self.retry_interval_days,
            days_to_arrears=self.days_to_arrears,
            days_to_suspension=self.days_to_suspension,
            days_to_lapse=self.days_to_lapse,
        )

        while True:
            current_date = self.sim_env.current_date

            # Generate invoices for policies whose billing day matches today
            # Each policy is billed on the anniversary of their effective_date
            yield from self._generate_monthly_invoices(current_date)

            # Process direct debits for invoices that are due
            yield from self._process_direct_debits(current_date)

            # Check for arrears daily
            self._check_arrears(current_date)

            # Process member change events (address changes affect billing address)
            self._process_member_change_events(current_date)

            # Wait until next day
            yield self.env.timeout(1.0)

            # Log progress on 1st of month
            if current_date.day == 1:
                self._log_progress()

    def _generate_monthly_invoices(self, current_date: date) -> Generator:
        """
        Generate invoices for policies whose billing day matches today.

        Each policy is billed on the anniversary of their effective_date
        (same day of month as when the policy started).

        Args:
            current_date: Current simulation date
        """
        invoices_generated = 0

        for policy_id, policy_data in list(self.active_policies.items()):
            # Skip suspended policies
            if policy_data.get("status") != "Active":
                continue

            policy = policy_data.get("policy")
            if policy is None:
                continue

            # Get policy's billing day (day of month from effective_date)
            policy_billing_day = policy.effective_date.day

            # Handle months with fewer days (e.g., policy started on 31st)
            # In shorter months, bill on the last day of the month
            import calendar
            days_in_month = calendar.monthrange(current_date.year, current_date.month)[1]
            effective_billing_day = min(policy_billing_day, days_in_month)

            # Only generate invoice if today is the policy's billing day
            if current_date.day != effective_billing_day:
                continue

            # Generate invoice with period starting today
            invoice = self.billing_gen.generate_invoice(
                policy=policy,
                period_start=current_date,
                lhc_loading_pct=Decimal(str(policy_data.get("lhc_loading", 0))),
                age_discount_pct=Decimal(str(policy_data.get("age_discount", 0))),
                rebate_pct=Decimal(str(policy_data.get("rebate_pct", 0))),
            )

            self.batch_writer.add("billing.invoice", invoice.model_dump_db())

            # Track pending invoice with retry tracking
            self.pending_invoices[invoice.invoice_id] = {
                "invoice": invoice,
                "policy_id": policy_id,
                "due_date": invoice.due_date,
                "next_attempt_date": invoice.due_date,  # First attempt on due date
                "attempts": 0,  # Will be incremented on each attempt
                "arrears_created": False,
            }

            invoices_generated += 1
            self.increment_stat("invoices_generated")

        if invoices_generated > 0:
            logger.debug(
                "invoices_generated_today",
                date=current_date.isoformat(),
                count=invoices_generated,
            )

        yield self.env.timeout(0)

    def _process_direct_debits(self, current_date: date) -> Generator:
        """
        Process direct debit attempts for invoices scheduled for today.

        Direct debits are attempted on:
        - Initial due date (15 days after invoice issue)
        - Retry dates (retry_interval_days after each failed attempt)

        After max_debit_retries failed retries, invoice remains unpaid and goes to arrears.

        Args:
            current_date: Current simulation date
        """
        debits_processed = 0
        total_attempts = 1 + self.max_debit_retries  # Initial + retries

        for invoice_id, invoice_data in list(self.pending_invoices.items()):
            invoice = invoice_data["invoice"]
            next_attempt_date = invoice_data["next_attempt_date"]
            policy_id = invoice_data["policy_id"]

            # Skip if no more attempts scheduled (None means retries exhausted)
            if next_attempt_date is None:
                continue

            # Process if today is on or after the scheduled attempt date
            # This handles resume scenarios where next_attempt_date may be in the past
            if current_date < next_attempt_date:
                continue

            policy_data = self.active_policies.get(policy_id)
            if policy_data is None:
                continue

            # Get mandate
            mandate = policy_data.get("mandate")
            if mandate is None:
                continue

            # Increment attempt counter
            invoice_data["attempts"] += 1
            attempt_number = invoice_data["attempts"]
            debits_processed += 1

            # Determine if more retries are available after this attempt
            retries_remaining = total_attempts - attempt_number

            # Create payment record for this attempt (PENDING status)
            # All direct debit attempts create a payment record for audit trail
            payment = self.billing_gen.generate_payment(
                policy=policy_data.get("policy"),
                invoice=invoice,
                payment_date=current_date,
                payment_method=PaymentMethod.DIRECT_DEBIT,
            )
            self.batch_writer.add("billing.payment", payment.model_dump_db())

            # Flush to commit PENDING state before updating (for CDC)
            self.batch_writer.flush_for_cdc("payment", "payment_id", payment.payment_id)

            # Determine direct debit outcome
            success = self.rng.random() < self.payment_success_rate

            if success:
                # Update payment status from PENDING to COMPLETED
                self._update_payment_status(payment, PaymentStatus.COMPLETED)

                # Record successful debit result
                result = self.billing_gen.generate_direct_debit_result(
                    mandate=mandate,
                    invoice=invoice,
                    attempt_date=current_date,
                    attempt_number=attempt_number,
                    success=True,
                    payment=payment,
                    retry_scheduled=False,
                    retry_date=None,
                )
                self.batch_writer.add("billing.direct_debit_result", result.model_dump())

                # Update invoice status in memory and in buffer/DB
                self.billing_gen.mark_invoice_paid(invoice, payment)
                self._update_invoice_status(invoice)

                # Persist retry state before removing (for resume support)
                self._update_invoice_retry_state(invoice_id, invoice_data)

                # Remove from pending
                del self.pending_invoices[invoice_id]

                # Check if suspended policy should be reinstated
                if policy_data.get("status") == "Suspended" and policy_data.get("suspension_reason") == "Arrears":
                    # Check if any pending invoices remain for this policy
                    has_remaining = any(
                        inv_data["policy_id"] == policy_id
                        for inv_data in self.pending_invoices.values()
                    )
                    if not has_remaining:
                        self._reinstate_policy_from_arrears(policy_id, policy_data, current_date)

                self.increment_stat("payments_successful")

                logger.debug(
                    "direct_debit_success",
                    invoice_id=str(invoice_id),
                    attempt=attempt_number,
                )

            else:
                # Update payment status from PENDING to FAILED
                self._update_payment_status(payment, PaymentStatus.FAILED)

                # Failed debit - determine retry scheduling
                if retries_remaining > 0:
                    # Schedule retry
                    next_retry = current_date + timedelta(days=self.retry_interval_days)
                    invoice_data["next_attempt_date"] = next_retry
                    retry_scheduled = True

                    # Persist retry state for resume support
                    self._update_invoice_retry_state(invoice_id, invoice_data)

                    logger.debug(
                        "direct_debit_failed_retry_scheduled",
                        invoice_id=str(invoice_id),
                        attempt=attempt_number,
                        retries_remaining=retries_remaining,
                        next_retry=next_retry.isoformat(),
                    )
                else:
                    # No more retries - invoice will go to arrears
                    invoice_data["next_attempt_date"] = None  # No more attempts
                    retry_scheduled = False
                    next_retry = None

                    # Persist retry state for resume support
                    self._update_invoice_retry_state(invoice_id, invoice_data)

                    logger.debug(
                        "direct_debit_failed_no_retries",
                        invoice_id=str(invoice_id),
                        attempt=attempt_number,
                        total_attempts=total_attempts,
                    )

                # Record failed debit result (now with payment reference)
                result = self.billing_gen.generate_direct_debit_result(
                    mandate=mandate,
                    invoice=invoice,
                    attempt_date=current_date,
                    attempt_number=attempt_number,
                    success=False,
                    payment=payment,  # Now includes payment reference
                    retry_scheduled=retry_scheduled,
                    retry_date=next_retry,
                )
                self.batch_writer.add("billing.direct_debit_result", result.model_dump())

                self.increment_stat("payments_failed")

                # Emit CRM event for payment failure
                primary_member_id = _get_primary_member_id(policy_data)
                self._emit_crm_event(
                    "payment_failed",
                    policy_id,
                    primary_member_id,
                    invoice_id=invoice_id,
                    attempt_number=attempt_number,
                )

        if debits_processed > 0:
            logger.debug(
                "direct_debits_processed",
                date=current_date.isoformat(),
                count=debits_processed,
            )

        yield self.env.timeout(0)

    def _check_arrears(self, current_date: date) -> None:
        """
        Check pending invoices for arrears, suspension, and lapse.

        Timeline:
        - days_to_arrears (14): Create arrears record
        - days_to_suspension (30): Suspend policy (claims blocked, can reinstate)
        - days_to_lapse (60): Lapse policy (terminated, new policy required)

        Args:
            current_date: Current simulation date
        """
        for invoice_id, invoice_data in list(self.pending_invoices.items()):
            invoice = invoice_data["invoice"]
            due_date = invoice_data["due_date"]
            policy_id = invoice_data["policy_id"]

            days_overdue = (current_date - due_date).days

            if days_overdue < self.days_to_arrears:
                continue

            policy_data = self.active_policies.get(policy_id)
            if policy_data is None:
                continue

            # Check for lapse first (60+ days overdue)
            if days_overdue >= self.days_to_lapse:
                self._lapse_policy_for_arrears(
                    policy_id, policy_data, invoice_id, invoice_data, current_date, days_overdue
                )
                continue

            # Check for suspension (30+ days overdue)
            if days_overdue >= self.days_to_suspension:
                self._suspend_policy_for_arrears(
                    policy_id, policy_data, invoice_data, current_date, days_overdue
                )

            # Create arrears record if not already done (14+ days overdue)
            if not invoice_data.get("arrears_created"):
                arrears = self.billing_gen.generate_arrears(
                    policy=policy_data.get("policy"),
                    invoice=invoice,
                    arrears_date=current_date,
                    days_overdue=days_overdue,
                )

                self.batch_writer.add("billing.arrears", arrears.model_dump())
                invoice_data["arrears_created"] = True

                self.increment_stat("arrears_created")

                # Persist retry state for resume support
                self._update_invoice_retry_state(invoice_id, invoice_data)

                # Update invoice status
                invoice.invoice_status = InvoiceStatus.OVERDUE

                logger.debug(
                    "arrears_created",
                    invoice_id=str(invoice_id),
                    days_overdue=days_overdue,
                )

                # Emit CRM event for arrears creation
                primary_member_id = _get_primary_member_id(policy_data)
                self._emit_crm_event(
                    "arrears_created",
                    policy_id,
                    primary_member_id,
                    invoice_id=invoice_id,
                    arrears_amount=float(arrears.arrears_amount) if arrears.arrears_amount else 0,
                )

    def _suspend_policy_for_arrears(
        self,
        policy_id: UUID,
        policy_data: dict,
        invoice_data: dict,
        current_date: date,
        days_overdue: int,
    ) -> None:
        """
        Suspend a policy due to arrears.

        Suspended policies:
        - Cannot make claims
        - Can be reinstated by paying arrears
        - Still tracked in active_policies (to check for lapse)

        Args:
            policy_id: Policy UUID
            policy_data: Policy data dictionary
            invoice_data: Invoice data dictionary
            current_date: Current simulation date
            days_overdue: Days the invoice is overdue
        """
        # Skip if already suspended or lapsed
        current_status = policy_data.get("status", "Active")
        if current_status in ("Suspended", "Lapsed"):
            return

        # Mark as suspended in memory
        policy_data["status"] = "Suspended"
        policy_data["suspension_date"] = current_date
        policy_data["suspension_reason"] = "Arrears"

        # Update policy status in database
        sql = f"""
            UPDATE policy
            SET policy_status = 'Suspended',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
        """
        self.batch_writer.add_raw_sql("policy_suspension_arrears", sql)

        self.increment_stat("policies_suspended_arrears")

        logger.info(
            "policy_suspended_for_arrears",
            policy_id=str(policy_id),
            days_overdue=days_overdue,
            date=current_date.isoformat(),
        )

        # Emit CRM event for policy suspension
        primary_member_id = _get_primary_member_id(policy_data)
        self._emit_crm_event(
            "policy_suspended",
            policy_id,
            primary_member_id,
            suspension_reason="arrears",
        )

    def _reinstate_policy_from_arrears(
        self,
        policy_id: UUID,
        policy_data: dict,
        current_date: date,
    ) -> None:
        """
        Reinstate a suspended policy after arrears are paid.

        Called when a policy that was suspended for non-payment has
        successfully paid all pending invoices.

        Args:
            policy_id: Policy UUID
            policy_data: Policy data dictionary
            current_date: Current simulation date
        """
        # Update in memory
        policy_data["status"] = "Active"
        policy_data.pop("suspension_date", None)
        policy_data.pop("suspension_reason", None)

        # Update in database
        sql = f"""
            UPDATE policy
            SET policy_status = 'Active',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
        """
        self.batch_writer.add_raw_sql("policy_reinstatement", sql)

        self.increment_stat("policies_reinstated")
        logger.info(
            "policy_reinstated_from_arrears",
            policy_id=str(policy_id),
            date=current_date.isoformat(),
        )

    def _lapse_policy_for_arrears(
        self,
        policy_id: UUID,
        policy_data: dict,
        invoice_id: UUID,
        invoice_data: dict,
        current_date: date,
        days_overdue: int,
    ) -> None:
        """
        Lapse a policy due to prolonged arrears (~2 months overdue).

        Lapsed policies:
        - Cannot make claims
        - Cannot be reinstated (new policy required)
        - Removed from active tracking
        - All coverages and members terminated

        Args:
            policy_id: Policy UUID
            policy_data: Policy data dictionary
            invoice_id: Invoice UUID
            invoice_data: Invoice data dictionary
            current_date: Current simulation date
            days_overdue: Days the invoice is overdue
        """
        # Skip if already lapsed
        if policy_data.get("status") == "Lapsed":
            return

        # Remove from active policies
        del self.active_policies[policy_id]

        # Remove pending invoice
        del self.pending_invoices[invoice_id]

        # Clean up policy members from shared state (so claims process stops tracking them)
        if self.shared_state:
            self.shared_state.remove_policy_members(policy_id)

        # Update policy status to Lapsed
        sql = f"""
            UPDATE policy
            SET policy_status = 'Lapsed',
                end_date = '{current_date.isoformat()}',
                cancellation_reason = 'Lapsed due to non-payment ({days_overdue} days overdue)',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
        """
        self.batch_writer.add_raw_sql("policy_lapse", sql)

        # End all coverage records
        sql = f"""
            UPDATE coverage
            SET status = 'Terminated',
                end_date = '{current_date.isoformat()}',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
              AND (status = 'Active' OR status = 'Suspended' OR status IS NULL)
        """
        self.batch_writer.add_raw_sql("coverage_lapse", sql)

        # End all policy member records
        sql = f"""
            UPDATE policy_member
            SET is_active = FALSE,
                end_date = '{current_date.isoformat()}'
            WHERE policy_id = '{policy_id}'
              AND is_active = TRUE
        """
        self.batch_writer.add_raw_sql("policy_member_lapse", sql)

        self.increment_stat("policies_lapsed")

        logger.info(
            "policy_lapsed_for_arrears",
            policy_id=str(policy_id),
            days_overdue=days_overdue,
            date=current_date.isoformat(),
        )

    def add_policy(
        self,
        policy_id: UUID,
        policy_data: dict,
    ) -> None:
        """
        Add a policy to billing tracking.

        Args:
            policy_id: Policy UUID
            policy_data: Policy data dictionary
        """
        self.active_policies[policy_id] = policy_data

    def remove_policy(self, policy_id: UUID) -> None:
        """
        Remove a policy from billing tracking.

        Args:
            policy_id: Policy UUID
        """
        self.active_policies.pop(policy_id, None)

        # Remove any pending invoices for this policy
        for inv_id, inv_data in list(self.pending_invoices.items()):
            if inv_data.get("policy_id") == policy_id:
                del self.pending_invoices[inv_id]

    def _log_progress(self) -> None:
        """Log billing progress."""
        stats = self.get_stats()
        logger.info(
            "billing_progress",
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            active_policies=len(self.active_policies),
            pending_invoices=len(self.pending_invoices),
            invoices_generated=stats.get("invoices_generated", 0),
            payments_successful=stats.get("payments_successful", 0),
            payments_failed=stats.get("payments_failed", 0),
            arrears_created=stats.get("arrears_created", 0),
            policies_suspended_arrears=stats.get("policies_suspended_arrears", 0),
            policies_reinstated=stats.get("policies_reinstated", 0),
            policies_lapsed=stats.get("policies_lapsed", 0),
        )

    def _update_invoice_status(self, invoice) -> None:
        """
        Update invoice status in BatchWriter buffer or database.

        Flushes buffer first if invoice is still pending to ensure INSERT
        is committed before UPDATE (for CDC visibility).

        Args:
            invoice: Invoice with updated status
        """
        # Flush if invoice is still in buffer to ensure INSERT is committed for CDC
        self.batch_writer.flush_for_cdc("invoice", "invoice_id", invoice.invoice_id)

        status = invoice.invoice_status
        if hasattr(status, 'value'):
            status = status.value

        # Use BatchWriter's update_record to update in DB
        self.batch_writer.update_record(
            table_name="invoice",
            key_field="invoice_id",
            key_value=invoice.invoice_id,
            updates={
                "invoice_status": status,
                "paid_amount": float(invoice.paid_amount) if invoice.paid_amount else 0,
                "balance_due": float(invoice.balance_due) if invoice.balance_due else 0,
                "modified_at": self.sim_env.current_datetime.isoformat(),
                "modified_by": "SIMULATION",
            },
        )

    def _update_payment_status(self, payment, new_status: PaymentStatus) -> None:
        """
        Update payment status in BatchWriter buffer or database.

        Called after direct debit result is known to transition payment
        from PENDING to COMPLETED (success) or FAILED (all retries exhausted).

        Args:
            payment: Payment object with payment_id
            new_status: New PaymentStatus (COMPLETED or FAILED)
        """
        self.batch_writer.update_record(
            table_name="payment",
            key_field="payment_id",
            key_value=payment.payment_id,
            updates={
                "payment_status": new_status.value,
                "modified_at": self.sim_env.current_datetime.isoformat(),
                "modified_by": "SIMULATION",
            },
        )

    def _update_invoice_retry_state(self, invoice_id: UUID, invoice_data: dict) -> None:
        """
        Persist invoice retry state to database for resume support.

        Saves retry_attempts, next_retry_date, and arrears_created to the invoice
        table so that resume from database (without checkpoint) works correctly.

        Args:
            invoice_id: Invoice UUID
            invoice_data: Invoice data dictionary with retry state
        """
        next_attempt = invoice_data.get("next_attempt_date")
        self.batch_writer.update_record(
            table_name="invoice",
            key_field="invoice_id",
            key_value=invoice_id,
            updates={
                "retry_attempts": invoice_data.get("attempts", 0),
                "next_retry_date": next_attempt.isoformat() if next_attempt else None,
                "arrears_created": invoice_data.get("arrears_created", False),
                "modified_at": self.sim_env.current_datetime.isoformat(),
                "modified_by": "SIMULATION",
            },
        )

    def _process_member_change_events(self, current_date: date) -> None:
        """
        Process member change events that affect billing.

        Handles:
        - ADDRESS_CHANGE: Update billing address on bank account
        """
        if not self.shared_state:
            return

        # Process address changes
        # Note: PolicyLifecycleProcess also processes these, so check if any remain
        # for billing-specific handling (address changes for billing address updates)
        address_events = self.shared_state.get_member_change_events("ADDRESS_CHANGE")
        for event in address_events:
            self._handle_address_change_for_billing(event, current_date)

    def _handle_address_change_for_billing(self, event: dict, current_date: date) -> None:
        """
        Handle address change for billing purposes.

        Updates the billing address on the bank account/direct debit mandate
        when the primary member's address changes.

        Args:
            event: Member change event dictionary
            current_date: Current simulation date
        """
        policy_id = event["policy_id"]
        change_data = event.get("change_data", {})
        member_role = change_data.get("member_role")
        new_state = change_data.get("new_state")

        # Only update billing address for primary member changes
        if member_role != "Primary":
            return

        # Note: Full implementation would update the bank_account record
        # with the new billing address. For now, we just track the event.
        logger.debug(
            "billing_address_change_noted",
            policy_id=str(policy_id),
            new_state=new_state,
        )
        self.increment_stat("billing_address_changes")




