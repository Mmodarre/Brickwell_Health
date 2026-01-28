"""
Suspension process for Brickwell Health Simulator.

Handles policy suspensions, reactivations, and extensions.
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import Generator, Any
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.enums import SuspensionType
from brickwell_health.domain.policy import SuspensionCreate
from brickwell_health.generators.billing_generator import BillingGenerator
from brickwell_health.utils.time_conversion import last_of_month


logger = structlog.get_logger()


class SuspensionProcess(BaseProcess):
    """
    SimPy process for policy suspension management.

    Handles:
    - Creating new suspensions (overseas travel, financial hardship, other)
    - Reactivating policies when suspensions end
    - Extending suspensions when requested
    - Enforcing maximum suspension duration (2 years / 730 days)

    Australian PHI Suspension Rules:
    - Members can suspend their policy for up to 2 years (730 days cumulative)
    - Overseas travel: Typically 1 month to 1 year
    - Financial hardship: Typically 2 weeks to 3 months
    - Waiting periods may be paused during overseas travel suspensions
    - Upon reactivation, any remaining waiting periods resume
    """

    # Maximum cumulative suspension days allowed
    MAX_SUSPENSION_DAYS = 730  # 2 years

    # Suspension type weights for random selection
    SUSPENSION_TYPE_WEIGHTS = {
        SuspensionType.OVERSEAS_TRAVEL: 0.6,
        SuspensionType.FINANCIAL_HARDSHIP: 0.3,
        SuspensionType.OTHER: 0.1,
    }

    # Duration ranges by suspension type (days)
    SUSPENSION_DURATIONS = {
        SuspensionType.OVERSEAS_TRAVEL: (30, 365),  # 1 month to 1 year
        SuspensionType.FINANCIAL_HARDSHIP: (14, 90),  # 2 weeks to 3 months
        SuspensionType.OTHER: (14, 60),  # 2 weeks to 2 months
    }

    def __init__(
        self,
        *args: Any,
        active_policies: dict[UUID, dict] | None = None,
        active_suspensions: dict[UUID, dict] | None = None,
        **kwargs: Any,
    ):
        """
        Initialize the suspension process.

        Args:
            active_policies: Dictionary of active policies (policy_id -> policy data)
            active_suspensions: Dictionary of active suspensions (suspension_id -> suspension data)
        """
        super().__init__(*args, **kwargs)

        # Track active policies (shared with PolicyLifecycleProcess)
        self.active_policies = active_policies if active_policies is not None else {}

        # Track active suspensions for reactivation checking
        self.active_suspensions = active_suspensions if active_suspensions is not None else {}

        # Initialize billing generator for refunds
        self.billing_gen = BillingGenerator(self.rng, self.reference, self.id_generator)

    def run(self) -> Generator:
        """
        Main suspension process loop.

        Checks for suspensions that need to be reactivated daily.
        Note: Suspension creation is triggered by PolicyLifecycleProcess
        and delegated to this class via create_suspension().
        """
        logger.info(
            "suspension_process_started",
            worker_id=self.worker_id,
            max_suspension_days=self.MAX_SUSPENSION_DAYS,
        )

        while True:
            # Skip processing during warmup's first week
            if self.sim_env.now < 7:
                yield self.env.timeout(1.0)
                continue

            current_date = self.sim_env.current_date

            # Check for suspensions that should end
            self._check_suspension_reactivations(current_date)

            # Wait until next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def create_suspension(
        self,
        policy_id: UUID,
        policy: dict,
        current_date: date,
        suspension_type: SuspensionType | None = None,
        duration_days: int | None = None,
    ) -> SuspensionCreate | None:
        """
        Create a new suspension for a policy.

        Args:
            policy_id: Policy UUID
            policy: Policy data dictionary
            current_date: Current simulation date
            suspension_type: Type of suspension (random if not specified)
            duration_days: Duration in days (random within type range if not specified)

        Returns:
            SuspensionCreate record, or None if suspension cannot be created
        """
        # Check if policy is already suspended
        if policy.get("status") == "Suspended":
            return None

        # Check cumulative suspension days
        cumulative_days = policy.get("cumulative_suspension_days", 0)
        if cumulative_days >= self.MAX_SUSPENSION_DAYS:
            logger.debug(
                "suspension_denied_max_days",
                policy_id=str(policy_id),
                cumulative_days=cumulative_days,
            )
            return None

        # Select suspension type if not specified
        if suspension_type is None:
            suspension_type = self._select_suspension_type()

        # Calculate duration if not specified
        if duration_days is None:
            min_days, max_days = self.SUSPENSION_DURATIONS[suspension_type]
            # Cap at remaining allowed days
            max_allowed = self.MAX_SUSPENSION_DAYS - cumulative_days
            max_days = min(max_days, max_allowed)
            duration_days = int(self.rng.uniform(min_days, max(min_days, max_days)))

        expected_end = current_date + timedelta(days=duration_days)

        suspension = SuspensionCreate(
            suspension_id=self.id_generator.generate_uuid(),
            policy_id=policy_id,
            suspension_type=suspension_type.value,
            start_date=current_date,
            expected_end_date=expected_end,
            actual_end_date=None,
            reason=f"{suspension_type.value} suspension",
            status="Active",
            max_suspension_days=self.MAX_SUSPENSION_DAYS,
            days_used=cumulative_days,
            waiting_period_impact=suspension_type == SuspensionType.OVERSEAS_TRAVEL,
            created_at=self.sim_env.current_datetime,
            created_by="SIMULATION",
        )

        self.batch_writer.add("suspension", suspension.model_dump())

        # Generate prorated refund for suspension period
        policy_obj = policy.get("policy")
        if policy_obj is not None:
            refund_amount = self._calculate_suspension_refund(
                policy_obj, current_date, duration_days
            )
            if refund_amount is not None and refund_amount >= Decimal("1.00"):
                # Get primary member for refund
                members = policy.get("members", [])
                primary_member = members[0] if members else None

                try:
                    refund = self.billing_gen.generate_refund(
                        policy=policy_obj,
                        refund_date=current_date,
                        refund_amount=refund_amount,
                        refund_reason=f"Prorated refund for {suspension_type.value} suspension",
                        refund_type="Suspension",
                        member=primary_member,
                    )
                    self.batch_writer.add("refund", refund.model_dump())

                    logger.debug(
                        "suspension_refund_created",
                        policy_id=str(policy_id),
                        suspension_id=str(suspension.suspension_id),
                        refund_amount=float(refund_amount),
                    )
                except Exception as e:
                    logger.error(
                        "suspension_refund_failed",
                        policy_id=str(policy_id),
                        suspension_id=str(suspension.suspension_id),
                        error=str(e),
                        refund_amount=float(refund_amount) if refund_amount else None,
                    )

        # Update policy status in memory
        policy["status"] = "Suspended"
        policy["suspension_end"] = expected_end
        policy["current_suspension_id"] = suspension.suspension_id
        policy["cumulative_suspension_days"] = cumulative_days + duration_days
        self.active_policies[policy_id] = policy

        # Track the active suspension
        self.active_suspensions[suspension.suspension_id] = {
            "policy_id": policy_id,
            "expected_end_date": expected_end,
            "suspension_type": suspension_type.value,
            "start_date": current_date,
        }

        self.increment_stat("suspensions_created")
        logger.debug(
            "policy_suspended",
            policy_id=str(policy_id),
            suspension_id=str(suspension.suspension_id),
            type=suspension_type.value,
            duration_days=duration_days,
            expected_end=expected_end.isoformat(),
        )

        return suspension

    def _calculate_suspension_refund(
        self,
        policy,
        suspension_start: date,
        suspension_days: int,
    ) -> Decimal | None:
        """
        Calculate prorated refund for suspension period.

        Args:
            policy: Policy object with premium_amount
            suspension_start: Date suspension starts
            suspension_days: Number of days of suspension

        Returns:
            Refund amount, or None if no refund applicable
        """
        if policy is None or not hasattr(policy, "premium_amount"):
            return None

        monthly_premium = policy.premium_amount
        if monthly_premium is None or monthly_premium <= Decimal("0"):
            return None

        # Calculate days in current billing period (assume monthly billing)
        period_end = last_of_month(suspension_start)
        period_start = suspension_start.replace(day=1)
        days_in_period = (period_end - period_start).days + 1

        # Calculate daily rate
        daily_rate = monthly_premium / Decimal(str(days_in_period))

        # Calculate refund for suspension days within current period
        # Only refund up to the end of current billing period
        days_to_period_end = (period_end - suspension_start).days + 1
        refundable_days = min(suspension_days, days_to_period_end)

        if refundable_days <= 0:
            return None

        refund_amount = daily_rate * Decimal(str(refundable_days))
        return refund_amount

    def _check_suspension_reactivations(self, current_date: date) -> None:
        """
        Check for suspensions that should end and reactivate policies.

        Args:
            current_date: Current simulation date
        """
        # Check each active policy for suspension end
        for policy_id, policy in list(self.active_policies.items()):
            if policy.get("status") != "Suspended":
                continue

            suspension_end = policy.get("suspension_end")
            if suspension_end and current_date >= suspension_end:
                self._reactivate_policy(policy_id, policy, current_date)

    def _reactivate_policy(
        self,
        policy_id: UUID,
        policy: dict,
        current_date: date,
    ) -> None:
        """
        Reactivate a suspended policy.

        Args:
            policy_id: Policy UUID
            policy: Policy data dictionary
            current_date: Current date (reactivation date)
        """
        suspension_id = policy.get("current_suspension_id")

        # Update policy status in memory
        policy["status"] = "Active"
        old_suspension_end = policy.get("suspension_end")
        policy["suspension_end"] = None
        policy["current_suspension_id"] = None
        self.active_policies[policy_id] = policy

        # Remove from active suspensions
        if suspension_id:
            self.active_suspensions.pop(suspension_id, None)

        # Write SQL to update SUSPENSION record with actual_end_date
        if suspension_id:
            sql = f"""
                UPDATE suspension
                SET actual_end_date = '{current_date.isoformat()}',
                    status = 'Completed',
                    modified_at = '{self.sim_env.current_datetime.isoformat()}',
                    modified_by = 'SIMULATION'
                WHERE suspension_id = '{suspension_id}'
            """
            self.batch_writer.add_raw_sql("suspension_update", sql)

        # Write SQL to update POLICY status back to Active
        sql = f"""
            UPDATE policy
            SET policy_status = 'Active',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
        """
        self.batch_writer.add_raw_sql("policy_update", sql)

        self.increment_stat("suspensions_reactivated")
        logger.debug(
            "policy_reactivated",
            policy_id=str(policy_id),
            suspension_id=str(suspension_id) if suspension_id else None,
            original_end=old_suspension_end.isoformat() if old_suspension_end else None,
            reactivation_date=current_date.isoformat(),
        )

    def extend_suspension(
        self,
        policy_id: UUID,
        policy: dict,
        extension_days: int,
        current_date: date,
    ) -> bool:
        """
        Extend an active suspension.

        Args:
            policy_id: Policy UUID
            policy: Policy data dictionary
            extension_days: Number of days to extend
            current_date: Current simulation date

        Returns:
            True if extension was successful, False otherwise
        """
        if policy.get("status") != "Suspended":
            return False

        suspension_id = policy.get("current_suspension_id")
        if not suspension_id:
            return False

        # Check cumulative days limit
        cumulative_days = policy.get("cumulative_suspension_days", 0)
        if cumulative_days + extension_days > self.MAX_SUSPENSION_DAYS:
            remaining = self.MAX_SUSPENSION_DAYS - cumulative_days
            if remaining <= 0:
                return False
            extension_days = remaining  # Extend only by remaining allowed days

        # Update expected end date
        current_end = policy.get("suspension_end")
        if current_end is None:
            return False

        new_end = current_end + timedelta(days=extension_days)

        # Update policy in memory
        policy["suspension_end"] = new_end
        policy["cumulative_suspension_days"] = cumulative_days + extension_days
        self.active_policies[policy_id] = policy

        # Update active suspension tracking
        if suspension_id in self.active_suspensions:
            self.active_suspensions[suspension_id]["expected_end_date"] = new_end

        # Write SQL to update SUSPENSION record
        sql = f"""
            UPDATE suspension
            SET expected_end_date = '{new_end.isoformat()}',
                days_used = {cumulative_days + extension_days},
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE suspension_id = '{suspension_id}'
        """
        self.batch_writer.add_raw_sql("suspension_update", sql)

        self.increment_stat("suspensions_extended")
        logger.debug(
            "suspension_extended",
            policy_id=str(policy_id),
            suspension_id=str(suspension_id),
            extension_days=extension_days,
            new_end_date=new_end.isoformat(),
        )

        return True

    def _select_suspension_type(self) -> SuspensionType:
        """Select a random suspension type based on weights."""
        types = list(self.SUSPENSION_TYPE_WEIGHTS.keys())
        weights = list(self.SUSPENSION_TYPE_WEIGHTS.values())
        idx = self.rng.choice(len(types), p=weights)
        return types[idx]

    def _log_progress(self) -> None:
        """Log suspension process progress."""
        stats = self.get_stats()
        active_count = sum(
            1 for p in self.active_policies.values()
            if p.get("status") == "Suspended"
        )
        logger.info(
            "suspension_progress",
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            active_suspensions=active_count,
            suspensions_created=stats.get("suspensions_created", 0),
            suspensions_reactivated=stats.get("suspensions_reactivated", 0),
            suspensions_extended=stats.get("suspensions_extended", 0),
        )
