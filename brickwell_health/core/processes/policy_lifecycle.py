"""
Policy lifecycle process for Brickwell Health Simulator.

Handles policy changes: upgrades, downgrades, cancellations, suspensions.
Uses ChurnPredictionModel for age-based churn with retention factors.
"""

from datetime import timedelta
from decimal import Decimal
from typing import Generator, Any, TYPE_CHECKING
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.enums import (
    CancellationReason,
    CoverageTier,
    PolicyStatus,
)
from brickwell_health.domain.policy import UpgradeRequestCreate
from brickwell_health.generators.billing_generator import BillingGenerator
from brickwell_health.generators.coverage_generator import CoverageGenerator
from brickwell_health.generators.waiting_period_generator import WaitingPeriodGenerator
from brickwell_health.statistics.churn_model import ChurnPredictionModel
from brickwell_health.utils.time_conversion import last_of_month

if TYPE_CHECKING:
    from brickwell_health.core.processes.suspension import SuspensionProcess
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class PolicyLifecycleProcess(BaseProcess):
    """
    SimPy process for policy lifecycle events.

    Handles:
    - Upgrades (Bronze → Silver → Gold)
    - Downgrades (Gold → Silver → Bronze)
    - Cancellations
    - Suspensions

    Events are processed daily with annual rates converted to daily probabilities.
    """

    # Tier ordering for upgrades/downgrades (tier_id: 4=Basic, 3=Bronze, 2=Silver, 1=Gold)
    TIER_ORDER = ["Basic", "Bronze", "Silver", "Gold"]
    TIER_ID_MAP = {"Basic": 4, "Bronze": 3, "Silver": 2, "Gold": 1}

    def __init__(
        self,
        *args: Any,
        active_policies: dict[UUID, dict] | None = None,
        suspension_process: "SuspensionProcess | None" = None,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """
        Initialize the lifecycle process.

        Args:
            active_policies: Dictionary of active policies to process
                            (policy_id -> policy data dict)
            suspension_process: Optional SuspensionProcess instance for delegation
            shared_state: SharedState for cross-process communication
        """
        super().__init__(*args, **kwargs)

        # Track active policies (use explicit None check to preserve empty dict references)
        self.active_policies = active_policies if active_policies is not None else {}

        # Suspension process for delegation
        self.suspension_process = suspension_process

        # Shared state for member change events
        self.shared_state = shared_state

        # Initialize generators
        self.coverage_gen = CoverageGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.waiting_gen = WaitingPeriodGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.billing_gen = BillingGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)

        # Initialize churn prediction model
        self.churn_model = ChurnPredictionModel(self.rng, self.reference, self.config)

        # Daily rates (converted from annual) for upgrade/downgrade/suspension
        # Note: Cancellation now uses ChurnPredictionModel instead of flat rate
        self.upgrade_daily = self.config.events.upgrade_rate / 365
        self.downgrade_daily = self.config.events.downgrade_rate / 365
        self.suspend_daily = self.config.events.suspension_rate / 365

        # Build product tier lookup: product_id -> tier_id
        # and tier products lookup: tier_id -> list of product_ids
        self._build_product_tier_lookups()

    def run(self) -> Generator:
        """
        Main lifecycle process loop.

        Checks each active policy daily for lifecycle events.
        """
        logger.info(
            "lifecycle_process_started",
            worker_id=self.worker_id,
            upgrade_rate=f"{self.config.events.upgrade_rate:.1%}",
            downgrade_rate=f"{self.config.events.downgrade_rate:.1%}",
            cancel_model="ChurnPredictionModel (age-based)",
            suspend_rate=f"{self.config.events.suspension_rate:.1%}",
        )

        while True:
            # Skip processing during warmup's first week
            if self.sim_env.now < 7:
                yield self.env.timeout(1.0)
                continue

            current_date = self.sim_env.current_date

            # Process each active policy
            policy_ids = list(self.active_policies.keys())
            for policy_id in policy_ids:
                policy = self.active_policies.get(policy_id)
                if policy is None:
                    continue

                # Skip if not active
                if policy.get("status") != "Active":
                    continue

                # Randomly select one event (mutually exclusive per day)
                # Cancellation uses ChurnPredictionModel for age-based probability
                event_type = self._select_event(policy, current_date)
                if event_type is None:
                    continue

                if event_type == "upgrade":
                    self._process_upgrade(policy_id, policy, current_date)
                elif event_type == "downgrade":
                    self._process_downgrade(policy_id, policy, current_date)
                elif event_type == "cancel":
                    self._process_cancellation(policy_id, policy, current_date)
                elif event_type == "suspend":
                    self._process_suspension(policy_id, policy, current_date)

            # Process member change events (death, address changes)
            self._process_member_change_events(current_date)

            # Wait until next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _select_event(self, policy: dict, current_date) -> str | None:
        """
        Select which event (if any) should occur.

        Events are mutually exclusive - only one can occur per policy per day.
        Cancellation probability is calculated using ChurnPredictionModel based on
        member age, tenure, claims history, and retention factors.

        Args:
            policy: Policy data dictionary
            current_date: Current simulation date

        Returns:
            Event type string or None if no event
        """
        rand = self.rng.random()

        # Calculate churn probability using the model
        cancel_daily = self._get_churn_probability(policy, current_date)

        cumulative = 0.0
        for event, rate in [
            ("upgrade", self.upgrade_daily),
            ("downgrade", self.downgrade_daily),
            ("cancel", cancel_daily),
            ("suspend", self.suspend_daily),
        ]:
            cumulative += rate
            if rand < cumulative:
                return event

        return None

    def _get_churn_probability(self, policy: dict, current_date) -> float:
        """
        Get daily churn probability for a policy using ChurnPredictionModel.

        Args:
            policy: Policy data dictionary
            current_date: Current simulation date

        Returns:
            Daily churn probability (0-1)
        """
        # Get primary member age
        members = policy.get("members", [])
        primary_member = members[0] if members else None

        if primary_member is None:
            # Fall back to default rate if no member info
            return self.config.events.cancellation_rate / 365

        # Calculate member age
        dob = primary_member.date_of_birth if hasattr(primary_member, "date_of_birth") else None
        if dob is None:
            dob = primary_member.get("date_of_birth") if isinstance(primary_member, dict) else None

        if dob is None:
            return self.config.events.cancellation_rate / 365

        age = (current_date - dob).days // 365

        # Build policy data for churn model
        policy_obj = policy.get("policy")
        effective_date = None
        annual_premium = 0

        if policy_obj is not None:
            if hasattr(policy_obj, "effective_date"):
                effective_date = policy_obj.effective_date
            if hasattr(policy_obj, "premium_amount"):
                annual_premium = float(policy_obj.premium_amount) * 12

        tenure_years = 0
        if effective_date:
            tenure_years = (current_date - effective_date).days // 365

        policy_data = {
            "tenure_years": tenure_years,
            "annual_premium": annual_premium,
            "has_lhc_loading": policy.get("has_lhc_loading", False),
            "mls_subject": policy.get("mls_subject", False),
        }

        # Build claims history (simplified - could be enhanced with real tracking)
        claims_history = self._get_claims_history(policy)

        # Get daily probability from churn model
        daily_prob = self.churn_model.predict_daily_churn_probability(
            member_age=age,
            policy_data=policy_data,
            claims_history=claims_history,
            current_date=current_date,
        )

        # Store policy_data on the policy dict for use in cancellation reason sampling
        policy["_churn_policy_data"] = policy_data

        return daily_prob

    def _get_claims_history(self, policy: dict) -> dict:
        """
        Get claims history for a policy.

        Args:
            policy: Policy data dictionary

        Returns:
            Dictionary with claims history metrics
        """
        # This is a simplified implementation
        # In a full implementation, this would query actual claims data
        return {
            "days_since_last_claim": policy.get("days_since_last_claim"),
            "denial_count": policy.get("denial_count", 0),
            "high_out_of_pocket": policy.get("high_out_of_pocket", False),
            "total_claims_amount": policy.get("total_claims_amount", 0),
        }

    def _build_product_tier_lookups(self) -> None:
        """Build lookups for product tiers from reference data."""
        products = self.reference.get_products(active_only=True)

        # product_id -> tier_id
        self.product_tier: dict[int, int] = {}
        # tier_id -> list of (product_id, excess_amount) tuples
        self.tier_products: dict[int, list[tuple[int, Decimal | None]]] = {}

        for p in products:
            product_id = p.get("product_id")
            tier_id = p.get("product_tier_id")
            # Try to extract excess from product name (e.g., "$500 Excess")
            name = p.get("product_name", "")
            excess = None
            if "$250" in name:
                excess = Decimal("250")
            elif "$500" in name:
                excess = Decimal("500")
            elif "$750" in name:
                excess = Decimal("750")
            elif "No Excess" in name:
                excess = Decimal("0")

            if product_id and tier_id:
                self.product_tier[product_id] = tier_id
                if tier_id not in self.tier_products:
                    self.tier_products[tier_id] = []
                self.tier_products[tier_id].append((product_id, excess))

    def _find_product_in_tier(
        self,
        target_tier: str,
        current_excess: Decimal | None,
    ) -> int | None:
        """
        Find a product in the target tier, preferring one with matching excess.

        Args:
            target_tier: Target tier name (Basic/Bronze/Silver/Gold)
            current_excess: Current policy excess amount

        Returns:
            Product ID in the target tier, or None if not found
        """
        target_tier_id = self.TIER_ID_MAP.get(target_tier)
        if target_tier_id is None:
            return None

        products_in_tier = self.tier_products.get(target_tier_id, [])
        if not products_in_tier:
            return None

        # Try to find a product with matching excess
        if current_excess is not None:
            for product_id, excess in products_in_tier:
                if excess == current_excess:
                    return product_id

        # Fall back to first product in tier
        return products_in_tier[0][0]

    def _process_upgrade(
        self,
        policy_id: UUID,
        policy: dict,
        current_date,
    ) -> None:
        """
        Process a policy upgrade.

        Upgrades move hospital tier up: Basic → Bronze → Silver → Gold
        """
        current_tier = policy.get("tier", "Bronze")

        # Find next tier up
        try:
            current_idx = self.TIER_ORDER.index(current_tier)
        except ValueError:
            return

        if current_idx >= len(self.TIER_ORDER) - 1:
            # Already at highest tier
            return

        new_tier = self.TIER_ORDER[current_idx + 1]
        current_product_id = policy.get("product_id", 1)
        current_excess = policy.get("excess")

        # Find a matching product in the new tier
        new_product_id = self._find_product_in_tier(new_tier, current_excess)
        if new_product_id is None:
            # Can't find a suitable product, skip upgrade
            return

        # Create upgrade request
        upgrade_request = UpgradeRequestCreate(
            upgrade_request_id=self.id_generator.generate_uuid(),
            policy_id=policy_id,
            request_type="Upgrade",
            current_product_id=current_product_id,
            requested_product_id=new_product_id,
            current_excess=current_excess,
            requested_excess=current_excess,  # Keep same excess level
            requested_effective_date=current_date,
            request_reason="Member requested upgrade",
            request_status="Approved",
            submission_date=self.sim_env.current_datetime,
            decision_date=self.sim_env.current_datetime,
            decision_by="SYSTEM",
            requires_waiting_period=True,
            waiting_period_details="New waiting periods apply to upgraded benefits",
            created_at=self.sim_env.current_datetime,
            created_by="SIMULATION",
        )

        self.batch_writer.add("upgrade_request", upgrade_request.model_dump())

        # Update policy tier and product in memory
        policy["tier"] = new_tier
        policy["product_id"] = new_product_id
        self.active_policies[policy_id] = policy

        self.increment_stat("upgrades")
        logger.debug(
            "policy_upgraded",
            policy_id=str(policy_id),
            from_tier=current_tier,
            to_tier=new_tier,
            from_product=current_product_id,
            to_product=new_product_id,
        )

    def _process_downgrade(
        self,
        policy_id: UUID,
        policy: dict,
        current_date,
    ) -> None:
        """
        Process a policy downgrade.

        Downgrades move hospital tier down: Gold → Silver → Bronze → Basic
        """
        current_tier = policy.get("tier", "Silver")

        # Find next tier down
        try:
            current_idx = self.TIER_ORDER.index(current_tier)
        except ValueError:
            return

        if current_idx <= 0:
            # Already at lowest tier
            return

        new_tier = self.TIER_ORDER[current_idx - 1]
        current_product_id = policy.get("product_id", 1)
        current_excess = policy.get("excess")

        # Find a matching product in the new tier
        new_product_id = self._find_product_in_tier(new_tier, current_excess)
        if new_product_id is None:
            # Can't find a suitable product, skip downgrade
            return

        # Create upgrade request (with type=Downgrade)
        downgrade_request = UpgradeRequestCreate(
            upgrade_request_id=self.id_generator.generate_uuid(),
            policy_id=policy_id,
            request_type="Downgrade",
            current_product_id=current_product_id,
            requested_product_id=new_product_id,
            current_excess=current_excess,
            requested_excess=current_excess,  # Keep same excess level
            requested_effective_date=current_date,
            request_reason="Member requested downgrade",
            request_status="Approved",
            submission_date=self.sim_env.current_datetime,
            decision_date=self.sim_env.current_datetime,
            decision_by="SYSTEM",
            requires_waiting_period=False,
            waiting_period_details=None,
            created_at=self.sim_env.current_datetime,
            created_by="SIMULATION",
        )

        self.batch_writer.add("upgrade_request", downgrade_request.model_dump())

        # Update policy tier and product in memory
        policy["tier"] = new_tier
        policy["product_id"] = new_product_id
        self.active_policies[policy_id] = policy

        self.increment_stat("downgrades")
        logger.debug(
            "policy_downgraded",
            policy_id=str(policy_id),
            from_tier=current_tier,
            to_tier=new_tier,
            from_product=current_product_id,
            to_product=new_product_id,
        )

    def _process_cancellation(
        self,
        policy_id: UUID,
        policy: dict,
        current_date,
    ) -> None:
        """
        Process a policy cancellation.

        Writes cancellation records to database:
        - Updates POLICY status to 'Cancelled' with end_date
        - Ends all COVERAGE records
        - Ends all POLICY_MEMBER records
        - Creates prorated refund for unused premium
        - Tracks cancellation reason using ChurnPredictionModel
        """
        # Sample cancellation reason using the churn model
        policy_data = policy.get("_churn_policy_data", {})
        cancellation_reason = self.churn_model.sample_cancellation_reason(policy_data)

        # Calculate prorated refund for unused portion of current month
        policy_obj = policy.get("policy")
        if policy_obj is not None:
            refund_amount = self._calculate_prorated_refund(
                policy_obj, current_date, "Cancellation"
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
                        refund_reason=f"Prorated refund for policy cancellation - {cancellation_reason}",
                        refund_type="Cancellation",
                        member=primary_member,
                    )
                    self.batch_writer.add("refund", refund.model_dump())

                    logger.debug(
                        "cancellation_refund_created",
                        policy_id=str(policy_id),
                        refund_amount=float(refund_amount),
                    )
                except Exception as e:
                    logger.error(
                        "cancellation_refund_failed",
                        policy_id=str(policy_id),
                        error=str(e),
                        refund_amount=float(refund_amount) if refund_amount else None,
                    )

        # Remove from active policies
        del self.active_policies[policy_id]

        # Map reason string to descriptive message for SQL
        reason_messages = {
            "Price": "Premium too expensive",
            "NoValue": "Not using coverage - low perceived value",
            "Switching": "Moving to competitor insurer",
            "LifeEvent": "Life event (job loss, divorce, etc.)",
            "Deceased": "Primary member deceased",
            "Other": "Other reasons",
        }
        reason_message = reason_messages.get(cancellation_reason, cancellation_reason)

        # Write SQL to update POLICY status to Cancelled with reason
        sql = f"""
            UPDATE policy
            SET policy_status = 'Cancelled',
                end_date = '{current_date.isoformat()}',
                cancellation_reason = '{reason_message}',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
        """
        self.batch_writer.add_raw_sql("policy_cancellation", sql)

        # Write SQL to end all COVERAGE records for this policy
        sql = f"""
            UPDATE coverage
            SET status = 'Terminated',
                end_date = '{current_date.isoformat()}',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
              AND (status = 'Active' OR status IS NULL)
        """
        self.batch_writer.add_raw_sql("coverage_termination", sql)

        # Write SQL to end all POLICY_MEMBER records for this policy
        sql = f"""
            UPDATE policy_member
            SET is_active = FALSE,
                end_date = '{current_date.isoformat()}'
            WHERE policy_id = '{policy_id}'
              AND is_active = TRUE
        """
        self.batch_writer.add_raw_sql("policy_member_termination", sql)

        self.increment_stat("cancellations")

        # Track cancellation by reason
        reason_stat = f"cancellation_reason_{cancellation_reason.lower()}"
        self.increment_stat(reason_stat)

        logger.debug(
            "policy_cancelled",
            policy_id=str(policy_id),
            date=current_date.isoformat(),
            reason=cancellation_reason,
        )

    def _calculate_prorated_refund(
        self,
        policy,
        event_date,
        event_type: str,
    ) -> Decimal | None:
        """
        Calculate prorated refund for unused premium.

        For cancellation: refund for remaining days in current billing period.
        For suspension: refund for the suspension duration.

        Args:
            policy: Policy object with premium_amount
            event_date: Date of cancellation or suspension start
            event_type: "Cancellation" or "Suspension"

        Returns:
            Refund amount, or None if no refund applicable
        """
        if policy is None or not hasattr(policy, "premium_amount"):
            return None

        monthly_premium = policy.premium_amount
        if monthly_premium is None or monthly_premium <= Decimal("0"):
            return None

        # Calculate days in current billing period (assume monthly billing)
        period_end = last_of_month(event_date)
        period_start = event_date.replace(day=1)
        days_in_period = (period_end - period_start).days + 1

        # Calculate daily rate
        daily_rate = monthly_premium / Decimal(str(days_in_period))

        # Calculate unused days
        unused_days = (period_end - event_date).days

        if unused_days <= 0:
            return None

        refund_amount = daily_rate * Decimal(str(unused_days))
        return refund_amount

    def _process_suspension(
        self,
        policy_id: UUID,
        policy: dict,
        current_date,
    ) -> None:
        """
        Process a policy suspension.

        Delegates to SuspensionProcess if available, otherwise logs a warning.
        Suspensions pause the policy for overseas travel or financial hardship.
        """
        if self.suspension_process is None:
            logger.warning(
                "suspension_process_not_available",
                policy_id=str(policy_id),
            )
            return

        # Delegate to SuspensionProcess
        suspension = self.suspension_process.create_suspension(
            policy_id=policy_id,
            policy=policy,
            current_date=current_date,
        )

        if suspension is not None:
            self.increment_stat("suspensions")

    def set_suspension_process(self, suspension_process: "SuspensionProcess") -> None:
        """
        Set the suspension process for delegation.

        Allows setting suspension process after initialization to avoid
        circular dependency issues.

        Args:
            suspension_process: SuspensionProcess instance
        """
        self.suspension_process = suspension_process

    def add_policy(self, policy_id: UUID, policy_data: dict) -> None:
        """
        Add a policy to track.

        Called by acquisition process when new policies are created.

        Args:
            policy_id: Policy UUID
            policy_data: Policy data dictionary
        """
        self.active_policies[policy_id] = policy_data

    def remove_policy(self, policy_id: UUID) -> None:
        """
        Remove a policy from tracking.

        Args:
            policy_id: Policy UUID
        """
        self.active_policies.pop(policy_id, None)

    def _process_member_change_events(self, current_date) -> None:
        """
        Process member change events from MemberLifecycleProcess.

        Handles:
        - DEATH: Cancel policy or transfer to surviving member
        - ADDRESS_CHANGE: Update policy address, may affect ambulance coverage
        """
        if not self.shared_state:
            return

        # Process death events
        death_events = self.shared_state.get_member_change_events("DEATH")
        for event in death_events:
            self._handle_member_death(event, current_date)

        # Process address changes (may affect ambulance coverage by state)
        address_events = self.shared_state.get_member_change_events("ADDRESS_CHANGE")
        for event in address_events:
            self._handle_address_change(event, current_date)

    def _handle_member_death(self, event: dict, current_date) -> None:
        """
        Handle policy implications of member death.

        - Primary death: Cancel policy with reason "Deceased"
        - Partner/Dependent death: Remove from policy, adjust policy type
        """
        policy_id = event["policy_id"]
        member_id = event["member_id"]
        change_data = event.get("change_data", {})
        member_role = change_data.get("member_role", "Primary")

        policy = self.active_policies.get(policy_id)
        if not policy:
            return

        if member_role == "Primary":
            # Primary death: Cancel the policy
            self._cancel_for_death(policy_id, policy, current_date)
        else:
            # Partner or Dependent death: Remove from policy
            self._remove_member_from_policy(policy_id, member_id, member_role, current_date)

        self.increment_stat("member_deaths_processed")

    def _cancel_for_death(self, policy_id: UUID, policy: dict, current_date) -> None:
        """Cancel policy due to primary member death."""
        # Remove from active policies
        del self.active_policies[policy_id]

        # Write SQL to update POLICY status to Cancelled with death reason
        sql = f"""
            UPDATE policy
            SET policy_status = 'Cancelled',
                end_date = '{current_date.isoformat()}',
                cancellation_reason = 'Primary member deceased',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
        """
        self.batch_writer.add_raw_sql("policy_cancellation_death", sql)

        # End all COVERAGE records
        sql = f"""
            UPDATE coverage
            SET status = 'Terminated',
                end_date = '{current_date.isoformat()}',
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE policy_id = '{policy_id}'
              AND (status = 'Active' OR status IS NULL)
        """
        self.batch_writer.add_raw_sql("coverage_termination_death", sql)

        # End all POLICY_MEMBER records
        sql = f"""
            UPDATE policy_member
            SET is_active = FALSE,
                end_date = '{current_date.isoformat()}'
            WHERE policy_id = '{policy_id}'
              AND is_active = TRUE
        """
        self.batch_writer.add_raw_sql("policy_member_termination_death", sql)

        # Remove members from shared state tracking
        if self.shared_state:
            self.shared_state.remove_policy_members(policy_id)

        self.increment_stat("cancellations")
        self.increment_stat("cancellation_reason_deceased")

        logger.info(
            "policy_cancelled_death",
            policy_id=str(policy_id),
            date=current_date.isoformat(),
        )

    def _remove_member_from_policy(
        self,
        policy_id: UUID,
        member_id: UUID,
        member_role: str,
        current_date,
    ) -> None:
        """Remove a member from policy (partner/dependent death)."""
        policy = self.active_policies.get(policy_id)
        if not policy:
            return

        # Update POLICY_MEMBER record to inactive
        sql = f"""
            UPDATE policy_member
            SET is_active = FALSE,
                end_date = '{current_date.isoformat()}'
            WHERE policy_id = '{policy_id}'
              AND member_id = '{member_id}'
              AND is_active = TRUE
        """
        self.batch_writer.add_raw_sql("policy_member_removal", sql)

        # Potentially update policy type
        current_type = policy.get("policy_type", "Single")
        new_type = None

        if member_role == "Partner":
            # Partner death: Family -> Single Parent, Couple -> Single
            if current_type == "Family":
                new_type = "Single Parent"
            elif current_type == "Couple":
                new_type = "Single"
        elif member_role == "Dependent":
            # Dependent death: May need to check remaining dependents
            # If no dependents left, Family -> Couple, Single Parent -> Single
            # For simplicity, we don't track exact counts, just log it
            pass

        if new_type:
            # Update policy type
            sql = f"""
                UPDATE policy
                SET policy_type = '{new_type}',
                    modified_at = '{self.sim_env.current_datetime.isoformat()}',
                    modified_by = 'SIMULATION'
                WHERE policy_id = '{policy_id}'
            """
            self.batch_writer.add_raw_sql("policy_type_change", sql)

            policy["policy_type"] = new_type

            logger.debug(
                "policy_type_changed",
                policy_id=str(policy_id),
                from_type=current_type,
                to_type=new_type,
                reason="member_death",
            )

        # Remove from shared state tracking if available
        if self.shared_state:
            # Find and remove the policy_member by member_id
            for pm_id, data in list(self.shared_state.policy_members.items()):
                member = data.get("member")
                if member and member.member_id == member_id:
                    self.shared_state.remove_policy_member(pm_id)
                    break

    def _handle_address_change(self, event: dict, current_date) -> None:
        """
        Handle address change for a member.

        Updates policy address and checks if ambulance coverage needs adjustment
        (ambulance coverage varies by state in Australia).
        """
        policy_id = event["policy_id"]
        change_data = event.get("change_data", {})
        previous_state = change_data.get("previous_state")
        new_state = change_data.get("new_state")
        member_role = change_data.get("member_role")

        policy = self.active_policies.get(policy_id)
        if not policy:
            return

        # Only update policy address if primary member moved
        if member_role != "Primary":
            return

        # Log interstate moves (may affect ambulance coverage)
        if previous_state != new_state:
            logger.debug(
                "policy_interstate_move",
                policy_id=str(policy_id),
                from_state=previous_state,
                to_state=new_state,
            )
            self.increment_stat("interstate_moves")

        # Note: Full ambulance coverage adjustment would require checking state-specific
        # ambulance schemes and updating coverage records. For now, just track the event.
        self.increment_stat("address_changes_processed")

    def _log_progress(self) -> None:
        """Log lifecycle progress."""
        stats = self.get_stats()
        logger.info(
            "lifecycle_progress",
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            active_policies=len(self.active_policies),
            upgrades=stats.get("upgrades", 0),
            downgrades=stats.get("downgrades", 0),
            cancellations=stats.get("cancellations", 0),
            suspensions=stats.get("suspensions", 0),
            member_deaths_processed=stats.get("member_deaths_processed", 0),
            address_changes_processed=stats.get("address_changes_processed", 0),
        )
