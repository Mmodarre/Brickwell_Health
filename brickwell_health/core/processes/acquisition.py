"""
Acquisition process for Brickwell Health Simulator.

Handles new member acquisition through applications.
"""

from datetime import timedelta
from typing import Generator, Any, TYPE_CHECKING

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.enums import (
    PolicyType,
    DistributionChannel,
    ApplicationStatus,
    CoverageType,
)
from brickwell_health.generators.member_generator import MemberGenerator
from brickwell_health.generators.application_generator import ApplicationGenerator
from brickwell_health.generators.policy_generator import PolicyGenerator
from brickwell_health.generators.coverage_generator import CoverageGenerator
from brickwell_health.generators.waiting_period_generator import WaitingPeriodGenerator
from brickwell_health.generators.regulatory_generator import RegulatoryGenerator
from brickwell_health.generators.billing_generator import BillingGenerator
from brickwell_health.generators.communication_generator import CommunicationPreferenceGenerator
from brickwell_health.statistics.product_selection import ProductSelectionModel

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class AcquisitionProcess(BaseProcess):
    """
    SimPy process for new member acquisition.

    Generates applications at Poisson rate, processes them, and creates
    policies/members for approved applications.

    Acquisition Rate Calculation:
    - Warmup period: target_members / warmup_days / approval_rate
    - Steady state: (growth_rate + churn_rate) * target_members / 365 / approval_rate

    Example at 100k members:
    - Warmup (730 days): 100000 / 730 / 0.92 ≈ 149 applications/day
    - Steady: (0.03 + 0.10) * 100000 / 365 / 0.92 ≈ 39 applications/day
    """

    def __init__(
        self,
        *args: Any,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """Initialize the acquisition process."""
        super().__init__(*args, **kwargs)

        # Shared state for cross-process communication
        self.shared_state = shared_state

        # Initialize generators
        self.member_gen = MemberGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.app_gen = ApplicationGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.policy_gen = PolicyGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.coverage_gen = CoverageGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.waiting_gen = WaitingPeriodGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.regulatory_gen = RegulatoryGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.billing_gen = BillingGenerator(self.rng, self.reference, self.id_generator, sim_env=self.sim_env)
        self.product_selector = ProductSelectionModel(
            self.rng,
            self.reference,
            self.config.policy.tier_distribution,
        )
        self.preference_gen = CommunicationPreferenceGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
        )

        # Statistics
        self._applications_submitted = 0
        self._applications_approved = 0
        self._applications_declined = 0
        self._members_created = 0
        self._policies_created = 0

    def run(self) -> Generator:
        """
        Main acquisition process loop.

        Generates applications at calculated rate and processes them.
        """
        logger.info(
            "acquisition_process_started",
            worker_id=self.worker_id,
            warmup_days=self.config.simulation.warmup_days,
            target_members=self.config.scale.target_member_count,
        )

        warmup_days = self.config.simulation.warmup_days

        while True:
            # Calculate current rate
            if self.sim_env.now < warmup_days:
                daily_rate = self._calculate_warmup_rate()
            else:
                daily_rate = self._calculate_steady_rate()

            # Generate arrivals for this day
            num_arrivals = self.rng.poisson(daily_rate)

            for _ in range(num_arrivals):
                # Spawn application as separate process (don't block on decision time)
                self.env.process(self._process_application())

            # Wait until next day
            yield self.env.timeout(1.0)

            # Log progress periodically
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _calculate_warmup_rate(self) -> float:
        """
        Calculate daily application rate during warmup.

        Goal: Reach target_member_count by end of warmup.
        """
        target = self.config.scale.target_member_count
        warmup_days = self.config.simulation.warmup_days
        approval_rate = self.config.acquisition.approval_rate

        # Average members per approved application
        avg_members_per_policy = 2.3  # Approximate based on policy type distribution

        # Daily rate to reach target
        policies_needed = target / avg_members_per_policy
        daily_rate = policies_needed / warmup_days / approval_rate

        # Adjust for worker partitioning
        daily_rate /= self.config.parallel.num_workers

        return daily_rate

    def _calculate_steady_rate(self) -> float:
        """
        Calculate daily application rate after warmup.

        Maintains growth while replacing churned members.
        """
        target = self.config.scale.target_member_count
        growth_rate = self.config.scale.target_growth_rate
        churn_rate = self.config.scale.target_churn_rate
        approval_rate = self.config.acquisition.approval_rate

        avg_members_per_policy = 2.3
        policies = target / avg_members_per_policy

        # New policies needed to maintain target + growth
        daily_rate = (growth_rate + churn_rate) * policies / 365 / approval_rate

        # Adjust for worker partitioning
        daily_rate /= self.config.parallel.num_workers

        return daily_rate

    def _process_application(self) -> Generator:
        """
        Process a single application from submission to policy creation.

        Yields control to SimPy during decision time.
        """
        current_date = self.sim_env.current_date

        # Select channel
        channel = self._select_channel()

        # Select policy type
        policy_type = self._select_policy_type()

        # Generate members
        members = self.member_gen.generate_family(
            policy_type=policy_type.value,
            as_of_date=current_date,
        )

        # Select product
        primary_age = self._get_age(members[0].date_of_birth, current_date)
        state = members[0].state
        product = self.product_selector.select_product(
            policy_type=policy_type.value,
            state=state,
            primary_age=primary_age,
        )

        if product is None:
            # No suitable product found, skip
            return

        product_id = product.get("product_id", 1)
        requested_start = current_date + timedelta(days=14)  # 2 week lead time

        # Create application
        application, app_members = self.app_gen.generate(
            members=members,
            policy_type=policy_type,
            product_id=product_id,
            channel=channel,
            requested_start_date=requested_start,
        )

        self._applications_submitted += 1
        self.increment_stat("applications_submitted")

        # Write application and application members
        self.batch_writer.add("application", application.model_dump_db())
        for app_member in app_members:
            self.batch_writer.add("application_member", app_member.model_dump_db())

        # Generate and write health declarations for all application members
        health_declarations = self.app_gen.generate_health_declarations(
            application_id=application.application_id,
            app_members=app_members,
            declaration_date=self.sim_env.current_datetime,
        )
        for declaration in health_declarations:
            self.batch_writer.add("health_declaration", declaration.model_dump())

        self.increment_stat("health_declarations", len(health_declarations))

        # Decision time
        decision_days = self._get_decision_time(channel)
        yield self.env.timeout(decision_days)

        # Make decision
        if self.rng.random() < self.config.acquisition.approval_rate:
            # Approved
            yield from self._create_policy_from_application(
                application, app_members, members, product_id
            )
            self._applications_approved += 1
            self.increment_stat("applications_approved")
        else:
            # Declined
            application = self.app_gen.decline_application(
                application,
                self.sim_env.current_datetime,
                "Application declined - underwriting criteria",
            )
            self._applications_declined += 1
            self.increment_stat("applications_declined")

    def _create_policy_from_application(
        self,
        application,
        app_members,
        members,
        product_id: int,
    ) -> Generator:
        """
        Create policy and all related records from approved application.
        """
        # Update application status
        application = self.app_gen.approve_application(
            application, self.sim_env.current_datetime
        )

        # Write members
        for member in members:
            self.batch_writer.add("member", member.model_dump_db())
            self._members_created += 1
            self.increment_stat("members_created")

        # Create policy
        policy, policy_members = self.policy_gen.generate(
            application=application,
            members=members,
        )

        self.batch_writer.add("policy", policy.model_dump_db())
        for pm in policy_members:
            self.batch_writer.add("policy_member", pm.model_dump_db())

        self._policies_created += 1
        self.increment_stat("policies_created")

        # Create coverages
        coverages = self.coverage_gen.generate_coverages_for_policy(policy)
        for coverage in coverages:
            self.batch_writer.add("coverage", coverage.model_dump_db())

        # Index coverages by type for shared state
        coverage_by_type = {}
        for cov in coverages:
            if cov.coverage_type == CoverageType.HOSPITAL:
                coverage_by_type["hospital"] = cov
            elif cov.coverage_type == CoverageType.EXTRAS:
                coverage_by_type["extras"] = cov
            elif cov.coverage_type == CoverageType.AMBULANCE:
                coverage_by_type["ambulance"] = cov

        # Create waiting periods
        is_transfer = application.transfer_certificate_received
        all_waiting_periods = {}  # policy_member_id -> list of (wp, coverage_type)
        coverage_type_by_id = {cov.coverage_id: cov.coverage_type for cov in coverages}
        for pm in policy_members:
            waiting_periods = self.waiting_gen.generate_waiting_periods_for_member(
                pm, coverages, policy.effective_date, is_transfer
            )
            # Store with coverage type for shared state
            all_waiting_periods[pm.policy_member_id] = [
                (wp, coverage_type_by_id.get(wp.coverage_id))
                for wp in waiting_periods
            ]
            for wp in waiting_periods:
                self.batch_writer.add("waiting_period", wp.model_dump_db())

        # Create regulatory records
        regulatory = self.regulatory_gen.generate_all_regulatory_records(
            policy, members, policy.effective_date
        )
        for lhc in regulatory["lhc_loadings"]:
            self.batch_writer.add("lhc_loading", lhc.model_dump())
        for age_disc in regulatory["age_discounts"]:
            self.batch_writer.add("age_based_discount", age_disc.model_dump())
        for rebate in regulatory["rebate_entitlements"]:
            self.batch_writer.add("phi_rebate_entitlement", rebate.model_dump())

        # Calculate total LHC loading percentage
        total_lhc_pct = sum(lhc.loading_percentage for lhc in regulatory["lhc_loadings"])
        avg_lhc_pct = total_lhc_pct / len(members) if members else 0

        # Calculate age discount (primary member)
        age_discount_pct = 0.0
        if regulatory["age_discounts"]:
            age_discount_pct = float(regulatory["age_discounts"][0].discount_percentage)

        # Get rebate percentage (primary member)
        rebate_pct = 0.0
        if regulatory["rebate_entitlements"]:
            rebate_pct = float(regulatory["rebate_entitlements"][0].rebate_percentage)

        # Create bank account and direct debit mandate
        primary_member = members[0]
        bank_account = self.billing_gen.generate_bank_account(
            primary_member, policy
        )
        self.batch_writer.add("bank_account", bank_account.model_dump())

        mandate = self.billing_gen.generate_direct_debit_mandate(
            policy, bank_account, policy.effective_date
        )
        self.batch_writer.add("direct_debit_mandate", mandate.model_dump())

        # Populate shared state for other processes
        if self.shared_state is not None:
            # Get tier from product
            tier = self._get_product_tier(product_id)

            # Register policy for Billing and Lifecycle processes
            self.shared_state.add_policy(
                policy.policy_id,
                {
                    "policy": policy,
                    "members": members,
                    "coverages": coverages,
                    "tier": tier,
                    "product_id": product_id,
                    "excess": policy.excess_amount,
                    "status": "Active",
                    "mandate": mandate,
                    "lhc_loading": avg_lhc_pct,
                    "age_discount": age_discount_pct,
                    "rebate_pct": rebate_pct,
                },
            )

            # Register each policy member for Claims process
            for i, pm in enumerate(policy_members):
                member = members[i] if i < len(members) else members[0]
                age = self._get_age(member.date_of_birth, self.sim_env.current_date)

                self.shared_state.add_policy_member(
                    pm.policy_member_id,
                    {
                        "policy": policy,
                        "member": member,
                        "policy_member_id": pm.policy_member_id,
                        "age": age,
                        "gender": member.gender,
                        "hospital_coverage": coverage_by_type.get("hospital"),
                        "extras_coverage": coverage_by_type.get("extras"),
                        "ambulance_coverage": coverage_by_type.get("ambulance"),
                    },
                )

                # Register waiting periods
                wp_tuples = all_waiting_periods.get(pm.policy_member_id, [])
                self.shared_state.add_waiting_periods(
                    pm.policy_member_id,
                    [
                        {
                            "coverage_type": cov_type.value if cov_type else None,
                            "start_date": wp.start_date,
                            "end_date": wp.end_date,
                            "waiting_period_type": wp.waiting_period_type.value if hasattr(wp.waiting_period_type, 'value') else str(wp.waiting_period_type),
                        }
                        for wp, cov_type in wp_tuples
                    ],
                )

                # Generate and write communication preferences for each member
                preferences = self.preference_gen.generate_default_preferences(
                    member_id=member.member_id,
                    policy_id=policy.policy_id,
                )
                for pref in preferences:
                    self.batch_writer.add("communication_preference", pref.model_dump_db())

                # Cache preferences in shared state for runtime lookups
                pref_cache = {}
                for pref in preferences:
                    pref_type_str = (
                        pref.preference_type.value
                        if hasattr(pref.preference_type, "value")
                        else str(pref.preference_type)
                    )
                    key = f"{pref_type_str.lower()}_{pref.channel.lower()}"
                    pref_cache[key] = pref.is_opted_in
                self.shared_state.set_communication_preferences(member.member_id, pref_cache)

                # Assign digital engagement level for member
                engagement_level = self._sample_engagement_level()
                self.shared_state.set_engagement_level(member.member_id, engagement_level)

        yield self.env.timeout(0)  # Allow SimPy to process

    def _get_product_tier(self, product_id: int) -> str:
        """Get the tier name for a product."""
        products = self.reference.get_products()
        for p in products:
            if p.get("product_id") == product_id:
                tier_id = p.get("product_tier_id", 3)
                tier_map = {1: "Gold", 2: "Silver", 3: "Bronze", 4: "Basic"}
                return tier_map.get(tier_id, "Bronze")
        return "Bronze"

    def _select_channel(self) -> DistributionChannel:
        """Select distribution channel based on config."""
        channels = list(self.config.acquisition.channels.keys())
        weights = list(self.config.acquisition.channels.values())
        channel_name = self.rng.choice(channels, p=[w / sum(weights) for w in weights])
        return DistributionChannel(channel_name)

    def _select_policy_type(self) -> PolicyType:
        """Select policy type based on config."""
        types = list(self.config.policy.type_distribution.keys())
        weights = list(self.config.policy.type_distribution.values())
        type_name = self.rng.choice(types, p=[w / sum(weights) for w in weights])
        return PolicyType(type_name)

    def _sample_engagement_level(self) -> str:
        """Sample digital engagement level from config distribution."""
        digital_config = getattr(self.config, "digital", None)
        if digital_config and hasattr(digital_config, "engagement_distribution"):
            distribution = digital_config.engagement_distribution
        else:
            distribution = {"high": 0.15, "medium": 0.35, "low": 0.50}

        levels = list(distribution.keys())
        probs = list(distribution.values())
        return self.rng.choice(levels, p=probs)

    def _get_decision_time(self, channel: DistributionChannel) -> float:
        """Get decision time in days for a channel."""
        decision_range = self.config.acquisition.decision_time_days.get(
            channel.value, (1.0, 3.0)
        )
        return self.rng.uniform(*decision_range)

    def _get_age(self, dob, as_of) -> int:
        """Calculate age."""
        age = as_of.year - dob.year
        if (as_of.month, as_of.day) < (dob.month, dob.day):
            age -= 1
        return max(0, age)

    def _log_progress(self) -> None:
        """Log acquisition progress."""
        logger.info(
            "acquisition_progress",
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            sim_date=self.sim_env.current_date.isoformat(),
            applications_submitted=self._applications_submitted,
            applications_approved=self._applications_approved,
            applications_declined=self._applications_declined,
            members_created=self._members_created,
            policies_created=self._policies_created,
        )
