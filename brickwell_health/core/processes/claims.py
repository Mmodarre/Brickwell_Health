"""
Claims process for Brickwell Health Simulator.

Generates claims for policy members (extras, hospital, ambulance).
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import Generator, Any, TYPE_CHECKING
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.coverage import BenefitUsageCreate
from brickwell_health.domain.enums import CoverageType, ClaimType, DenialReason
from brickwell_health.generators.claims_generator import ClaimsGenerator
from brickwell_health.statistics.claim_propensity import ClaimPropensityModel
from brickwell_health.utils.time_conversion import get_age, get_financial_year

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class ClaimsProcess(BaseProcess):
    """
    SimPy process for claims generation.

    Generates:
    - Extras claims (dental, optical, physio, etc.)
    - Hospital admissions
    - Ambulance claims

    Claims are generated at rates based on:
    - Member age
    - Time since waiting periods completed
    - Annual rates from configuration
    """

    def __init__(
        self,
        *args: Any,
        policy_members: dict[UUID, dict] | None = None,
        waiting_periods: dict[UUID, list[dict]] | None = None,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """
        Initialize the claims process.

        Args:
            policy_members: Dictionary of policy_member_id -> member data
            waiting_periods: Dictionary of policy_member_id -> waiting periods
            shared_state: Shared state for cross-process communication (needed to check policy status)
        """
        super().__init__(*args, **kwargs)

        # Track members and their waiting periods
        # Use explicit None check to preserve empty dict references for shared state
        self.policy_members = policy_members if policy_members is not None else {}
        self.waiting_periods = waiting_periods if waiting_periods is not None else {}

        # Shared state for checking policy status
        self.shared_state = shared_state

        # Initialize generators with config for APRA-based claiming patterns
        self.claims_gen = ClaimsGenerator(
            self.rng, self.reference, self.id_generator, self.config.claims
        )
        self.propensity = ClaimPropensityModel(self.rng, self.reference, self.config.claims)

        # Build benefit limit lookup: (product_id, benefit_category_id) -> limit info
        self.benefit_limits = self.reference.build_benefit_limit_lookup()

        # Track cumulative usage: (member_id, benefit_category_id, financial_year) -> total_used
        # Financial year is a string like "2024-2025"
        self.cumulative_usage: dict[tuple[UUID, int, str], Decimal] = {}

        # Benefit category ID mapping for extras services
        self._benefit_category_map = {
            "Dental": 3,        # DENTAL
            "Optical": 7,       # OPTICAL
            "Physiotherapy": 8, # PHYSIO
            "Chiropractic": 9,  # CHIRO
            "Podiatry": 10,     # PODIATRY
            "Psychology": 11,   # PSYCHOLOGY
            "Massage": 13,      # MASSAGE (Remedial Massage)
            "Acupuncture": 14,  # ACUPUNCTURE
        }

    def _should_approve_claim(self, claim_type: ClaimType) -> tuple[bool, DenialReason | None]:
        """
        Stochastic approval check for claims.

        This check is applied AFTER deterministic checks (limits, waiting periods,
        membership status) have passed.

        Args:
            claim_type: Type of claim (HOSPITAL, EXTRAS, AMBULANCE)

        Returns:
            Tuple of (approved: bool, denial_reason: DenialReason | None)
        """
        approval_config = self.config.claims.approval

        # Get approval rate by claim type
        rate_map = {
            ClaimType.HOSPITAL: approval_config.hospital_approval_rate,
            ClaimType.EXTRAS: approval_config.extras_approval_rate,
            ClaimType.AMBULANCE: approval_config.ambulance_approval_rate,
        }
        rate = rate_map.get(claim_type, 0.90)

        # Stochastic approval check
        if self.rng.random() < rate:
            return True, None

        # Claim denied - sample denial reason from stochastic weights
        weights = approval_config.stochastic_denial_weights.copy()

        # pre_existing only applies to hospital claims
        if claim_type != ClaimType.HOSPITAL:
            weights.pop("pre_existing", None)

        # Normalize weights and sample
        total = sum(weights.values())
        if total == 0:
            # Fallback if weights are empty
            return False, DenialReason.ADMINISTRATIVE

        reasons = list(weights.keys())
        probs = [w / total for w in weights.values()]
        reason_key = self.rng.choice(reasons, p=probs)

        # Map config key to DenialReason enum
        reason_map = {
            "policy_exclusions": DenialReason.POLICY_EXCLUSIONS,
            "pre_existing": DenialReason.PRE_EXISTING,
            "provider_issues": DenialReason.PROVIDER_ISSUES,
            "administrative": DenialReason.ADMINISTRATIVE,
        }
        denial_reason = reason_map.get(reason_key, DenialReason.ADMINISTRATIVE)

        return False, denial_reason

    def _get_benefit_category_id(self, service_type: str) -> int:
        """
        Get benefit category ID for an extras service type.

        Args:
            service_type: Service type name (e.g., "Dental", "Optical")

        Returns:
            Benefit category ID
        """
        return self._benefit_category_map.get(service_type, 1)  # Default to EXTRAS parent category

    def run(self) -> Generator:
        """
        Main claims process loop.

        Checks each member daily for potential claims.
        """
        logger.info(
            "claims_process_started",
            worker_id=self.worker_id,
            hospital_frequency=self.config.claims.hospital_frequency,
            high_claim_probability=self.config.claims.high_claim_probability,
            dental_frequency=self.config.claims.dental_frequency,
        )

        while True:
            # Skip initial period (need waiting periods to complete)
            if self.sim_env.now < 60:  # First 2 months
                yield self.env.timeout(1.0)
                continue

            current_date = self.sim_env.current_date

            # Process each member
            # Note: Waiting period and membership checks are handled inside _process_member_claims
            # and will generate rejected claims when appropriate
            for pm_id, member_data in list(self.policy_members.items()):
                yield from self._process_member_claims(pm_id, member_data, current_date)

            # Wait until next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _process_member_claims(
        self,
        pm_id: UUID,
        member_data: dict,
        current_date: date,
    ) -> Generator:
        """
        Process potential claims for a member on a given day.

        Members don't attempt claims when they know they'll fail (waiting period,
        suspended policy). Only stochastic denials and limits_exhausted generate
        rejected claims, matching real-world claim submission patterns.

        Args:
            pm_id: Policy member ID
            member_data: Member data dictionary
            current_date: Current simulation date
        """
        # Check if policy is suspended or lapsed - skip silently (members know not to claim)
        policy = member_data.get("policy")
        if policy and self.shared_state:
            policy_id = policy.policy_id
            policy_data = self.shared_state.active_policies.get(policy_id)
            # If policy not in active_policies or suspended/lapsed, skip silently
            if policy_data is None or policy_data.get("status") in ("Suspended", "Lapsed"):
                yield self.env.timeout(0)
                return

        age = member_data.get("age", 40)
        gender = member_data.get("gender", "Male")

        # Rate multiplier for uncovered claim attempts (members trying to claim without coverage)
        uncovered_attempt_rate = self.config.claims.uncovered_claim_attempt_rate

        # --- Extras claims ---
        extras_rate = self.propensity.get_extras_claim_rate(age) / 365
        if member_data.get("extras_coverage") is not None:
            if self.rng.random() < extras_rate:
                # Skip silently if waiting period not complete (member knows not to claim)
                if self._can_claim(pm_id, current_date, CoverageType.EXTRAS):
                    # Limits check and stochastic approval handled in _generate_extras_claim
                    self._generate_extras_claim(member_data, current_date)
        else:
            # Member has no extras coverage - may still attempt to claim (rejected)
            if self.rng.random() < extras_rate * uncovered_attempt_rate:
                self._generate_rejected_claim(
                    member_data, current_date, ClaimType.EXTRAS,
                    DenialReason.NO_COVERAGE
                )

        # --- Hospital admissions ---
        hospital_rate = self.propensity.get_hospital_admission_rate(age) / 365
        if member_data.get("hospital_coverage") is not None:
            if self.rng.random() < hospital_rate:
                # Skip silently if waiting period not complete (member knows not to claim)
                if self._can_claim(pm_id, current_date, CoverageType.HOSPITAL):
                    # Stochastic approval handled in _generate_hospital_claim
                    self._generate_hospital_claim(member_data, current_date, age, gender)
        else:
            # Member has no hospital coverage - may still attempt to claim (rejected)
            if self.rng.random() < hospital_rate * uncovered_attempt_rate:
                self._generate_rejected_claim(
                    member_data, current_date, ClaimType.HOSPITAL,
                    DenialReason.NO_COVERAGE, age=age
                )

        # --- Ambulance claims ---
        ambulance_rate = self.propensity.get_ambulance_claim_rate(age) / 365
        if member_data.get("ambulance_coverage") is not None:
            if self.rng.random() < ambulance_rate:
                # Skip silently if waiting period not complete (member knows not to claim)
                if self._can_claim(pm_id, current_date, CoverageType.AMBULANCE):
                    # Stochastic approval handled in _generate_ambulance_claim
                    self._generate_ambulance_claim(member_data, current_date)
        else:
            # Member has no ambulance coverage - may still attempt to claim (rejected)
            if self.rng.random() < ambulance_rate * uncovered_attempt_rate:
                self._generate_rejected_claim(
                    member_data, current_date, ClaimType.AMBULANCE,
                    DenialReason.NO_COVERAGE
                )

        yield self.env.timeout(0)  # Allow SimPy to process

    def _generate_extras_claim(
        self,
        member_data: dict,
        service_date: date,
    ) -> None:
        """
        Generate an extras claim with deterministic-first denial approach.

        Flow:
        1. Sample service type (to determine benefit category for limit check)
        2. Check if limits exhausted (deterministic denial)
        3. Apply stochastic approval check
        4. Generate approved claim with benefit capping if partial limit remaining
        """
        # Step 1: Sample service type FIRST (to get benefit_category_id for pre-check)
        service_type = self.propensity.sample_extras_service_type()
        benefit_category_id = self._get_benefit_category_id(service_type)

        # Step 2: Check remaining limit (deterministic denial if limit = $0)
        benefit_year = get_financial_year(service_date)
        remaining_limit = self._get_remaining_limit(
            member_data,
            benefit_category_id,
            benefit_year,
        )

        # If limit is exactly $0, deny the claim
        if remaining_limit is not None and remaining_limit <= Decimal("0"):
            self._generate_rejected_claim(
                member_data, service_date, ClaimType.EXTRAS,
                DenialReason.LIMITS_EXHAUSTED
            )
            return

        # Step 3: Stochastic approval check
        approved, denial_reason = self._should_approve_claim(ClaimType.EXTRAS)
        if not approved:
            self._generate_rejected_claim(
                member_data, service_date, ClaimType.EXTRAS, denial_reason
            )
            return

        # Step 4: Generate approved claim (pass service_type to avoid re-sampling)
        claim, claim_line, extras_claim = self.claims_gen.generate_extras_claim(
            policy=member_data.get("policy"),
            member=member_data.get("member"),
            coverage=member_data.get("extras_coverage"),
            service_date=service_date,
            service_type=service_type,  # Pass pre-sampled service type
        )

        # Step 5: Cap benefit at remaining limit if partial limit remains
        # Re-fetch remaining limit using actual claim line category (may differ)
        remaining_limit = self._get_remaining_limit(
            member_data,
            claim_line.benefit_category_id,
            benefit_year,
        )

        if remaining_limit is not None:
            original_benefit = claim.total_benefit
            # Cap benefit at remaining limit
            capped_benefit = min(original_benefit, remaining_limit)
            # Member pays the difference as gap
            additional_gap = original_benefit - capped_benefit

            if additional_gap > Decimal("0"):
                # Update claim header
                claim.total_benefit = capped_benefit
                claim.total_gap = claim.total_gap + additional_gap

                # Update claim line
                claim_line.benefit_amount = capped_benefit
                claim_line.gap_amount = claim_line.gap_amount + additional_gap

                # Update extras claim
                extras_claim.benefit_amount = capped_benefit
                extras_claim.annual_limit_impact = capped_benefit

        self.batch_writer.add("claim", claim.model_dump_db())
        self.batch_writer.add("claim_line", claim_line.model_dump())
        self.batch_writer.add("extras_claim", extras_claim.model_dump_db())

        # Record benefit usage (only track the actual benefit paid, not the charge)
        self._record_benefit_usage(
            member_data,
            claim,
            extras_claim.benefit_amount,
            claim_line.benefit_category_id,
            service_date,
        )

        self.increment_stat("extras_claims")

    def _generate_hospital_claim(
        self,
        member_data: dict,
        admission_date: date,
        age: int,
        gender: str,
    ) -> None:
        """
        Generate a hospital admission claim with stochastic approval check.

        Note: Hospital claims don't have annual limits like extras, so only
        stochastic approval is checked here. Deterministic checks (membership,
        waiting period) are done in _process_member_claims.
        """
        # Stochastic approval check
        approved, denial_reason = self._should_approve_claim(ClaimType.HOSPITAL)
        if not approved:
            self._generate_rejected_claim(
                member_data, admission_date, ClaimType.HOSPITAL,
                denial_reason, age=age
            )
            return

        # Generate approved claim
        claim, claim_lines, admission, prosthesis_claims, medical_services = self.claims_gen.generate_hospital_claim(
            policy=member_data.get("policy"),
            member=member_data.get("member"),
            coverage=member_data.get("hospital_coverage"),
            admission_date=admission_date,
            age=age,
            gender=gender,
        )

        self.batch_writer.add("claim", claim.model_dump_db())
        for cl in claim_lines:
            self.batch_writer.add("claim_line", cl.model_dump())
        self.batch_writer.add("hospital_admission", admission.model_dump_db())

        # Write prosthesis claims if any
        for prosthesis in prosthesis_claims:
            self.batch_writer.add("prosthesis_claim", prosthesis.model_dump())

        # Write medical services (MBS items billed by doctors)
        for medical_service in medical_services:
            self.batch_writer.add("medical_service", medical_service.model_dump())

        self.increment_stat("hospital_claims")
        if prosthesis_claims:
            self.increment_stat("prosthesis_claims", len(prosthesis_claims))
        if medical_services:
            self.increment_stat("medical_services", len(medical_services))

    def _generate_ambulance_claim(
        self,
        member_data: dict,
        incident_date: date,
    ) -> None:
        """
        Generate an ambulance claim with stochastic approval check.

        Note: Ambulance claims don't have annual limits, so only stochastic
        approval is checked here. Deterministic checks (membership, waiting
        period) are done in _process_member_claims.
        """
        # Stochastic approval check
        approved, denial_reason = self._should_approve_claim(ClaimType.AMBULANCE)
        if not approved:
            self._generate_rejected_claim(
                member_data, incident_date, ClaimType.AMBULANCE, denial_reason
            )
            return

        # Generate approved claim
        claim, ambulance = self.claims_gen.generate_ambulance_claim(
            policy=member_data.get("policy"),
            member=member_data.get("member"),
            coverage=member_data.get("ambulance_coverage"),
            incident_date=incident_date,
        )

        self.batch_writer.add("claim", claim.model_dump_db())
        self.batch_writer.add("ambulance_claim", ambulance.model_dump())

        self.increment_stat("ambulance_claims")

    def _generate_rejected_claim(
        self,
        member_data: dict,
        service_date: date,
        claim_type: ClaimType,
        denial_reason: DenialReason,
        **kwargs,
    ) -> None:
        """
        Generate a rejected claim with a specific denial reason.

        Args:
            member_data: Member data dictionary
            service_date: Date of attempted service
            claim_type: Type of claim (EXTRAS, HOSPITAL, AMBULANCE)
            denial_reason: Reason for denial (DenialReason enum)
            **kwargs: Additional args (e.g., age for hospital claims)
        """
        claim = self.claims_gen.generate_rejected_claim(
            policy=member_data.get("policy"),
            member=member_data.get("member"),
            claim_type=claim_type,
            service_date=service_date,
            denial_reason=denial_reason,
            **kwargs,
        )

        self.batch_writer.add("claim", claim.model_dump_db())

        self.increment_stat("rejected_claims")
        logger.debug(
            "rejected_claim_generated",
            claim_type=claim_type.value,
            denial_reason=denial_reason.value,
            member_id=str(claim.member_id),
        )

    def _can_claim(
        self,
        pm_id: UUID,
        current_date: date,
        coverage_type: CoverageType,
    ) -> bool:
        """
        Check if member can claim based on waiting periods.

        Args:
            pm_id: Policy member ID
            current_date: Current date
            coverage_type: Type of coverage

        Returns:
            True if member can claim
        """
        waiting_periods = self.waiting_periods.get(pm_id, [])

        for wp in waiting_periods:
            if wp.get("coverage_type") != coverage_type.value:
                continue

            # Check if waiting period is complete
            end_date = wp.get("end_date")
            if end_date and current_date < end_date:
                return False

        return True

    def _get_remaining_limit(
        self,
        member_data: dict,
        benefit_category_id: int,
        benefit_year: str,
    ) -> Decimal | None:
        """
        Get remaining benefit limit for a member/category/financial year.

        Args:
            member_data: Member data dictionary
            benefit_category_id: Benefit category ID
            benefit_year: Financial year for the limit period (e.g., "2024-2025")

        Returns:
            Remaining limit amount, or None if no limit applies
        """
        policy = member_data.get("policy")
        member = member_data.get("member")
        if not policy or not member:
            return None

        product_id = policy.product_id
        cat_id = benefit_category_id or 1

        # Look up annual limit from reference data
        limit_key = (product_id, cat_id)
        limit_info = self.benefit_limits.get(limit_key)
        if not limit_info:
            return None  # No limit defined for this product/category

        annual_limit = limit_info.get("per_person_limit") or limit_info.get("limit_amount")
        if annual_limit is None:
            return None

        annual_limit = Decimal(str(annual_limit))

        # Get current cumulative usage
        usage_key = (member.member_id, cat_id, benefit_year)
        current_usage = self.cumulative_usage.get(usage_key, Decimal("0"))

        # Calculate remaining
        remaining = annual_limit - current_usage
        return max(Decimal("0"), remaining)

    def _record_benefit_usage(
        self,
        member_data: dict,
        claim,
        benefit_amount,
        benefit_category_id,
        usage_date: date,
    ) -> None:
        """Record benefit usage for limit tracking."""
        # Get product_id from the policy to look up limits
        policy = member_data.get("policy")
        product_id = policy.product_id if policy else None
        cat_id = benefit_category_id or 1
        # Use Australian financial year (July-June) for benefit limits
        benefit_year = get_financial_year(usage_date)

        # Look up annual limit from reference data
        annual_limit = None
        limit_type = "Dollar"

        if product_id:
            limit_key = (product_id, cat_id)
            limit_info = self.benefit_limits.get(limit_key)
            if limit_info:
                annual_limit = limit_info.get("per_person_limit") or limit_info.get("limit_amount")
                limit_type = limit_info.get("limit_type", "Dollar")

        # Track cumulative usage for this member/category/year
        usage_key = (claim.member_id, cat_id, benefit_year)
        prev_usage = self.cumulative_usage.get(usage_key, Decimal("0"))
        new_total = prev_usage + Decimal(str(benefit_amount))
        self.cumulative_usage[usage_key] = new_total

        # Calculate remaining limit
        remaining_limit = None
        if annual_limit is not None:
            remaining_limit = max(Decimal("0"), Decimal(str(annual_limit)) - new_total)

        usage = BenefitUsageCreate(
            benefit_usage_id=self.id_generator.generate_uuid(),
            policy_id=claim.policy_id,
            member_id=claim.member_id,
            claim_id=claim.claim_id,
            benefit_category_id=cat_id,
            benefit_year=benefit_year,
            usage_date=usage_date,
            usage_amount=benefit_amount,
            usage_count=1,
            annual_limit=Decimal(str(annual_limit)) if annual_limit else None,
            remaining_limit=remaining_limit,
            limit_type=limit_type,
            created_at=self.sim_env.current_datetime,
            created_by="SIMULATION",
        )

        self.batch_writer.add("benefit_usage", usage.model_dump())

    def add_member(
        self,
        pm_id: UUID,
        member_data: dict,
        waiting_periods: list[dict],
    ) -> None:
        """
        Add a member to track for claims.

        Args:
            pm_id: Policy member ID
            member_data: Member data dictionary
            waiting_periods: Member's waiting periods
        """
        self.policy_members[pm_id] = member_data
        self.waiting_periods[pm_id] = waiting_periods

    def remove_member(self, pm_id: UUID) -> None:
        """
        Remove a member from tracking.

        Args:
            pm_id: Policy member ID
        """
        self.policy_members.pop(pm_id, None)
        self.waiting_periods.pop(pm_id, None)

    def _log_progress(self) -> None:
        """Log claims progress."""
        stats = self.get_stats()
        logger.info(
            "claims_progress",
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            active_members=len(self.policy_members),
            extras_claims=stats.get("extras_claims", 0),
            hospital_claims=stats.get("hospital_claims", 0),
            ambulance_claims=stats.get("ambulance_claims", 0),
            prosthesis_claims=stats.get("prosthesis_claims", 0),
            rejected_claims=stats.get("rejected_claims", 0),
        )
