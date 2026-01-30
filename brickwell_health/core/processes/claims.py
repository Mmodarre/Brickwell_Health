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
from brickwell_health.domain.enums import CoverageType, ClaimType, ClaimStatus, DenialReason
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

        # Reference to pending claims for lifecycle transitions
        # Claims are INSERT'd as SUBMITTED, then UPDATE'd through lifecycle
        self.pending_claims = shared_state.pending_claims if shared_state else {}

        # Initialize generators with config for APRA-based claiming patterns
        self.claims_gen = ClaimsGenerator(
            self.rng, self.reference, self.id_generator, sim_env=self.sim_env, config=self.config.claims
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

        # Clinical category to waiting period type mapping
        # Used to check if a hospital claim is blocked by specialized waiting periods
        self._clinical_category_wp_map = self.reference.get_clinical_category_wp_mapping()

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

            # Process pending claim state transitions FIRST
            # This handles SUBMITTED -> ASSESSED -> APPROVED/REJECTED -> PAID
            self._process_claim_transitions(current_date)

            # Process each member for new claims
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
                # Sample clinical category FIRST (before WP check)
                # This determines which specialized waiting period applies
                clinical_category_id = self.propensity.sample_clinical_category(age, gender)

                # Check waiting period with clinical category
                # - General WP blocks all claims
                # - Obstetric WP blocks pregnancy-related categories
                # - Psychiatric WP blocks psychiatric category
                # - Pre-existing WP probabilistically blocks general claims
                if self._can_claim_hospital(pm_id, current_date, clinical_category_id):
                    # Stochastic approval handled in _generate_hospital_claim
                    self._generate_hospital_claim(
                        member_data, current_date, age, gender,
                        clinical_category_id=clinical_category_id,
                    )
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
        2. Check if limits exhausted (deterministic denial - immediate REJECTED)
        3. Apply stochastic approval check (determines outcome, doesn't reject yet)
        4. Generate claim as SUBMITTED with benefit capping
        5. Schedule lifecycle transitions (approved or stochastic rejection)
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

        # If limit is exactly $0, deny the claim IMMEDIATELY (deterministic)
        if remaining_limit is not None and remaining_limit <= Decimal("0"):
            self._generate_rejected_claim(
                member_data, service_date, ClaimType.EXTRAS,
                DenialReason.LIMITS_EXHAUSTED
            )
            return

        # Step 3: Stochastic approval check - determines outcome but doesn't reject yet
        approved, denial_reason = self._should_approve_claim(ClaimType.EXTRAS)

        # Step 4: Generate claim as SUBMITTED (both approved and stochastic rejected)
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

        # Write claim as SUBMITTED
        self.batch_writer.add("claim", claim.model_dump_db())
        self.batch_writer.add("claim_line", claim_line.model_dump())
        self.batch_writer.add("extras_claim", extras_claim.model_dump_db())

        # Step 6: Schedule lifecycle transitions (benefit usage recorded when PAID)
        self._schedule_claim_transitions(
            claim=claim,
            claim_line_ids=[claim_line.claim_line_id],
            member_data=member_data,
            lodgement_date=service_date,
            approved=approved,
            denial_reason=denial_reason if not approved else None,
            benefit_category_id=claim_line.benefit_category_id,
            benefit_amount=extras_claim.benefit_amount,
        )

        self.increment_stat("extras_claims")

    def _generate_hospital_claim(
        self,
        member_data: dict,
        admission_date: date,
        age: int,
        gender: str,
        clinical_category_id: int | None = None,
    ) -> None:
        """
        Generate a hospital admission claim with stochastic approval check.

        Note: Hospital claims don't have annual limits like extras, so only
        stochastic approval is checked here. Deterministic checks (membership,
        waiting period) are done in _process_member_claims.

        Claim is created as SUBMITTED and transitions through lifecycle.

        Args:
            member_data: Member data dictionary
            admission_date: Date of admission
            age: Patient age
            gender: Patient gender
            clinical_category_id: Optional pre-sampled clinical category ID
                (used when category was sampled for waiting period check)
        """
        # Stochastic approval check - determines outcome but doesn't reject yet
        approved, denial_reason = self._should_approve_claim(ClaimType.HOSPITAL)

        # Generate claim as SUBMITTED (both approved and stochastic rejected)
        # Pass clinical_category_id if provided (avoids re-sampling)
        claim, claim_lines, admission, prosthesis_claims, medical_services = self.claims_gen.generate_hospital_claim(
            policy=member_data.get("policy"),
            member=member_data.get("member"),
            coverage=member_data.get("hospital_coverage"),
            admission_date=admission_date,
            age=age,
            gender=gender,
            clinical_category_id=clinical_category_id,
        )

        # Write claim as SUBMITTED
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

        # Get discharge date for lodgement (hospital claims lodge on discharge)
        lodgement_date = admission.discharge_date or admission_date

        # Schedule lifecycle transitions
        self._schedule_claim_transitions(
            claim=claim,
            claim_line_ids=[cl.claim_line_id for cl in claim_lines],
            member_data=member_data,
            lodgement_date=lodgement_date,
            approved=approved,
            denial_reason=denial_reason if not approved else None,
            benefit_category_id=None,  # Hospital claims don't track per-category usage
            benefit_amount=None,
        )

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

        Claim is created as SUBMITTED and transitions through lifecycle.
        """
        # Stochastic approval check - determines outcome but doesn't reject yet
        approved, denial_reason = self._should_approve_claim(ClaimType.AMBULANCE)

        # Generate claim as SUBMITTED (both approved and stochastic rejected)
        claim, ambulance = self.claims_gen.generate_ambulance_claim(
            policy=member_data.get("policy"),
            member=member_data.get("member"),
            coverage=member_data.get("ambulance_coverage"),
            incident_date=incident_date,
        )

        # Write claim as SUBMITTED
        self.batch_writer.add("claim", claim.model_dump_db())
        self.batch_writer.add("ambulance_claim", ambulance.model_dump())

        # Ambulance claims don't have claim_lines in the traditional sense
        # We don't track them separately, but schedule transitions for the main claim
        self._schedule_claim_transitions(
            claim=claim,
            claim_line_ids=[],  # Ambulance claims have no separate claim_lines
            member_data=member_data,
            lodgement_date=incident_date,
            approved=approved,
            denial_reason=denial_reason if not approved else None,
            benefit_category_id=None,  # Ambulance claims don't track per-category usage
            benefit_amount=None,
        )

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

    def _schedule_claim_transitions(
        self,
        claim,
        claim_line_ids: list[UUID],
        member_data: dict,
        lodgement_date: date,
        approved: bool,
        denial_reason: DenialReason | None = None,
        benefit_category_id: int | None = None,
        benefit_amount: Decimal | None = None,
    ) -> None:
        """
        Schedule future state transitions for a claim.

        Claims are INSERT'd as SUBMITTED and transition through:
        - SUBMITTED -> ASSESSED (after assessment_days)
        - ASSESSED -> APPROVED (if approved) or REJECTED (if stochastic denial)
        - APPROVED -> PAID (after payment_days)

        Processing times use a bimodal distribution:
        - Auto-adjudicated claims: Lognormal with median ~0.5 days (same-day/next-day)
        - Manual review claims: Lognormal with median ~5 days (3-14 day range)

        Args:
            claim: The claim object
            claim_line_ids: List of claim line UUIDs for this claim
            member_data: Member data dict
            lodgement_date: Date claim was lodged
            approved: True if claim will be approved, False for stochastic rejection
            denial_reason: Denial reason for stochastic rejections
            benefit_category_id: Benefit category for usage tracking (extras)
            benefit_amount: Benefit amount for usage tracking (extras)
        """
        delays = self.config.claims.processing_delays
        auto_adj = delays.auto_adjudication

        # Determine if claim is auto-adjudicated based on claim type
        claim_type = claim.claim_type
        is_auto_adjudicated = self._is_auto_adjudicated(claim_type, auto_adj)

        # Sample assessment delay from appropriate distribution
        assessment_delay = self._sample_assessment_delay(is_auto_adjudicated, auto_adj)

        # Approval and payment delays (uniform distribution)
        approval_delay = self.rng.integers(
            delays.approval_days[0], delays.approval_days[1] + 1
        )
        payment_delay = self.rng.integers(
            delays.payment_days[0], delays.payment_days[1] + 1
        )

        assessment_date = lodgement_date + timedelta(days=int(assessment_delay))
        approval_date = assessment_date + timedelta(days=int(approval_delay))
        payment_date = approval_date + timedelta(days=int(payment_delay))

        self.pending_claims[claim.claim_id] = {
            "status": "SUBMITTED",
            "assessment_date": assessment_date,
            "approval_date": approval_date,
            "payment_date": payment_date,
            "approved": approved,
            "denial_reason": denial_reason,
            "claim_line_ids": claim_line_ids,
            "benefit_category_id": benefit_category_id,
            "benefit_amount": benefit_amount,
            "member_data": member_data,
            "policy_id": claim.policy_id,  # Store for benefit usage recording
            "is_auto_adjudicated": is_auto_adjudicated,  # Track for analytics
        }

    def _is_auto_adjudicated(self, claim_type: str, auto_adj) -> bool:
        """
        Determine if a claim is auto-adjudicated based on claim type.

        Industry benchmarks (2024-2025):
        - Extras/Dental: 85-90% auto-adjudicated
        - Hospital: 60-70% auto-adjudicated
        - Ambulance: 90-95% auto-adjudicated

        Args:
            claim_type: Type of claim (Hospital, Extras, Ambulance)
            auto_adj: AutoAdjudicationConfig

        Returns:
            True if claim should be auto-adjudicated
        """
        if claim_type == "Extras":
            auto_rate = auto_adj.extras_auto_rate
        elif claim_type == "Hospital":
            auto_rate = auto_adj.hospital_auto_rate
        elif claim_type == "Ambulance":
            auto_rate = auto_adj.ambulance_auto_rate
        else:
            auto_rate = 0.80  # Default fallback

        return self.rng.random() < auto_rate

    def _sample_assessment_delay(self, is_auto_adjudicated: bool, auto_adj) -> int:
        """
        Sample assessment delay from lognormal distribution.

        Uses bimodal distribution:
        - Auto-adjudicated: Lognormal(mu=-0.7, sigma=0.5) → median ~0.5 days
        - Manual review: Lognormal(mu=1.6, sigma=0.6) → median ~5 days

        Args:
            is_auto_adjudicated: Whether claim is auto-adjudicated
            auto_adj: AutoAdjudicationConfig

        Returns:
            Number of days for assessment (integer, minimum 0)
        """
        import numpy as np

        if is_auto_adjudicated:
            # Auto-adjudicated: fast processing (median ~0.5 days)
            mu = auto_adj.auto_assessment_mu
            sigma = auto_adj.auto_assessment_sigma
            max_days = auto_adj.max_auto_days
        else:
            # Manual review: slower processing (median ~5 days)
            mu = auto_adj.manual_assessment_mu
            sigma = auto_adj.manual_assessment_sigma
            max_days = auto_adj.max_manual_days

        # Sample from lognormal distribution
        delay = self.rng.lognormal(mean=mu, sigma=sigma)

        # Cap at maximum and convert to integer (round to nearest day)
        delay = min(delay, max_days)
        return max(0, int(round(delay)))

    def _process_claim_transitions(self, current_date: date) -> None:
        """
        Process scheduled claim state transitions.

        Iterates through pending claims and performs state transitions
        when the scheduled date is reached.
        """
        for claim_id, data in list(self.pending_claims.items()):
            status = data["status"]

            # SUBMITTED -> ASSESSED
            if status == "SUBMITTED" and current_date >= data["assessment_date"]:
                # Flush if claim is still in buffer to ensure INSERT is committed for CDC
                self.batch_writer.flush_for_cdc("claim", "claim_id", claim_id)

                self._update_claim_status(
                    claim_id,
                    ClaimStatus.ASSESSED,
                    assessment_date=data["assessment_date"].isoformat(),
                )
                data["status"] = "ASSESSED"
                continue

            # ASSESSED -> APPROVED or REJECTED
            if status == "ASSESSED" and current_date >= data["approval_date"]:
                if data["approved"]:
                    self._update_claim_status(claim_id, ClaimStatus.APPROVED)
                    data["status"] = "APPROVED"
                else:
                    # Stochastic rejection at approval stage
                    self._update_claim_status(
                        claim_id,
                        ClaimStatus.REJECTED,
                        rejection_reason_id=self._get_rejection_reason_id(data["denial_reason"]),
                        rejection_notes=data["denial_reason"].value if data["denial_reason"] else None,
                    )
                    # Update claim lines to Rejected
                    for line_id in data["claim_line_ids"]:
                        # Flush if claim_line is still in buffer for CDC
                        self.batch_writer.flush_for_cdc("claim_line", "claim_line_id", line_id)
                        self._update_claim_line_status(line_id, "Rejected")
                    del self.pending_claims[claim_id]
                    self.increment_stat("stochastic_rejections")
                continue

            # APPROVED -> PAID
            if status == "APPROVED" and current_date >= data["payment_date"]:
                self._update_claim_status(
                    claim_id,
                    ClaimStatus.PAID,
                    payment_date=data["payment_date"].isoformat(),
                )
                # Update claim lines to Paid
                for line_id in data["claim_line_ids"]:
                    # Flush if claim_line is still in buffer for CDC
                    self.batch_writer.flush_for_cdc("claim_line", "claim_line_id", line_id)
                    self._update_claim_line_status(line_id, "Paid")

                # Record benefit usage NOW (when actually paid)
                if data["benefit_amount"] and data["benefit_category_id"]:
                    # Create a simple object with required attributes for _record_benefit_usage
                    claim_obj = type("ClaimRef", (object,), {
                        "claim_id": claim_id,
                        "member_id": data["member_data"]["member"].member_id,
                        "policy_id": data["policy_id"],
                    })()
                    self._record_benefit_usage(
                        data["member_data"],
                        claim_obj,
                        data["benefit_amount"],
                        data["benefit_category_id"],
                        data["payment_date"],
                    )

                del self.pending_claims[claim_id]

    def _update_claim_status(
        self,
        claim_id: UUID,
        new_status: ClaimStatus,
        **field_updates,
    ) -> None:
        """
        Update claim status and optional fields in database.

        Args:
            claim_id: The claim UUID
            new_status: New ClaimStatus value
            **field_updates: Additional fields to update (e.g., assessment_date, payment_date)
        """
        updates = {
            "claim_status": new_status.value,
            "modified_at": self.sim_env.current_datetime.isoformat(),
            "modified_by": "SIMULATION",
        }
        updates.update(field_updates)
        self.batch_writer.update_record("claim", "claim_id", claim_id, updates)

    def _update_claim_line_status(self, claim_line_id: UUID, new_status: str) -> None:
        """
        Update claim line status in database.

        Args:
            claim_line_id: The claim line UUID
            new_status: New line status ("Pending", "Paid", "Rejected")
        """
        self.batch_writer.update_record(
            "claim_line",
            "claim_line_id",
            claim_line_id,
            {
                "line_status": new_status,
                "modified_at": self.sim_env.current_datetime.isoformat(),
                "modified_by": "SIMULATION",
            },
        )

    def _get_rejection_reason_id(self, denial_reason: DenialReason | None) -> int | None:
        """
        Get rejection reason ID from denial reason enum.

        Args:
            denial_reason: DenialReason enum value or None

        Returns:
            Integer rejection reason ID or None
        """
        if denial_reason is None:
            return None
        return self.claims_gen.DENIAL_REASON_IDS.get(denial_reason, 1)

    def _can_claim(
        self,
        pm_id: UUID,
        current_date: date,
        coverage_type: CoverageType,
    ) -> bool:
        """
        Check if member can claim based on waiting periods (for Extras/Ambulance).

        For Hospital claims, use _can_claim_hospital instead which considers
        clinical category-specific waiting periods.

        Args:
            pm_id: Policy member ID
            current_date: Current date
            coverage_type: Type of coverage (EXTRAS or AMBULANCE)

        Returns:
            True if member can claim
        """
        waiting_periods = self.waiting_periods.get(pm_id, [])

        for wp in waiting_periods:
            if wp.get("coverage_type") != coverage_type.value:
                continue

            # Only check General waiting period for Extras/Ambulance
            if wp.get("waiting_period_type") != "General":
                continue

            # Check if waiting period is complete
            end_date = wp.get("end_date")
            if end_date and current_date < end_date:
                return False

        return True

    def _can_claim_hospital(
        self,
        pm_id: UUID,
        current_date: date,
        clinical_category_id: int,
    ) -> bool:
        """
        Check if member can make a hospital claim based on waiting periods.

        Waiting period rules for hospital claims:
        - General WP (2mo): Blocks ALL hospital claims
        - Obstetric WP (12mo): Blocks pregnancy-related categories (13, 30, 31)
        - Psychiatric WP (2mo): Blocks psychiatric category (36)
        - Pre-existing WP (12mo): Probabilistically blocks ~15% of General claims
          (representing claims for pre-existing conditions)

        Args:
            pm_id: Policy member ID
            current_date: Current date
            clinical_category_id: The clinical category for this claim

        Returns:
            True if member can claim
        """
        waiting_periods = self.waiting_periods.get(pm_id, [])

        # Get the WP type this claim falls under based on clinical category
        claim_wp_type = self._clinical_category_wp_map.get(clinical_category_id, "General")

        for wp in waiting_periods:
            if wp.get("coverage_type") != CoverageType.HOSPITAL.value:
                continue

            end_date = wp.get("end_date")
            if not end_date or current_date >= end_date:
                continue  # This waiting period has ended

            wp_type = wp.get("waiting_period_type")

            # General WP blocks ALL hospital claims
            if wp_type == "General":
                return False

            # Specific WP blocks matching claim types (Obstetric or Psychiatric)
            if wp_type == claim_wp_type:
                return False

            # Pre-existing WP: probabilistically blocks General category claims
            # ~15% of non-specialty hospital claims are for pre-existing conditions
            if wp_type == "Pre-existing" and claim_wp_type == "General":
                if self.rng.random() < 0.15:
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
