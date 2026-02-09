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
from brickwell_health.domain.claims import ClaimAssessmentCreate, ClaimCreate, ClaimLineCreate
from brickwell_health.domain.enums import (
    CoverageType, ClaimType, ClaimStatus, ClaimChannel, DenialReason, FraudType,
)
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

        # Initialize fraud generator if fraud is enabled
        self.fraud_gen = None
        fraud_config = getattr(self.config, "fraud", None)
        if fraud_config and fraud_config.enabled:
            from brickwell_health.generators.fraud_generator import FraudGenerator
            self.fraud_gen = FraudGenerator(
                rng=self.rng,
                reference=self.reference,
                id_generator=self.id_generator,
                sim_env=self.sim_env,
                fraud_config=fraud_config,
            )

        # Build benefit limit lookup: (product_id, benefit_category_id) -> limit info
        self.benefit_limits = self.reference.build_benefit_limit_lookup()

        # Track cumulative usage: (member_id, benefit_category_id, financial_year) -> total_used
        # Financial year is a string like "2024-2025"
        self.cumulative_usage: dict[tuple[UUID, int, str], Decimal] = {}

        # Build benefit category ID mapping from database reference table
        self._benefit_category_map = self._build_benefit_category_map()

        # Clinical category to waiting period type mapping
        # Used to check if a hospital claim is blocked by specialized waiting periods
        self._clinical_category_wp_map = self.reference.get_clinical_category_wp_mapping()

    def _build_benefit_category_map(self) -> dict[str, int]:
        """
        Build benefit category ID mapping from database reference table.

        Maps service type names (e.g., "Dental", "Optical") to benefit_category_id values.

        Returns:
            Dict mapping service type name to benefit_category_id
        """
        benefit_categories = self.reference.get_benefit_categories()

        # Build mapping from category_name to benefit_category_id
        # Also map from common service type names to their corresponding categories
        category_map = {}

        for cat in benefit_categories:
            cat_id = cat.get("benefit_category_id")
            cat_name = cat.get("category_name", "")
            cat_code = cat.get("category_code", "")

            # Map by category_name (e.g., "Dental" -> 3)
            if cat_name:
                category_map[cat_name] = cat_id

            # Map by common service type variations
            # Match category_code to service type names
            code_to_service = {
                "DENTAL": "Dental",
                "OPTICAL": "Optical",
                "PHYSIO": "Physiotherapy",
                "CHIRO": "Chiropractic",
                "PODIATRY": "Podiatry",
                "PSYCHOLOGY": "Psychology",
                "MASSAGE": "Massage",
                "ACUPUNCTURE": "Acupuncture",
                "NATURAL_THERAPIES": "Natural Therapies",
                "OSTEOPATHY": "Osteopathy",
                "SPEECH_PATHOLOGY": "Speech Pathology",
                "DIETETICS": "Dietetics",
                "OCC_THERAPY": "Occupational Therapy",
            }

            if cat_code in code_to_service:
                service_name = code_to_service[cat_code]
                if service_name not in category_map:
                    category_map[service_name] = cat_id

        return category_map

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

        # Step 4a: Fraud injection (before benefit capping)
        fraud_type_applied = None
        if self.fraud_gen and self.shared_state:
            if self.fraud_gen.should_apply_fraud(claim.member_id, self.shared_state):
                fraud_type_applied = self.fraud_gen.select_fraud_type(ClaimType.EXTRAS)

                if fraud_type_applied == FraudType.UNBUNDLING:
                    n_frags = self._write_unbundled_claims(
                        claim, claim_line, member_data, service_date,
                        approved, denial_reason,
                        extras_claim=extras_claim,
                    )
                    self.increment_stat("extras_claims")
                    self.increment_stat("fraud_claims", n_frags)
                    return

                if fraud_type_applied not in (
                    FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
                ):
                    # Inline fraud: modify claim in place
                    fraud_fields = self._get_inline_fraud_fields(
                        claim, fraud_type_applied, member_data,
                    )
                    for key, value in fraud_fields.items():
                        object.__setattr__(claim, key, value)
                    # Keep charge/benefit consistent across related records
                    if "total_charge" in fraud_fields:
                        object.__setattr__(
                            claim_line, "charge_amount", fraud_fields["total_charge"],
                        )
                        object.__setattr__(
                            extras_claim, "charge_amount", fraud_fields["total_charge"],
                        )
                    if fraud_fields.get("total_benefit") is not None:
                        object.__setattr__(
                            claim_line, "benefit_amount", fraud_fields["total_benefit"],
                        )
                        object.__setattr__(
                            extras_claim, "benefit_amount", fraud_fields["total_benefit"],
                        )

        # Step 5: Calculate benefit capping (for audit trail - applied during ASSESSED)
        # Re-fetch remaining limit using actual claim line category (may differ)
        remaining_limit = self._get_remaining_limit(
            member_data,
            claim_line.benefit_category_id,
            benefit_year,
        )

        # Store original amounts for SUBMITTED state
        original_benefit = claim.total_benefit
        original_gap = claim.total_gap

        # Pre-calculate capping info (applied during ASSESSED transition for audit trail)
        capped_benefit: Decimal | None = None
        additional_gap = Decimal("0")
        if remaining_limit is not None:
            capped_benefit = min(original_benefit, remaining_limit)
            additional_gap = original_benefit - capped_benefit

        # Write claim as SUBMITTED with ORIGINAL uncapped amounts (audit trail)
        self.batch_writer.add("claim", claim.model_dump_db())
        self.batch_writer.add("claim_line", claim_line.model_dump())
        self.batch_writer.add("extras_claim", extras_claim.model_dump_db())

        # Step 6: Schedule lifecycle transitions (benefit usage recorded when PAID)
        # Store capping info for application during ASSESSED transition
        self._schedule_claim_transitions(
            claim=claim,
            claim_line_ids=[claim_line.claim_line_id],
            member_data=member_data,
            lodgement_date=service_date,
            approved=approved,
            denial_reason=denial_reason if not approved else None,
            benefit_category_id=claim_line.benefit_category_id,
            benefit_amount=capped_benefit if capped_benefit is not None else original_benefit,
            # Additional capping info for ASSESSED transition (audit trail)
            capped_benefit=capped_benefit,
            additional_gap=additional_gap,
            original_benefit=original_benefit,
            original_gap=original_gap,
            extras_claim_id=extras_claim.extras_claim_id,
        )

        self.increment_stat("extras_claims")

        # Fraud: generate duplicate claim or track inline fraud stats
        if fraud_type_applied:
            if fraud_type_applied in (
                FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
            ):
                self._generate_duplicate_claim(
                    claim, member_data, fraud_type_applied, service_date,
                )
            self.increment_stat("fraud_claims")

        # Add legitimate claims to duplication pool for future duplicate sources
        if self.fraud_gen and self.shared_state:
            if not fraud_type_applied or fraud_type_applied in (
                FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
            ):
                self._add_to_duplication_pool(claim, member_data)

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

        # Fraud injection (before writing)
        fraud_type_applied = None
        if self.fraud_gen and self.shared_state:
            if self.fraud_gen.should_apply_fraud(claim.member_id, self.shared_state):
                fraud_type_applied = self.fraud_gen.select_fraud_type(ClaimType.HOSPITAL)

                if fraud_type_applied == FraudType.UNBUNDLING:
                    first_line = claim_lines[0] if claim_lines else None
                    hospital_records = (
                        [("hospital_admission", admission.model_dump_db())]
                        + [("prosthesis_claim", p.model_dump()) for p in prosthesis_claims]
                        + [("medical_service", m.model_dump()) for m in medical_services]
                    )
                    n_frags = self._write_unbundled_claims(
                        claim, first_line, member_data, admission_date,
                        approved, denial_reason,
                        hospital_records=hospital_records,
                    )
                    self.increment_stat("hospital_claims")
                    self.increment_stat("fraud_claims", n_frags)
                    return

                if fraud_type_applied not in (
                    FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
                ):
                    # Inline fraud: modify claim header
                    fraud_fields = self._get_inline_fraud_fields(
                        claim, fraud_type_applied, member_data,
                    )
                    for key, value in fraud_fields.items():
                        object.__setattr__(claim, key, value)

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

        # Fraud: generate duplicate or track inline fraud stats
        if fraud_type_applied:
            if fraud_type_applied in (
                FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
            ):
                self._generate_duplicate_claim(
                    claim, member_data, fraud_type_applied, admission_date,
                )
            self.increment_stat("fraud_claims")

        # Add legitimate claims to duplication pool
        if self.fraud_gen and self.shared_state:
            if not fraud_type_applied or fraud_type_applied in (
                FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
            ):
                self._add_to_duplication_pool(claim, member_data)

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

        # Fraud injection (before writing)
        fraud_type_applied = None
        if self.fraud_gen and self.shared_state:
            if self.fraud_gen.should_apply_fraud(claim.member_id, self.shared_state):
                fraud_type_applied = self.fraud_gen.select_fraud_type(ClaimType.AMBULANCE)

                if fraud_type_applied not in (
                    FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
                ):
                    # Inline fraud: modify claim header
                    fraud_fields = self._get_inline_fraud_fields(
                        claim, fraud_type_applied, member_data,
                    )
                    for key, value in fraud_fields.items():
                        object.__setattr__(claim, key, value)

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

        # Fraud: generate duplicate or track inline fraud stats
        if fraud_type_applied:
            if fraud_type_applied in (
                FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
            ):
                self._generate_duplicate_claim(
                    claim, member_data, fraud_type_applied, incident_date,
                )
            self.increment_stat("fraud_claims")

        # Add legitimate claims to duplication pool
        if self.fraud_gen and self.shared_state:
            if not fraud_type_applied or fraud_type_applied in (
                FraudType.EXACT_DUPLICATE, FraudType.NEAR_DUPLICATE,
            ):
                self._add_to_duplication_pool(claim, member_data)

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

        Claims go through lifecycle transitions: SUBMITTED -> ASSESSED -> REJECTED

        Args:
            member_data: Member data dictionary
            service_date: Date of attempted service
            claim_type: Type of claim (EXTRAS, HOSPITAL, AMBULANCE)
            denial_reason: Reason for denial (DenialReason enum)
            **kwargs: Additional args (e.g., age for hospital claims)
        """
        claim, claim_line = self.claims_gen.generate_rejected_claim(
            policy=member_data.get("policy"),
            member=member_data.get("member"),
            claim_type=claim_type,
            service_date=service_date,
            denial_reason=denial_reason,
            **kwargs,
        )

        # Write claim and claim line as SUBMITTED
        self.batch_writer.add("claim", claim.model_dump_db())
        self.batch_writer.add("claim_line", claim_line.model_dump())

        # Schedule lifecycle transitions: SUBMITTED -> ASSESSED -> REJECTED
        self._schedule_claim_transitions(
            claim=claim,
            claim_line_ids=[claim_line.claim_line_id],
            member_data=member_data,
            lodgement_date=service_date,
            approved=False,  # Will transition to REJECTED
            denial_reason=denial_reason,
            benefit_category_id=None,  # No benefit tracking for rejected claims
            benefit_amount=None,
        )

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
        # Benefit capping info for audit trail (applied during ASSESSED transition)
        capped_benefit: Decimal | None = None,
        additional_gap: Decimal | None = None,
        original_benefit: Decimal | None = None,
        original_gap: Decimal | None = None,
        extras_claim_id: UUID | None = None,
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
            capped_benefit: Pre-calculated capped benefit for audit trail (extras)
            additional_gap: Gap adjustment due to capping (extras)
            original_benefit: Original uncapped benefit (extras)
            original_gap: Original gap before capping (extras)
            extras_claim_id: Extras claim ID for capping updates (extras)
        """
        delays = self.config.claims.processing_delays
        auto_adj = delays.auto_adjudication

        # Determine if claim is auto-adjudicated based on claim type and amount
        claim_type = claim.claim_type
        is_auto_adjudicated = self._is_auto_adjudicated(claim_type, claim.total_charge, auto_adj)

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
            "claim_type": claim.claim_type,  # Needed for assessment record assessor
            "total_charge": claim.total_charge,  # For assessment audit trail
            # Benefit capping info for audit trail (applied during ASSESSED)
            "capped_benefit": capped_benefit,
            "additional_gap": additional_gap,
            "original_benefit": original_benefit,
            "original_gap": original_gap,
            "extras_claim_id": extras_claim_id,
            # Claim-level totals for churn model write-back
            "claim_total_benefit": claim.total_benefit,
            "claim_total_gap": claim.total_gap,
        }

        # Emit CRM event for claim submission
        self._emit_crm_event(
            "claim_submitted",
            claim.claim_id,
            {
                "policy_id": claim.policy_id,
                "member_id": claim.member_id,
                "charge_amount": float(claim.total_charge) if claim.total_charge else 0,
            },
        )

    def _is_auto_adjudicated(self, claim_type: str, total_charge: Decimal, auto_adj) -> bool:
        """
        Determine if a claim is auto-adjudicated based on claim type and amount.

        Uses a logistic modifier so higher-value claims are progressively more
        likely to be routed to manual review, reflecting real-world clinical
        review thresholds.

        Args:
            claim_type: Type of claim (Hospital, Extras, Ambulance)
            total_charge: Total claim charge amount
            auto_adj: AutoAdjudicationConfig

        Returns:
            True if claim should be auto-adjudicated
        """
        import math

        if claim_type == "Extras":
            base_rate = auto_adj.extras_auto_rate
            threshold = auto_adj.extras_manual_threshold
        elif claim_type == "Hospital":
            base_rate = auto_adj.hospital_auto_rate
            threshold = auto_adj.hospital_manual_threshold
        elif claim_type == "Ambulance":
            base_rate = auto_adj.ambulance_auto_rate
            threshold = auto_adj.ambulance_manual_threshold
        else:
            base_rate = 0.80
            threshold = 10_000

        # Apply logistic penalty based on claim amount
        amount = float(total_charge) if total_charge else 0.0
        if amount > 0 and threshold > 0:
            z = auto_adj.amount_steepness * (math.log(amount) - math.log(threshold))
            penalty = auto_adj.amount_penalty_weight / (1 + math.exp(-z))
            effective_rate = base_rate * (1 - penalty)
        else:
            effective_rate = base_rate

        return self.rng.random() < effective_rate

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

                # Apply benefit capping during ASSESSED transition (audit trail)
                # SUBMITTED shows original uncapped amounts, ASSESSED shows capped amounts
                if data.get("capped_benefit") is not None and data.get("additional_gap"):
                    self._apply_benefit_capping(claim_id, data)

                # Create claim assessment audit record
                assessment = self._create_claim_assessment(claim_id, data)
                self.batch_writer.add("claim_assessment", assessment.model_dump())

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

                    # Emit CRM event for claim rejection
                    member_id = data["member_data"]["member"].member_id
                    self._emit_crm_event(
                        "claim_rejected",
                        claim_id,
                        {
                            "policy_id": data["policy_id"],
                            "member_id": member_id,
                            "charge_amount": float(data.get("benefit_amount", 0) or 0),
                            "denial_reason": data["denial_reason"].value if data["denial_reason"] else None,
                        },
                    )

                    # Update policy claims stats for churn model
                    self._update_policy_claims_stats(
                        data["policy_id"], data, "REJECTED", current_date
                    )

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

                # Emit CRM event for claim payment
                member_id = data["member_data"]["member"].member_id
                self._emit_crm_event(
                    "claim_paid",
                    claim_id,
                    {
                        "policy_id": data["policy_id"],
                        "member_id": member_id,
                        "charge_amount": float(data.get("benefit_amount", 0) or 0),
                    },
                )

                # Update policy claims stats for churn model
                self._update_policy_claims_stats(
                    data["policy_id"], data, "PAID", current_date
                )

                del self.pending_claims[claim_id]

    def _apply_benefit_capping(self, claim_id: UUID, data: dict) -> None:
        """
        Apply benefit capping during ASSESSED transition for audit trail.

        SUBMITTED state shows original uncapped amounts.
        ASSESSED state shows capped amounts (after limit verification).

        Updates claim, claim_line, and extras_claim records with capped amounts.

        Args:
            claim_id: The claim UUID
            data: Pending claim data with capping info
        """
        capped_benefit = data["capped_benefit"]
        additional_gap = data["additional_gap"]
        original_gap = data.get("original_gap", Decimal("0"))
        claim_line_ids = data["claim_line_ids"]
        extras_claim_id = data.get("extras_claim_id")

        # Update claim header with capped amounts
        self.batch_writer.update_record(
            "claim",
            "claim_id",
            claim_id,
            {
                "total_benefit": float(capped_benefit),
                "total_gap": float(original_gap + additional_gap),
                "modified_at": self.sim_env.current_datetime.isoformat(),
                "modified_by": "SIMULATION",
            },
        )

        # Update claim line with capped amounts
        for claim_line_id in claim_line_ids:
            self.batch_writer.flush_for_cdc("claim_line", "claim_line_id", claim_line_id)
            self.batch_writer.update_record(
                "claim_line",
                "claim_line_id",
                claim_line_id,
                {
                    "benefit_amount": float(capped_benefit),
                    "gap_amount": float(original_gap + additional_gap),
                    "modified_at": self.sim_env.current_datetime.isoformat(),
                    "modified_by": "SIMULATION",
                },
            )

        # Update extras claim with capped amounts (if extras claim)
        if extras_claim_id:
            self.batch_writer.flush_for_cdc("extras_claim", "extras_claim_id", extras_claim_id)
            self.batch_writer.update_record(
                "extras_claim",
                "extras_claim_id",
                extras_claim_id,
                {
                    "benefit_amount": float(capped_benefit),
                    "annual_limit_impact": float(capped_benefit),
                    "modified_at": self.sim_env.current_datetime.isoformat(),
                    "modified_by": "SIMULATION",
                },
            )

        logger.debug(
            "benefit_capping_applied",
            claim_id=str(claim_id),
            original_benefit=float(data.get("original_benefit", 0) or 0),
            capped_benefit=float(capped_benefit),
            additional_gap=float(additional_gap),
        )

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

    def _create_claim_assessment(self, claim_id: UUID, data: dict) -> ClaimAssessmentCreate:
        """
        Create a claim assessment audit record from pending claim data.

        Populates assessment type, assessor, outcome, validation checks,
        and benefit adjustment details based on adjudication results.

        Args:
            claim_id: The claim UUID
            data: Pending claim data dict

        Returns:
            ClaimAssessmentCreate ready for batch writing
        """
        is_auto = data.get("is_auto_adjudicated", False)
        approved = data.get("approved", False)
        denial_reason = data.get("denial_reason")
        claim_type = data.get("claim_type", "Extras")

        # Assessment type
        assessment_type = "Auto" if is_auto else "Manual"

        # Assessed by (based on adjudication type and claim type)
        if is_auto:
            assessed_by = "AUTO_RULES_ENGINE"
        elif claim_type == "Hospital":
            assessed_by = "CLINICAL_REVIEWER"
        else:
            assessed_by = "BENEFITS_ASSESSOR"

        # Outcome
        outcome = "Approved" if approved else "Rejected"

        # Validation checks
        # Claims that fail WP never reach SUBMITTED, so waiting_period_check is always True
        waiting_period_check = True
        eligibility_check = True
        benefit_limit_check = True

        if denial_reason:
            if denial_reason in (DenialReason.NO_COVERAGE, DenialReason.POLICY_EXCLUSIONS):
                eligibility_check = False
            if denial_reason == DenialReason.LIMITS_EXHAUSTED:
                benefit_limit_check = False

        # Benefit adjustments (extras capping)
        original_benefit = data.get("original_benefit") or data.get("claim_total_benefit")
        capped_benefit = data.get("capped_benefit")
        if capped_benefit is not None:
            adjusted_benefit = capped_benefit
            if capped_benefit < (original_benefit or Decimal("0")):
                adjustment_reason = "Annual benefit limit reached"
            else:
                adjustment_reason = None
        else:
            adjusted_benefit = original_benefit
            adjustment_reason = None

        # Notes for rejections
        notes = None
        if not approved and denial_reason:
            notes = f"Denied: {denial_reason.value}"

        return ClaimAssessmentCreate(
            assessment_id=self.id_generator.generate_uuid(),
            claim_id=claim_id,
            assessment_type=assessment_type,
            assessment_date=self.sim_env.current_datetime,
            assessed_by=assessed_by,
            original_benefit=original_benefit,
            adjusted_benefit=adjusted_benefit,
            adjustment_reason=adjustment_reason,
            waiting_period_check=waiting_period_check,
            benefit_limit_check=benefit_limit_check,
            eligibility_check=eligibility_check,
            outcome=outcome,
            notes=notes,
            created_at=self.sim_env.current_datetime,
            created_by="SIMULATION",
        )

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
        return self.claims_gen.denial_reason_ids.get(denial_reason, 1)

    def _emit_crm_event(
        self,
        event_type: str,
        claim_id: UUID,
        data: dict,
    ) -> None:
        """
        Emit a CRM trigger event for processing by CRMProcess.

        Args:
            event_type: Type of event (claim_submitted, claim_rejected, claim_paid)
            claim_id: The claim UUID
            data: Additional event data
        """
        if not self.shared_state:
            return

        self.shared_state.add_crm_event({
            "event_type": event_type,
            "claim_id": claim_id,
            "policy_id": data.get("policy_id"),
            "member_id": data.get("member_id"),
            "charge_amount": data.get("charge_amount", 0),
            "timestamp": self.sim_env.current_datetime,
        })

    def _update_policy_claims_stats(
        self,
        policy_id: UUID,
        data: dict,
        outcome: str,
        current_date: date,
    ) -> None:
        """
        Append claim outcome to policy's rolling event logs in SharedState.

        Read by PolicyLifecycleProcess._get_claims_history() to feed
        the ChurnPredictionModel with a 12-month rolling window.

        Args:
            policy_id: The policy UUID
            data: Pending claim data dict
            outcome: "PAID" or "REJECTED"
            current_date: Current simulation date
        """
        if not self.shared_state:
            return
        policy_dict = self.shared_state.active_policies.get(policy_id)
        if policy_dict is None:
            return

        if outcome == "REJECTED":
            denial_log = policy_dict.get("denial_log", [])
            denial_log.append(current_date)
            policy_dict["denial_log"] = denial_log

        elif outcome == "PAID":
            policy_dict["last_claim_date"] = current_date

            benefit = float(
                data.get("benefit_amount") or data.get("claim_total_benefit") or 0
            )

            # Extras claims have capping info; hospital/ambulance use claim_total_gap
            if (
                data.get("original_gap") is not None
                and data.get("additional_gap") is not None
            ):
                gap = float(data["original_gap"]) + float(data["additional_gap"])
            else:
                gap = float(data.get("claim_total_gap") or 0)

            paid_log = policy_dict.get("paid_claim_log", [])
            paid_log.append((current_date, benefit, gap))
            policy_dict["paid_claim_log"] = paid_log

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

    # =========================================================================
    # Fraud Helper Methods
    # =========================================================================

    def _get_inline_fraud_fields(
        self,
        claim: ClaimCreate,
        fraud_type: FraudType,
        member_data: dict,
    ) -> dict[str, Any]:
        """
        Dispatch to appropriate FraudGenerator method for inline fraud.

        Returns dict of fields to set on the claim via object.__setattr__.
        """
        if fraud_type == FraudType.DRG_UPCODING:
            return self.fraud_gen.apply_drg_upcoding(claim)
        elif fraud_type == FraudType.EXTRAS_UPCODING:
            return self.fraud_gen.apply_extras_upcoding(claim)
        elif fraud_type == FraudType.PHANTOM_BILLING:
            return self.fraud_gen.apply_phantom_billing(claim, self.shared_state)
        elif fraud_type == FraudType.PROVIDER_OUTLIER:
            return self.fraud_gen.apply_provider_outlier(claim)
        elif fraud_type == FraudType.TEMPORAL_ANOMALY:
            return self.fraud_gen.apply_temporal_anomaly(claim)
        elif fraud_type == FraudType.GEOGRAPHIC_ANOMALY:
            member = member_data.get("member")
            member_state = member.state if member else "NSW"
            return self.fraud_gen.apply_geographic_anomaly(claim, member_state)
        return {}

    def _generate_duplicate_claim(
        self,
        source_claim: ClaimCreate,
        member_data: dict,
        fraud_type: FraudType,
        service_date: date,
    ) -> None:
        """Generate a duplicate fraud claim from a legitimate source claim."""
        # Build claim snapshot for the fraud generator
        claim_snapshot = {
            "claim_id": source_claim.claim_id,
            "policy_id": source_claim.policy_id,
            "member_id": source_claim.member_id,
            "coverage_id": source_claim.coverage_id,
            "claim_type": (
                source_claim.claim_type.value
                if isinstance(source_claim.claim_type, ClaimType)
                else source_claim.claim_type
            ),
            "service_date": source_claim.service_date,
            "total_charge": source_claim.total_charge,
            "provider_id": source_claim.provider_id,
            "hospital_id": source_claim.hospital_id,
            "claim_channel": (
                source_claim.claim_channel.value
                if isinstance(source_claim.claim_channel, ClaimChannel)
                else source_claim.claim_channel
            ),
        }

        if fraud_type == FraudType.EXACT_DUPLICATE:
            dup_data = self.fraud_gen.generate_exact_duplicate(
                claim_snapshot, service_date,
            )
        else:
            dup_data = self.fraud_gen.generate_near_duplicate(
                claim_snapshot, service_date,
            )

        self._write_duplicate_claim(dup_data, member_data)

    def _write_duplicate_claim(
        self,
        dup_data: dict[str, Any],
        member_data: dict,
    ) -> None:
        """Write a duplicate fraud claim to batch writer."""
        claim_id = self.id_generator.generate_uuid()
        claim_number = self.id_generator.generate_claim_number()

        # Convert string values back to enums
        claim_type = dup_data.get("claim_type")
        if isinstance(claim_type, str):
            claim_type = ClaimType(claim_type)

        claim_channel = dup_data.get("claim_channel", "Online")
        if isinstance(claim_channel, str):
            claim_channel = ClaimChannel(claim_channel)

        claim = ClaimCreate(
            claim_id=claim_id,
            claim_number=claim_number,
            policy_id=dup_data["policy_id"],
            member_id=dup_data["member_id"],
            coverage_id=dup_data["coverage_id"],
            claim_type=claim_type,
            claim_status=ClaimStatus.SUBMITTED,
            service_date=dup_data["service_date"],
            lodgement_date=dup_data["lodgement_date"],
            provider_id=dup_data.get("provider_id"),
            hospital_id=dup_data.get("hospital_id"),
            total_charge=dup_data["total_charge"],
            claim_channel=claim_channel,
            is_fraud=True,
            fraud_type=dup_data["fraud_type"],
            fraud_original_charge=dup_data.get("fraud_original_charge"),
            fraud_inflation_amount=dup_data.get("fraud_inflation_amount"),
            fraud_inflation_ratio=dup_data.get("fraud_inflation_ratio"),
            fraud_source_claim_id=dup_data.get("fraud_source_claim_id"),
            created_at=self.sim_env.current_datetime,
        )

        # Create minimal claim line for duplicate
        claim_line_id = self.id_generator.generate_uuid()
        claim_line = ClaimLineCreate(
            claim_line_id=claim_line_id,
            claim_id=claim_id,
            line_number=1,
            item_code="DUP",
            service_date=dup_data["service_date"],
            charge_amount=dup_data["total_charge"],
            created_at=self.sim_env.current_datetime,
        )

        self.batch_writer.add("claim", claim.model_dump_db())
        self.batch_writer.add("claim_line", claim_line.model_dump())

        # Schedule lifecycle transitions (duplicates pass initial checks)
        self._schedule_claim_transitions(
            claim=claim,
            claim_line_ids=[claim_line_id],
            member_data=member_data,
            lodgement_date=dup_data["lodgement_date"],
            approved=True,
            denial_reason=None,
            benefit_category_id=None,
            benefit_amount=None,
        )

    def _write_unbundled_claims(
        self,
        original_claim: ClaimCreate,
        original_line: Any,
        member_data: dict,
        service_date: date,
        approved: bool,
        denial_reason: DenialReason | None,
        extras_claim: Any = None,
        hospital_records: list[tuple[str, Any]] | None = None,
    ) -> int:
        """
        Write unbundled fraud claims (fragments) to batch writer.

        Splits the original claim into multiple fragments with inflation.
        First fragment reuses original claim_id (so detail records still link).
        Additional fragments get new claim_ids with minimal records.

        Returns number of fragments written.
        """
        fragments = self.fraud_gen.generate_unbundled_claims(
            original_claim.total_charge,
        )

        original_benefit = original_claim.total_benefit or Decimal("0")
        original_charge = original_claim.total_charge
        benefit_ratio = (
            original_benefit / original_charge
            if original_charge > 0 else Decimal("0")
        )

        for i, fragment in enumerate(fragments):
            frag_charge = fragment["charge_amount"]
            frag_benefit = (frag_charge * benefit_ratio).quantize(Decimal("0.01"))
            frag_gap = frag_charge - frag_benefit

            fraud_update = {
                "total_charge": frag_charge,
                "total_benefit": frag_benefit,
                "total_gap": frag_gap,
                "is_fraud": True,
                "fraud_type": FraudType.UNBUNDLING,
                "fraud_original_charge": fragment["fraud_original_charge"],
                "fraud_inflation_amount": fragment["fraud_inflation_amount"],
                "fraud_inflation_ratio": fragment["fraud_inflation_ratio"],
            }

            if i == 0:
                # First fragment: reuse original claim_id
                frag_claim = original_claim.model_copy(update=fraud_update)
                frag_claim_id = original_claim.claim_id

                if original_line:
                    frag_line = original_line.model_copy(update={
                        "charge_amount": frag_charge,
                        "benefit_amount": frag_benefit,
                        "gap_amount": frag_gap,
                    })
                    frag_line_id = frag_line.claim_line_id
                else:
                    frag_line_id = self.id_generator.generate_uuid()
                    frag_line = ClaimLineCreate(
                        claim_line_id=frag_line_id,
                        claim_id=frag_claim_id,
                        line_number=1,
                        item_code="UNBUNDLE",
                        service_date=service_date,
                        charge_amount=frag_charge,
                        benefit_amount=frag_benefit,
                        gap_amount=frag_gap,
                        created_at=self.sim_env.current_datetime,
                    )
            else:
                # Additional fragments: new claim_ids
                frag_claim_id = self.id_generator.generate_uuid()
                fraud_update["claim_id"] = frag_claim_id
                fraud_update["claim_number"] = (
                    self.id_generator.generate_claim_number()
                )
                fraud_update["created_at"] = self.sim_env.current_datetime
                frag_claim = original_claim.model_copy(update=fraud_update)

                frag_line_id = self.id_generator.generate_uuid()
                frag_line = ClaimLineCreate(
                    claim_line_id=frag_line_id,
                    claim_id=frag_claim_id,
                    line_number=1,
                    item_code="UNBUNDLE",
                    service_date=service_date,
                    charge_amount=frag_charge,
                    benefit_amount=frag_benefit,
                    gap_amount=frag_gap,
                    created_at=self.sim_env.current_datetime,
                )

            # Write claim and line
            self.batch_writer.add("claim", frag_claim.model_dump_db())
            self.batch_writer.add("claim_line", frag_line.model_dump())

            # Write detail records for first fragment only
            if i == 0:
                if extras_claim:
                    updated_extras = extras_claim.model_copy(update={
                        "charge_amount": frag_charge,
                        "benefit_amount": frag_benefit,
                    })
                    self.batch_writer.add(
                        "extras_claim", updated_extras.model_dump_db(),
                    )
                if hospital_records:
                    for table_name, record_data in hospital_records:
                        self.batch_writer.add(table_name, record_data)

            # Schedule lifecycle transitions for each fragment
            self._schedule_claim_transitions(
                claim=frag_claim,
                claim_line_ids=[frag_line_id],
                member_data=member_data,
                lodgement_date=service_date,
                approved=approved,
                denial_reason=denial_reason if not approved else None,
                benefit_category_id=None,
                benefit_amount=frag_benefit,
            )

        return len(fragments)

    def _add_to_duplication_pool(
        self,
        claim: ClaimCreate,
        member_data: dict,
    ) -> None:
        """Add a legitimate claim to the SharedState duplication pool."""
        if not self.shared_state:
            return
        self.shared_state.add_claim_for_duplication({
            "claim_id": claim.claim_id,
            "policy_id": claim.policy_id,
            "member_id": claim.member_id,
            "coverage_id": claim.coverage_id,
            "claim_type": (
                claim.claim_type.value
                if isinstance(claim.claim_type, ClaimType)
                else claim.claim_type
            ),
            "service_date": claim.service_date,
            "total_charge": claim.total_charge,
            "provider_id": claim.provider_id,
            "hospital_id": claim.hospital_id,
            "claim_channel": (
                claim.claim_channel.value
                if isinstance(claim.claim_channel, ClaimChannel)
                else claim.claim_channel
            ),
        })

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
            fraud_claims=stats.get("fraud_claims", 0),
        )
