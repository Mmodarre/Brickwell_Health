"""
Claims generator for Brickwell Health Simulator.

Generates claims (Extras, Hospital, Ambulance) for policy members.
"""

from datetime import date
from decimal import Decimal
from typing import Any, TYPE_CHECKING
from uuid import UUID

# Placeholder coverage ID for rejected claims (members attempting to claim without coverage)
# This UUID must match the placeholder record created in init_db.py
NO_COVERAGE_PLACEHOLDER_ID = UUID("00000000-0000-0000-0000-000000000000")

from brickwell_health.domain.claims import (
    ClaimCreate,
    ClaimLineCreate,
    HospitalAdmissionCreate,
    ExtrasClaimCreate,
    AmbulanceClaimCreate,
    ProsthesisClaimCreate,
    MedicalServiceCreate,
)
from brickwell_health.domain.enums import (
    ClaimType,
    ClaimStatus,
    ClaimChannel,
    AdmissionType,
    AccommodationType,
    DentalServiceType,
    DenialReason,
)
from brickwell_health.domain.coverage import CoverageCreate
from brickwell_health.domain.member import MemberCreate
from brickwell_health.domain.policy import PolicyCreate
from brickwell_health.config.models import ClaimsConfig
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.statistics.claim_propensity import ClaimPropensityModel

if TYPE_CHECKING:
    from brickwell_health.core.environment import SimulationEnvironment


class ClaimsGenerator(BaseGenerator[ClaimCreate]):
    """
    Generates claims for policy members.
    """

    # Prosthesis-eligible procedures by DRG prefix and their probability of having prosthesis
    # Key: DRG prefix, Value: (probability, prosthesis_category_patterns)
    # Category patterns are used to match prosthesis_list_item.category_description
    PROSTHESIS_PROCEDURES = {
        "I03": (0.95, ["hip"]),  # Hip replacement
        "I04": (0.95, ["knee"]),  # Knee replacement
        "I08": (0.80, ["spinal", "spine"]),  # Spinal procedures
        "I18": (0.70, ["joint", "shoulder", "ankle", "elbow"]),  # Other joint procedures
        "F01": (0.85, ["pacemaker", "cardiac"]),  # Pacemaker/ICD
        "F05": (0.75, ["stent", "cardiac"]),  # Cardiac catheterization
        "F10": (0.60, ["valve", "cardiac"]),  # Cardiac valve procedures
        "D01": (0.90, ["cochlear", "implant"]),  # Cochlear implant
        "G02": (0.40, ["hernia", "mesh"]),  # Hernia repair
        "J10": (0.30, ["lens", "intraocular"]),  # Cataract/lens
    }

    # Provider types likely to be involved by admission type
    PROVIDER_MIX = {
        AdmissionType.ELECTIVE: {
            "Surgeon": 1.0,      # Always
            "Anesthetist": 0.9,  # Usually
            "Assistant": 0.4,   # Sometimes
            "Physician": 0.3,   # Sometimes
            "Pathology": 0.6,   # Often
            "Radiology": 0.5,   # Often
        },
        AdmissionType.EMERGENCY: {
            "Surgeon": 0.6,
            "Anesthetist": 0.5,
            "Assistant": 0.2,
            "Physician": 0.8,   # More common in emergency
            "Pathology": 0.8,
            "Radiology": 0.7,
        },
        AdmissionType.MATERNITY: {
            "Surgeon": 0.3,     # C-section only
            "Anesthetist": 0.7, # Epidural/C-section
            "Assistant": 0.2,
            "Physician": 0.5,
            "Pathology": 0.5,
            "Radiology": 0.3,
        },
    }

    def __init__(
        self,
        rng,
        reference,
        id_generator: IDGenerator,
        sim_env: "SimulationEnvironment",
        config: ClaimsConfig | None = None,
    ):
        """
        Initialize the claims generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
            sim_env: Simulation environment for time access
            config: Optional claims configuration (uses defaults if not provided)
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.propensity = ClaimPropensityModel(rng, reference, config)

        # Load reference data from database
        self._load_denial_reason_mapping()
        self._load_prosthesis_catalog()
        self._load_mbs_items_by_provider()

    def _load_denial_reason_mapping(self) -> None:
        """Load claim rejection reasons from reference data."""
        rejection_reasons = self.reference.get_claim_rejection_reasons()

        # Build mapping from reason_code to rejection_reason_id
        self.denial_reason_ids = {}
        for reason in rejection_reasons:
            reason_code = reason.get("reason_code", "").upper()
            reason_id = reason.get("rejection_reason_id")

            # Map DenialReason enum values to rejection_reason_id
            # Match by reason_code (e.g., "NO_COVERAGE" -> DenialReason.NO_COVERAGE)
            if reason_code == "NO_COVERAGE":
                self.denial_reason_ids[DenialReason.NO_COVERAGE] = reason_id
            elif reason_code == "LIMITS_EXHAUSTED":
                self.denial_reason_ids[DenialReason.LIMITS_EXHAUSTED] = reason_id
            elif reason_code == "WAITING_PERIOD":
                self.denial_reason_ids[DenialReason.WAITING_PERIOD] = reason_id
            elif reason_code == "POLICY_EXCLUSIONS":
                self.denial_reason_ids[DenialReason.POLICY_EXCLUSIONS] = reason_id
            elif reason_code == "PRE_EXISTING":
                self.denial_reason_ids[DenialReason.PRE_EXISTING] = reason_id
            elif reason_code == "PROVIDER_ISSUES":
                self.denial_reason_ids[DenialReason.PROVIDER_ISSUES] = reason_id
            elif reason_code == "ADMINISTRATIVE":
                self.denial_reason_ids[DenialReason.ADMINISTRATIVE] = reason_id
            elif reason_code == "MEMBERSHIP_INACTIVE":
                self.denial_reason_ids[DenialReason.MEMBERSHIP_INACTIVE] = reason_id

    def _load_prosthesis_catalog(self) -> None:
        """Load prosthesis items from reference data and build catalog by category patterns."""
        prosthesis_items = self.reference.get_prosthesis_items()

        # Build category_id -> patterns lookup from prosthesis_category.json
        prosthesis_categories = self.reference.get_prosthesis_categories()
        category_patterns: dict[int, list[str]] = {
            cat["prosthesis_category_id"]: cat.get("patterns", [])
            for cat in prosthesis_categories
        }

        # Build catalog grouped by category patterns (for PROSTHESIS_PROCEDURES mapping)
        # Structure: {pattern: [(item_id, billing_code, description, min_benefit, max_benefit), ...]}
        self.prosthesis_catalog: dict[str, list[tuple]] = {}

        for item in prosthesis_items:
            item_id = item.get("prosthesis_item_id")
            billing_code = item.get("billing_code", "")
            description = item.get("item_name", "")
            min_benefit = float(item.get("minimum_benefit") or 0)
            max_benefit = float(item.get("maximum_benefit") or 0)

            # Look up patterns from category data
            category_id = item.get("prosthesis_category_id")
            patterns = category_patterns.get(category_id, [])

            # Add to catalog under each matching pattern
            for pattern in patterns:
                if pattern not in self.prosthesis_catalog:
                    self.prosthesis_catalog[pattern] = []
                self.prosthesis_catalog[pattern].append(
                    (item_id, billing_code, description, min_benefit, max_benefit)
                )

    def _load_mbs_items_by_provider(self) -> None:
        """Load MBS items from reference data and group by provider type."""
        mbs_items = self.reference.get_mbs_items()

        # Build category_id -> provider_type lookup from mbs_category.json
        mbs_categories = self.reference.get_mbs_categories()
        category_provider_types: dict[int, str | None] = {
            cat["mbs_category_id"]: cat.get("provider_type")
            for cat in mbs_categories
        }

        # Charge multipliers by provider type
        charge_multipliers = {
            "Surgeon": 1.8,
            "Anesthetist": 1.3,
            "Assistant": 1.2,
            "Physician": 1.5,
            "Pathology": 1.0,
            "Radiology": 1.2,
        }

        # Build MBS items grouped by provider type
        # Structure: {provider_type: [(item_code, description, min_fee, max_fee, charge_multiplier), ...]}
        self.mbs_items_by_provider: dict[str, list[tuple]] = {
            pt: [] for pt in charge_multipliers
        }

        for item in mbs_items:
            item_code = item.get("item_number", "")
            description = item.get("item_description", "")
            schedule_fee = float(item.get("schedule_fee") or 0)

            # Look up provider type from category data
            category_id = item.get("category_id")
            provider_type = category_provider_types.get(category_id)

            if provider_type and provider_type in charge_multipliers:
                charge_multiplier = charge_multipliers[provider_type]
                # Higher-fee surgeons get a larger multiplier
                if provider_type == "Surgeon" and schedule_fee > 400:
                    charge_multiplier = 2.0

                # Generate fee range (+/- 20% of schedule fee)
                min_fee = schedule_fee * 0.8
                max_fee = schedule_fee * 1.2

                self.mbs_items_by_provider[provider_type].append(
                    (item_code, description, min_fee, max_fee, charge_multiplier)
                )

    def _select_provider(
        self,
        provider_type: str | None = None,
        state: str | None = None,
    ) -> dict[str, Any]:
        """
        Select a real provider from reference data.

        Args:
            provider_type: Optional provider type filter
            state: Optional state code filter

        Returns:
            Provider dict with provider_id, provider_number, etc.
        """
        providers = self.reference.get_providers_by_type_and_state(
            provider_type=provider_type,
            state=state,
            active_only=True,
        )

        if not providers:
            # Fallback: get any active provider
            providers = self.reference.get_providers(active_only=True)

        if providers:
            return self.rng.choice(providers)

        # Ultimate fallback: return a fake provider dict
        return {
            "provider_id": 1,
            "provider_number": f"PRV{self.uniform_int(100000, 999999)}",
            "provider_name": "Unknown Provider",
        }

    def _select_hospital(
        self,
        state: str | None = None,
        has_icu: bool = False,
    ) -> dict[str, Any]:
        """
        Select a real hospital from reference data.

        Args:
            state: Optional state code filter
            has_icu: If True, only select hospitals with ICU

        Returns:
            Hospital dict with hospital_id, hospital_name, etc.
        """
        hospitals = self.reference.get_hospitals_by_state(
            state=state,
            has_icu=has_icu if has_icu else None,
            active_only=True,
        )

        if not hospitals:
            # Fallback: get any active hospital
            hospitals = self.reference.get_hospitals(active_only=True)

        if hospitals:
            return self.rng.choice(hospitals)

        # Ultimate fallback: return a fake hospital dict
        return {
            "hospital_id": 1,
            "hospital_name": "Unknown Hospital",
        }

    def _select_extras_item(
        self,
        service_type: str,
    ) -> dict[str, Any]:
        """
        Select a real extras item code from reference data.

        Args:
            service_type: Service type name (e.g., "Dental", "Optical")

        Returns:
            Extras item dict with extras_item_id, item_code, typical_fee, etc.
        """
        items = self.reference.get_extras_items_by_service_type(
            service_type=service_type,
            active_only=True,
        )

        if items:
            return self.rng.choice(items)

        # Fallback: return a fake item dict
        return {
            "extras_item_id": 1,
            "item_code": "000",
            "item_description": f"{service_type} Service",
            "typical_fee": 50.0,
        }

    def generate(self, **kwargs: Any) -> ClaimCreate:
        """
        Generate a claim (delegates to generate_extras_claim for header).

        This is the abstract method implementation required by BaseGenerator.
        For full claim generation, use generate_extras_claim, generate_hospital_claim,
        or generate_ambulance_claim which return tuples with related records.

        Args:
            **kwargs: Arguments including policy, member, coverage, service_date

        Returns:
            ClaimCreate instance (header only)
        """
        # Default to extras claim and return just the header
        claim, _, _ = self.generate_extras_claim(**kwargs)
        return claim

    def generate_extras_claim(
        self,
        policy: PolicyCreate,
        member: MemberCreate,
        coverage: CoverageCreate,
        service_date: date,
        service_type: str | None = None,
        **kwargs: Any,
    ) -> tuple[ClaimCreate, ClaimLineCreate, ExtrasClaimCreate]:
        """
        Generate an extras (ancillary) claim.

        Args:
            policy: Policy
            member: Claiming member
            coverage: Extras coverage
            service_date: Date of service
            service_type: Optional pre-sampled service type (e.g., "Dental", "Optical").
                         If not provided, will be sampled.

        Returns:
            Tuple of (ClaimCreate, ClaimLineCreate, ExtrasClaimCreate)
        """
        claim_id = self.id_generator.generate_uuid()
        claim_line_id = self.id_generator.generate_uuid()
        extras_claim_id = self.id_generator.generate_uuid()

        # Use provided service type or sample a new one
        if service_type is None:
            service_type = self.propensity.sample_extras_service_type()

        # Handle dental sub-categories
        dental_service_type: DentalServiceType | None = None
        if service_type == "Dental":
            dental_service_type = self.propensity.sample_dental_service_type()
            charge_amount = Decimal(str(round(
                self.propensity.sample_dental_claim_amount(dental_service_type), 2
            )))
            # Update item description with dental sub-type
            item_description = f"Dental {dental_service_type.value} service"
        else:
            charge_amount = Decimal(str(round(
                self.propensity.sample_claim_amount(service_type), 2
            )))
            item_description = f"{service_type} service"

        # Calculate benefit
        benefit_pct = self.propensity.sample_benefit_percentage(service_type)
        benefit_amount = (charge_amount * Decimal(str(benefit_pct))).quantize(
            Decimal("0.01")
        )
        gap_amount = charge_amount - benefit_amount

        # Select a real provider from reference data
        member_state = getattr(member, "state", None)
        selected_provider = self._select_provider(state=member_state)
        provider_id = selected_provider.get("provider_id")

        # Select a real extras item from reference data
        selected_extras_item = self._select_extras_item(service_type)

        # Claim header - created as SUBMITTED for lifecycle transitions
        claim = ClaimCreate(
            claim_id=claim_id,
            claim_number=self.id_generator.generate_claim_number(),
            policy_id=policy.policy_id,
            member_id=member.member_id,
            coverage_id=coverage.coverage_id,
            claim_type=ClaimType.EXTRAS,
            claim_status=ClaimStatus.SUBMITTED,  # Changed from PAID for lifecycle
            service_date=service_date,
            lodgement_date=service_date,
            assessment_date=None,  # Set during lifecycle transition
            payment_date=None,     # Set during lifecycle transition
            provider_id=provider_id,
            hospital_id=None,
            total_charge=charge_amount,
            total_benefit=benefit_amount,
            total_gap=gap_amount,
            excess_applied=Decimal("0"),
            co_payment_applied=Decimal("0"),
            rejection_reason_id=None,
            rejection_notes=None,
            claim_channel=ClaimChannel.HICAPS,
            pay_to="Member",
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        # Get item code
        item_code = self._get_extras_item_code(service_type, dental_service_type)

        # Claim line - created as Pending for lifecycle transitions
        claim_line = ClaimLineCreate(
            claim_line_id=claim_line_id,
            claim_id=claim_id,
            line_number=1,
            item_code=item_code,
            item_description=item_description,
            clinical_category_id=None,
            benefit_category_id=self._get_benefit_category_id(service_type),
            service_date=service_date,
            quantity=1,
            charge_amount=charge_amount,
            schedule_fee=None,
            benefit_amount=benefit_amount,
            gap_amount=gap_amount,
            line_status="Pending",  # Changed from "Paid" for lifecycle
            rejection_reason_id=None,
            provider_id=claim.provider_id,
            provider_number=None,
            tooth_number=self._generate_tooth_number() if service_type == "Dental" else None,
            body_part=None,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        # Extras claim detail - use real extras item from reference data
        extras_claim = ExtrasClaimCreate(
            extras_claim_id=extras_claim_id,
            claim_id=claim_id,
            claim_line_id=claim_line_id,
            service_type=service_type,
            dental_service_type=dental_service_type,
            extras_item_id=selected_extras_item.get("extras_item_id", 1),
            provider_id=claim.provider_id,
            provider_location_id=None,
            service_date=service_date,
            tooth_number=claim_line.tooth_number,
            charge_amount=charge_amount,
            benefit_amount=benefit_amount,
            annual_limit_impact=benefit_amount,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        return claim, claim_line, extras_claim

    def generate_hospital_claim(
        self,
        policy: PolicyCreate,
        member: MemberCreate,
        coverage: CoverageCreate,
        admission_date: date,
        age: int,
        gender: str,
        clinical_category_id: int | None = None,
        **kwargs: Any,
    ) -> tuple[ClaimCreate, list[ClaimLineCreate], HospitalAdmissionCreate, list[ProsthesisClaimCreate], list[MedicalServiceCreate]]:
        """
        Generate a hospital admission claim.

        Args:
            policy: Policy
            member: Admitted member
            coverage: Hospital coverage
            admission_date: Date of admission
            age: Patient age
            gender: Patient gender
            clinical_category_id: Optional pre-sampled clinical category ID.
                If provided, uses this instead of sampling a new one.
                This is used when the category was already sampled for
                waiting period checks.

        Returns:
            Tuple of (ClaimCreate, list[ClaimLineCreate], HospitalAdmissionCreate, 
                      list[ProsthesisClaimCreate], list[MedicalServiceCreate])
        """
        claim_id = self.id_generator.generate_uuid()
        admission_id = self.id_generator.generate_uuid()

        # Determine admission type
        admission_type = self._sample_admission_type(age)
        accommodation_type = self._sample_accommodation_type(admission_type)

        # Generate DRG code (may trigger prosthesis)
        drg_code = self._generate_drg_code(age, admission_type)

        # Length of stay
        los = self.propensity.sample_hospital_length_of_stay(
            admission_type.value, age
        )
        discharge_date = admission_date if los == 0 else date(
            admission_date.year,
            admission_date.month,
            min(28, admission_date.day + los),
        )

        # Charges
        base_charge = Decimal(str(round(
            self.propensity.sample_claim_amount("Hospital", age), 2
        )))

        # Adjust for LOS - calibrated to IHACPA ward costs (~$350-400/day for PHI portion)
        if los > 0:
            daily_rate = Decimal("350")
            accommodation_charge = daily_rate * los
        else:
            accommodation_charge = Decimal("800")  # Day surgery rate

        # Clinical category - use provided or sample new
        # (provided when category was sampled for waiting period check)
        if clinical_category_id is None:
            clinical_category_id = self.propensity.sample_clinical_category(age, gender)

        # Generate prosthesis claims if applicable
        prosthesis_claims, prosthesis_charge = self._generate_prosthesis_claims(
            claim_id, admission_id, drg_code, admission_date
        )

        # Generate medical services (MBS items billed by doctors)
        medical_services, medical_service_charge = self.generate_medical_services(
            claim_id=claim_id,
            admission_id=admission_id,
            admission_type=admission_type,
            clinical_category_id=clinical_category_id,
            service_date=admission_date,
        )

        total_charge = base_charge + accommodation_charge + prosthesis_charge + medical_service_charge

        # Apply excess (only to non-prosthesis charges - prostheses have no-gap)
        excess = coverage.excess_amount or Decimal("0")
        excess_applied = min(excess, base_charge + accommodation_charge)

        # Prosthesis benefits are typically paid in full (no-gap)
        prosthesis_benefit = prosthesis_charge
        benefit_amount = (base_charge + accommodation_charge - excess_applied) + prosthesis_benefit
        # Member gap for hospital claims is the excess amount they pay out-of-pocket
        gap_amount = excess_applied

        # Select a real hospital from reference data based on member's state
        member_state = getattr(member, "state", None)
        # Emergency admissions may need ICU
        needs_icu = admission_type == AdmissionType.EMERGENCY and self.bernoulli(0.1)
        selected_hospital = self._select_hospital(state=member_state, has_icu=needs_icu)
        hospital_id = selected_hospital.get("hospital_id")

        # Claim header - created as SUBMITTED for lifecycle transitions
        claim = ClaimCreate(
            claim_id=claim_id,
            claim_number=self.id_generator.generate_claim_number(),
            policy_id=policy.policy_id,
            member_id=member.member_id,
            coverage_id=coverage.coverage_id,
            claim_type=ClaimType.HOSPITAL,
            claim_status=ClaimStatus.SUBMITTED,  # Changed from PAID for lifecycle
            service_date=admission_date,
            lodgement_date=discharge_date,
            assessment_date=None,  # Set during lifecycle transition
            payment_date=None,     # Set during lifecycle transition
            provider_id=None,
            hospital_id=hospital_id,
            total_charge=total_charge,
            total_benefit=benefit_amount,
            total_gap=gap_amount,
            excess_applied=excess_applied,
            co_payment_applied=Decimal("0"),
            rejection_reason_id=None,
            rejection_notes=None,
            claim_channel=ClaimChannel.HOSPITAL,
            pay_to="Provider",
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        # Claim lines (simplified - accommodation) - created as Pending for lifecycle
        claim_lines = [
            ClaimLineCreate(
                claim_line_id=self.id_generator.generate_uuid(),
                claim_id=claim_id,
                line_number=1,
                item_code="ACCOM",
                item_description="Hospital accommodation",
                clinical_category_id=clinical_category_id,
                benefit_category_id=None,
                service_date=admission_date,
                quantity=max(1, los),
                charge_amount=accommodation_charge,
                schedule_fee=None,
                benefit_amount=accommodation_charge,
                gap_amount=Decimal("0"),
                line_status="Pending",  # Changed from "Paid" for lifecycle
                rejection_reason_id=None,
                provider_id=None,
                provider_number=None,
                tooth_number=None,
                body_part=None,
                created_at=self.get_current_datetime(),
                created_by="SIMULATION",
            )
        ]

        # Calculate theatre charge (base charge minus accommodation)
        theatre_charge = max(Decimal("0"), base_charge - accommodation_charge)

        # Hospital admission
        admission = HospitalAdmissionCreate(
            admission_id=admission_id,
            claim_id=claim_id,
            hospital_id=claim.hospital_id,
            admission_number=f"ADM{self.uniform_int(100000, 999999)}",
            admission_date=admission_date,
            discharge_date=discharge_date,
            admission_type=admission_type,
            accommodation_type=accommodation_type,
            drg_code=drg_code,
            clinical_category_id=clinical_category_id,
            principal_diagnosis=None,
            principal_procedure=None,
            length_of_stay=los,
            theatre_minutes=self.uniform_int(30, 180) if admission_type == AdmissionType.ELECTIVE else None,
            accommodation_charge=accommodation_charge,
            theatre_charge=theatre_charge,
            prosthesis_charge=prosthesis_charge if prosthesis_charge > 0 else None,
            other_charges=None,
            accommodation_benefit=accommodation_charge,
            theatre_benefit=theatre_charge,
            excess_applicable=True,
            excess_amount=excess_applied,
            co_payment_amount=Decimal("0"),
            contracted_hospital=True,
            informed_financial_consent=True,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        return claim, claim_lines, admission, prosthesis_claims, medical_services

    def generate_ambulance_claim(
        self,
        policy: PolicyCreate,
        member: MemberCreate,
        coverage: CoverageCreate,
        incident_date: date,
        **kwargs: Any,
    ) -> tuple[ClaimCreate, AmbulanceClaimCreate]:
        """
        Generate an ambulance claim.

        Args:
            policy: Policy
            member: Member transported
            coverage: Ambulance coverage
            incident_date: Date of incident

        Returns:
            Tuple of (ClaimCreate, AmbulanceClaimCreate)
        """
        claim_id = self.id_generator.generate_uuid()
        ambulance_claim_id = self.id_generator.generate_uuid()

        charge_amount = Decimal(str(round(
            self.propensity.sample_claim_amount("Ambulance"), 2
        )))
        benefit_amount = charge_amount  # Full coverage usually
        gap_amount = Decimal("0")

        # Claim header - created as SUBMITTED for lifecycle transitions
        claim = ClaimCreate(
            claim_id=claim_id,
            claim_number=self.id_generator.generate_claim_number(),
            policy_id=policy.policy_id,
            member_id=member.member_id,
            coverage_id=coverage.coverage_id,
            claim_type=ClaimType.AMBULANCE,
            claim_status=ClaimStatus.SUBMITTED,  # Changed from PAID for lifecycle
            service_date=incident_date,
            lodgement_date=incident_date,
            assessment_date=None,  # Set during lifecycle transition
            payment_date=None,     # Set during lifecycle transition
            provider_id=None,
            hospital_id=None,
            total_charge=charge_amount,
            total_benefit=benefit_amount,
            total_gap=gap_amount,
            excess_applied=Decimal("0"),
            co_payment_applied=Decimal("0"),
            rejection_reason_id=None,
            rejection_notes=None,
            claim_channel=ClaimChannel.PAPER,
            pay_to="Member",
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        ambulance = AmbulanceClaimCreate(
            ambulance_claim_id=ambulance_claim_id,
            claim_id=claim_id,
            incident_date=incident_date,
            incident_location="Unknown",
            incident_state=policy.state_of_residence,
            transport_type=self.choice(["Emergency", "Non-Emergency"]),
            pickup_location=None,
            destination="Hospital",
            distance_km=Decimal(str(self.uniform(5, 50))),
            charge_amount=charge_amount,
            benefit_amount=benefit_amount,
            state_scheme_contribution=Decimal("0"),
            ambulance_provider="State Ambulance Service",
            case_number=f"AMB{self.uniform_int(100000, 999999)}",
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        return claim, ambulance

    def generate_rejected_claim(
        self,
        policy: PolicyCreate,
        member: MemberCreate,
        claim_type: ClaimType,
        service_date: date,
        denial_reason: DenialReason,
        **kwargs: Any,
    ) -> tuple[ClaimCreate, ClaimLineCreate]:
        """
        Generate a rejected claim with a specific denial reason.

        Claims are created as SUBMITTED and go through lifecycle transitions:
        SUBMITTED -> ASSESSED -> REJECTED

        Args:
            policy: Policy
            member: Claiming member
            claim_type: Type of claim (EXTRAS, HOSPITAL, AMBULANCE)
            service_date: Date of attempted service
            denial_reason: Reason for denial (DenialReason enum)

        Returns:
            Tuple of (ClaimCreate, ClaimLineCreate) with SUBMITTED status
        """
        claim_id = self.id_generator.generate_uuid()
        claim_line_id = self.id_generator.generate_uuid()

        # Sample a charge amount and determine item details based on claim type
        if claim_type == ClaimType.EXTRAS:
            service_type = self.propensity.sample_extras_service_type()
            charge_amount = Decimal(str(round(
                self.propensity.sample_claim_amount(service_type), 2
            )))
            claim_channel = ClaimChannel.HICAPS
            item_code = self._get_extras_item_code(service_type, None)
            item_description = f"{service_type} service (rejected)"
            benefit_category_id = self._get_benefit_category_id(service_type)
            # Select real provider from reference data
            selected_provider = self._select_provider()
            provider_id = selected_provider.get("provider_id")
            hospital_id = None
        elif claim_type == ClaimType.HOSPITAL:
            charge_amount = Decimal(str(round(
                self.propensity.sample_claim_amount("Hospital", kwargs.get("age", 40)), 2
            )))
            claim_channel = ClaimChannel.HOSPITAL
            item_code = "HOSP001"
            item_description = "Hospital admission (rejected)"
            benefit_category_id = None
            provider_id = None
            # Select real hospital from reference data
            selected_hospital = self._select_hospital()
            hospital_id = selected_hospital.get("hospital_id")
        else:  # AMBULANCE
            charge_amount = Decimal(str(round(
                self.propensity.sample_claim_amount("Ambulance"), 2
            )))
            claim_channel = ClaimChannel.PAPER
            item_code = "AMB001"
            item_description = "Ambulance service (rejected)"
            benefit_category_id = None
            provider_id = None
            hospital_id = None

        # Claim header - created as SUBMITTED for lifecycle transitions
        claim = ClaimCreate(
            claim_id=claim_id,
            claim_number=self.id_generator.generate_claim_number(),
            policy_id=policy.policy_id,
            member_id=member.member_id,
            coverage_id=NO_COVERAGE_PLACEHOLDER_ID,  # Placeholder for rejected claims
            claim_type=claim_type,
            claim_status=ClaimStatus.SUBMITTED,  # Start as SUBMITTED for lifecycle
            service_date=service_date,
            lodgement_date=service_date,
            assessment_date=None,  # Set during lifecycle transition
            payment_date=None,  # No payment for rejected claims
            provider_id=provider_id,
            hospital_id=hospital_id,
            total_charge=charge_amount,
            total_benefit=Decimal("0"),  # No benefit for rejected claims
            total_gap=charge_amount,  # Member pays full amount (or doesn't proceed)
            excess_applied=Decimal("0"),
            co_payment_applied=Decimal("0"),
            rejection_reason_id=None,  # Set during REJECTED transition
            rejection_notes=None,  # Set during REJECTED transition
            claim_channel=claim_channel,
            pay_to="N/A",  # No payment for rejected claims
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        # Claim line - created as Pending for lifecycle transitions
        claim_line = ClaimLineCreate(
            claim_line_id=claim_line_id,
            claim_id=claim_id,
            line_number=1,
            item_code=item_code,
            item_description=item_description,
            clinical_category_id=None,
            benefit_category_id=benefit_category_id,
            service_date=service_date,
            quantity=1,
            charge_amount=charge_amount,
            schedule_fee=None,
            benefit_amount=Decimal("0"),  # No benefit for rejected claims
            gap_amount=charge_amount,
            line_status="Pending",  # Start as Pending for lifecycle
            rejection_reason_id=None,  # Set during REJECTED transition
            provider_id=provider_id,
            provider_number=None,
            tooth_number=None,
            body_part=None,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        return claim, claim_line

    def _get_rejection_reason_id(self, denial_reason: DenialReason) -> int:
        """
        Get the rejection reason ID for a denial reason.

        Args:
            denial_reason: DenialReason enum value

        Returns:
            Integer ID matching claim_rejection_reason table
        """
        return self.denial_reason_ids.get(denial_reason, 1)

    def _sample_admission_type(self, age: int) -> AdmissionType:
        """Sample admission type based on age."""
        # Emergency more common for older ages
        if age > 65:
            weights = [0.4, 0.5, 0.1]  # Elective, Emergency, Maternity
        elif 20 <= age <= 45:
            weights = [0.5, 0.3, 0.2]
        else:
            weights = [0.6, 0.4, 0.0]

        types = [AdmissionType.ELECTIVE, AdmissionType.EMERGENCY, AdmissionType.MATERNITY]
        return self.choice(types, weights)

    def _sample_accommodation_type(self, admission_type: AdmissionType) -> AccommodationType:
        """Sample accommodation type."""
        if admission_type == AdmissionType.EMERGENCY:
            return self.choice(
                [AccommodationType.PRIVATE_ROOM, AccommodationType.SHARED_ROOM, AccommodationType.ICU],
                [0.5, 0.3, 0.2],
            )
        else:
            return self.choice(
                [AccommodationType.PRIVATE_ROOM, AccommodationType.DAY_SURGERY, AccommodationType.SHARED_ROOM],
                [0.5, 0.3, 0.2],
            )

    def _get_extras_item_code(
        self,
        service_type: str,
        dental_service_type: DentalServiceType | None = None,
    ) -> str:
        """
        Get item code for extras service type.

        Args:
            service_type: Main service type (Dental, Optical, etc.)
            dental_service_type: Optional dental sub-category

        Returns:
            Item code string
        """
        # Dental sub-type specific prefixes
        if service_type == "Dental" and dental_service_type:
            dental_prefixes = {
                DentalServiceType.PREVENTATIVE: "DP",  # Dental Preventative
                DentalServiceType.GENERAL: "DG",       # Dental General
                DentalServiceType.MAJOR: "DM",         # Dental Major
            }
            prefix = dental_prefixes.get(dental_service_type, "D")
        else:
            codes = {
                "Dental": "D",
                "Optical": "O",
                "Physiotherapy": "P",
                "Chiropractic": "C",
                "Podiatry": "PD",
                "Psychology": "PS",
                "Massage": "M",
                "Acupuncture": "A",
            }
            prefix = codes.get(service_type, "X")

        return f"{prefix}{self.uniform_int(100, 999)}"

    def _get_benefit_category_id(self, service_type: str) -> int:
        """Get benefit category ID for service type."""
        # Mapping aligned with reference/benefit_category.json
        category_map = {
            "Dental": 3,        # DENTAL
            "Optical": 7,       # OPTICAL
            "Physiotherapy": 8, # PHYSIO
            "Chiropractic": 9,  # CHIRO
            "Podiatry": 10,     # PODIATRY
            "Psychology": 11,   # PSYCHOLOGY
            "Massage": 13,      # MASSAGE (Remedial Massage)
            "Acupuncture": 14,  # ACUPUNCTURE
        }
        return category_map.get(service_type, 1)  # Default to EXTRAS parent category

    def _generate_tooth_number(self) -> str:
        """Generate a valid tooth number."""
        quadrant = self.uniform_int(1, 5)
        tooth = self.uniform_int(1, 9)
        return f"{quadrant}{tooth}"

    def _generate_drg_code(self, age: int, admission_type: AdmissionType) -> str:
        """
        Generate a DRG code, potentially one associated with prosthesis procedures.

        Args:
            age: Patient age
            admission_type: Type of admission

        Returns:
            DRG code string
        """
        # Prosthesis-related DRGs are more likely for elective admissions and older patients
        if admission_type == AdmissionType.ELECTIVE:
            # Higher chance of prosthesis-eligible procedure for older patients
            prosthesis_probability = 0.15 if age >= 50 else 0.05

            if self.rng.random() < prosthesis_probability:
                # Select a prosthesis-eligible DRG
                drg_prefixes = list(self.PROSTHESIS_PROCEDURES.keys())
                # Weight towards joint replacements for older patients
                if age >= 60:
                    weights = [0.25, 0.25, 0.10, 0.10, 0.10, 0.05, 0.05, 0.02, 0.05, 0.03]
                else:
                    weights = [0.10, 0.10, 0.15, 0.10, 0.15, 0.10, 0.10, 0.05, 0.10, 0.05]
                # Normalize weights to match available prefixes
                weights = [w / sum(weights) for w in weights[:len(drg_prefixes)]]
                selected_prefix = self.choice(drg_prefixes, weights)
                return f"{selected_prefix}Z"

        # Non-prosthesis DRG codes (general medical/surgical)
        general_prefixes = ["J", "G", "E", "B", "H", "K", "L", "M", "N"]
        prefix = self.choice(general_prefixes)
        return f"{prefix}{self.uniform_int(10, 99)}Z"

    def _generate_prosthesis_claims(
        self,
        claim_id: UUID,
        admission_id: UUID,
        drg_code: str,
        implant_date: date,
    ) -> tuple[list[ProsthesisClaimCreate], Decimal]:
        """
        Generate prosthesis claims based on DRG code.

        Args:
            claim_id: Parent claim ID
            admission_id: Hospital admission ID
            drg_code: DRG code of the admission
            implant_date: Date of implantation (admission date)

        Returns:
            Tuple of (list of ProsthesisClaimCreate, total prosthesis charge)
        """
        prosthesis_claims = []
        total_charge = Decimal("0")

        # Extract DRG prefix (e.g., "I03" from "I03Z")
        drg_prefix = drg_code[:3] if len(drg_code) >= 3 else drg_code

        # Check if this DRG has prosthesis
        if drg_prefix not in self.PROSTHESIS_PROCEDURES:
            return prosthesis_claims, total_charge

        probability, prosthesis_patterns = self.PROSTHESIS_PROCEDURES[drg_prefix]

        # Check if prosthesis is used in this case
        if self.rng.random() > probability:
            return prosthesis_claims, total_charge

        # Select prosthesis pattern and get matching items
        # Find patterns that have items in our catalog
        available_patterns = [p for p in prosthesis_patterns if p in self.prosthesis_catalog and self.prosthesis_catalog[p]]

        if not available_patterns:
            return prosthesis_claims, total_charge

        prosthesis_pattern = self.choice(available_patterns)
        items = self.prosthesis_catalog[prosthesis_pattern]

        # Usually 1-2 items per procedure (e.g., implant + lead for pacemaker)
        num_items = 1 if len(items) == 1 else self.uniform_int(1, min(2, len(items)))

        # Select random indices to pick items
        indices = self.rng.choice(len(items), size=num_items, replace=False)
        selected_items = [items[i] for i in indices]

        for idx, (item_id, billing_code, description, min_benefit, max_benefit) in enumerate(selected_items):
            # Generate charge within benefit range
            charge_amount = Decimal(str(round(self.rng.uniform(min_benefit, max_benefit), 2)))

            # Prosthesis benefit is typically the full charge (no-gap arrangement)
            benefit_amount = charge_amount
            gap_amount = Decimal("0")

            prosthesis_claim = ProsthesisClaimCreate(
                prosthesis_claim_id=self.id_generator.generate_uuid(),
                claim_id=claim_id,
                admission_id=admission_id,
                prosthesis_item_id=item_id,  # Use actual item_id from reference table
                billing_code=billing_code,
                item_description=description,
                quantity=1,
                charge_amount=charge_amount,
                benefit_amount=benefit_amount,
                gap_amount=gap_amount,
                implant_date=implant_date,
                created_at=self.get_current_datetime(),
                created_by="SIMULATION",
            )

            prosthesis_claims.append(prosthesis_claim)
            total_charge += charge_amount

        return prosthesis_claims, total_charge

    def generate_medical_services(
        self,
        claim_id: UUID,
        admission_id: UUID,
        admission_type: AdmissionType,
        clinical_category_id: int,
        service_date: date,
    ) -> tuple[list[MedicalServiceCreate], Decimal]:
        """
        Generate medical services (MBS items) billed by doctors for a hospital admission.

        Args:
            claim_id: Parent claim ID
            admission_id: Hospital admission ID
            admission_type: Type of admission (affects provider mix)
            clinical_category_id: Clinical category for the admission
            service_date: Date of service

        Returns:
            Tuple of (list of MedicalServiceCreate, total medical services charge)
        """
        medical_services = []
        total_charge = Decimal("0")

        # Get provider mix for this admission type
        provider_mix = self.PROVIDER_MIX.get(admission_type, self.PROVIDER_MIX[AdmissionType.ELECTIVE])

        # Determine which providers are involved
        for provider_type, probability in provider_mix.items():
            if self.rng.random() > probability:
                continue

            # Get MBS items for this provider type
            mbs_items = self.mbs_items_by_provider.get(provider_type, [])
            if not mbs_items:
                continue

            # Select 1-3 items per provider type
            num_items = self.uniform_int(1, min(3, len(mbs_items)))
            indices = self.rng.choice(len(mbs_items), size=num_items, replace=False)
            selected_items = [mbs_items[i] for i in indices]

            for mbs_item_number, description, min_fee, max_fee, charge_multiplier in selected_items:
                schedule_fee = Decimal(str(round(self.rng.uniform(min_fee, max_fee), 2)))

                # Charge is typically higher than MBS fee
                charge_amount = (schedule_fee * Decimal(str(charge_multiplier))).quantize(Decimal("0.01"))

                # Calculate benefits
                # Medicare pays 75% of schedule fee for in-hospital services
                medicare_benefit = (schedule_fee * Decimal("0.75")).quantize(Decimal("0.01"))

                # PHI gap cover: funds typically cover 25% of schedule fee
                fund_benefit = (schedule_fee * Decimal("0.25")).quantize(Decimal("0.01"))

                # Gap is charge minus total benefits
                gap_amount = max(Decimal("0"), charge_amount - medicare_benefit - fund_benefit)

                # Determine if no-gap arrangement applies (common for contracted doctors)
                no_gap = self.bernoulli(0.6)  # 60% of services are no-gap
                if no_gap:
                    gap_amount = Decimal("0")
                    # Adjust fund benefit to cover the difference
                    fund_benefit = charge_amount - medicare_benefit

                # Select a real provider from reference data
                selected_provider = self._select_provider(provider_type=provider_type)
                provider_id = selected_provider.get("provider_id")
                provider_number = selected_provider.get("provider_number", f"PRV{self.uniform_int(100000, 999999)}")

                medical_service = MedicalServiceCreate(
                    medical_service_id=self.id_generator.generate_uuid(),
                    claim_id=claim_id,
                    admission_id=admission_id,
                    mbs_item_number=mbs_item_number,
                    mbs_item_description=description,
                    mbs_schedule_fee=schedule_fee,
                    provider_id=provider_id,
                    provider_type=provider_type,
                    provider_number=provider_number,
                    service_date=service_date,
                    service_text=f"{provider_type} service - {description}",
                    charge_amount=charge_amount,
                    medicare_benefit=medicare_benefit,
                    fund_benefit=fund_benefit,
                    gap_amount=gap_amount,
                    no_gap_indicator=no_gap,
                    gap_cover_scheme="AccessGap" if no_gap else None,
                    clinical_category_id=clinical_category_id,
                    body_part=None,
                    procedure_laterality=None,
                    multiple_service_rule_applied=False,
                    multiple_service_percentage=None,
                    created_at=self.get_current_datetime(),
                    created_by="SIMULATION",
                )

                medical_services.append(medical_service)
                total_charge += charge_amount

        return medical_services, total_charge
