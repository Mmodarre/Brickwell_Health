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

    # Denial reason ID mapping - must match claim_rejection_reason.json
    DENIAL_REASON_IDS = {
        DenialReason.NO_COVERAGE: 1,
        DenialReason.LIMITS_EXHAUSTED: 2,
        DenialReason.WAITING_PERIOD: 3,
        DenialReason.POLICY_EXCLUSIONS: 4,
        DenialReason.PRE_EXISTING: 5,
        DenialReason.PROVIDER_ISSUES: 6,
        DenialReason.ADMINISTRATIVE: 7,
        DenialReason.MEMBERSHIP_INACTIVE: 8,
    }

    # Prosthesis-eligible procedures by DRG prefix and their probability of having prosthesis
    # Key: DRG prefix, Value: (probability, prosthesis_types)
    PROSTHESIS_PROCEDURES = {
        "I03": (0.95, ["hip_replacement"]),  # Hip replacement
        "I04": (0.95, ["knee_replacement"]),  # Knee replacement
        "I08": (0.80, ["spinal_fusion"]),  # Spinal procedures
        "I18": (0.70, ["joint_implant"]),  # Other joint procedures
        "F01": (0.85, ["pacemaker", "cardiac_device"]),  # Pacemaker/ICD
        "F05": (0.75, ["cardiac_stent"]),  # Cardiac catheterization
        "F10": (0.60, ["cardiac_valve"]),  # Cardiac valve procedures
        "D01": (0.90, ["cochlear_implant"]),  # Cochlear implant
        "G02": (0.40, ["hernia_mesh"]),  # Hernia repair
        "J10": (0.30, ["lens_implant"]),  # Cataract/lens
    }

    # Prosthesis item catalog (simplified - billing code, description, avg cost range)
    # Calibrated to ~40% of original values to match APRA/IHACPA prosthesis benefit data
    PROSTHESIS_CATALOG = {
        "hip_replacement": [
            ("HIP001", "Total Hip Prosthesis - Cemented", (3200, 6000)),
            ("HIP002", "Total Hip Prosthesis - Uncemented", (4000, 7200)),
            ("HIP003", "Hip Resurfacing Prosthesis", (3600, 5600)),
        ],
        "knee_replacement": [
            ("KNE001", "Total Knee Prosthesis - Standard", (2800, 4800)),
            ("KNE002", "Total Knee Prosthesis - High Flex", (3600, 6000)),
            ("KNE003", "Unicompartmental Knee Prosthesis", (2400, 4000)),
        ],
        "spinal_fusion": [
            ("SPN001", "Spinal Fusion Cage - Cervical", (1200, 2400)),
            ("SPN002", "Spinal Fusion Cage - Lumbar", (1600, 3200)),
            ("SPN003", "Pedicle Screw System", (2000, 4800)),
        ],
        "joint_implant": [
            ("JNT001", "Shoulder Prosthesis", (2400, 4800)),
            ("JNT002", "Ankle Prosthesis", (2000, 4000)),
            ("JNT003", "Elbow Prosthesis", (2000, 3600)),
        ],
        "pacemaker": [
            ("PAC001", "Single Chamber Pacemaker", (1600, 3200)),
            ("PAC002", "Dual Chamber Pacemaker", (2400, 4800)),
            ("PAC003", "Pacemaker Lead", (600, 1200)),
        ],
        "cardiac_device": [
            ("ICD001", "Implantable Cardioverter Defibrillator", (6000, 14000)),
            ("ICD002", "ICD Lead", (1200, 2400)),
        ],
        "cardiac_stent": [
            ("STN001", "Drug Eluting Stent", (800, 2000)),
            ("STN002", "Bare Metal Stent", (400, 1000)),
            ("STN003", "Coronary Stent - Bioresorbable", (1200, 2800)),
        ],
        "cardiac_valve": [
            ("VAL001", "Mechanical Heart Valve", (3200, 6000)),
            ("VAL002", "Bioprosthetic Heart Valve", (4000, 8000)),
            ("VAL003", "TAVR Valve", (10000, 16000)),
        ],
        "cochlear_implant": [
            ("COC001", "Cochlear Implant System", (8000, 14000)),
            ("COC002", "Cochlear Implant Processor", (3200, 6000)),
        ],
        "hernia_mesh": [
            ("HRN001", "Hernia Mesh - Synthetic", (200, 600)),
            ("HRN002", "Hernia Mesh - Biological", (600, 1600)),
        ],
        "lens_implant": [
            ("LNS001", "Intraocular Lens - Monofocal", (120, 320)),
            ("LNS002", "Intraocular Lens - Multifocal", (400, 1000)),
            ("LNS003", "Intraocular Lens - Toric", (320, 800)),
        ],
    }

    # Common MBS items by provider type and clinical category
    # (mbs_item_number, description, schedule_fee_range, typical_charge_multiplier)
    # Fee ranges calibrated to ~40% of original to match PHI fund benefit portions
    MBS_ITEMS_BY_PROVIDER = {
        "Surgeon": [
            ("30001", "Initial consultation", (35, 60), 1.5),
            ("30003", "Subsequent consultation", (18, 35), 1.5),
            ("30571", "Surgical procedure - minor", (80, 200), 2.0),
            ("30572", "Surgical procedure - intermediate", (200, 480), 2.0),
            ("30573", "Surgical procedure - major", (480, 1200), 2.5),
            ("35503", "Orthopaedic procedure", (600, 1600), 2.5),
            ("35506", "Joint procedure - major", (800, 2000), 2.5),
            ("37800", "Abdominal surgery", (400, 1200), 2.0),
            ("38200", "Cardiac surgery", (1200, 3200), 2.5),
        ],
        "Anesthetist": [
            ("20100", "Anaesthesia - basic", (60, 120), 1.3),
            ("20110", "Anaesthesia - intermediate", (120, 240), 1.3),
            ("20120", "Anaesthesia - complex", (240, 480), 1.5),
            ("20200", "Epidural anaesthesia", (160, 320), 1.5),
            ("20500", "Post-operative pain management", (40, 100), 1.3),
        ],
        "Assistant": [
            ("51300", "Surgical assistant - minor", (40, 100), 1.2),
            ("51303", "Surgical assistant - intermediate", (80, 160), 1.2),
            ("51306", "Surgical assistant - major", (160, 320), 1.3),
        ],
        "Physician": [
            ("104", "Initial consultation - physician", (60, 120), 1.5),
            ("105", "Subsequent consultation", (30, 60), 1.5),
            ("116", "Specialist consultation", (80, 160), 1.8),
            ("132", "Emergency consultation", (100, 200), 2.0),
        ],
        "Pathology": [
            ("65070", "Blood tests - basic panel", (12, 32), 1.0),
            ("65120", "Blood tests - comprehensive", (32, 80), 1.0),
            ("73525", "Histopathology", (40, 120), 1.0),
        ],
        "Radiology": [
            ("57506", "X-ray", (20, 60), 1.2),
            ("57700", "CT scan", (80, 240), 1.3),
            ("63001", "MRI scan", (120, 320), 1.3),
            ("57960", "Ultrasound", (32, 80), 1.2),
        ],
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
            provider_id=self.uniform_int(1, 1000),
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

        # Extras claim detail
        extras_claim = ExtrasClaimCreate(
            extras_claim_id=extras_claim_id,
            claim_id=claim_id,
            claim_line_id=claim_line_id,
            service_type=service_type,
            dental_service_type=dental_service_type,
            extras_item_id=self.uniform_int(1, 500),
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
            hospital_id=self.uniform_int(1, 200),
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
    ) -> ClaimCreate:
        """
        Generate a rejected claim with a specific denial reason.

        Args:
            policy: Policy
            member: Claiming member
            claim_type: Type of claim (EXTRAS, HOSPITAL, AMBULANCE)
            service_date: Date of attempted service
            denial_reason: Reason for denial (DenialReason enum)

        Returns:
            ClaimCreate instance with REJECTED status
        """
        claim_id = self.id_generator.generate_uuid()

        # Sample a charge amount based on claim type
        if claim_type == ClaimType.EXTRAS:
            service_type = self.propensity.sample_extras_service_type()
            charge_amount = Decimal(str(round(
                self.propensity.sample_claim_amount(service_type), 2
            )))
            claim_channel = ClaimChannel.HICAPS
        elif claim_type == ClaimType.HOSPITAL:
            charge_amount = Decimal(str(round(
                self.propensity.sample_claim_amount("Hospital", kwargs.get("age", 40)), 2
            )))
            claim_channel = ClaimChannel.HOSPITAL
        else:  # AMBULANCE
            charge_amount = Decimal(str(round(
                self.propensity.sample_claim_amount("Ambulance"), 2
            )))
            claim_channel = ClaimChannel.PAPER

        return ClaimCreate(
            claim_id=claim_id,
            claim_number=self.id_generator.generate_claim_number(),
            policy_id=policy.policy_id,
            member_id=member.member_id,
            coverage_id=NO_COVERAGE_PLACEHOLDER_ID,  # Placeholder for rejected claims
            claim_type=claim_type,
            claim_status=ClaimStatus.REJECTED,
            service_date=service_date,
            lodgement_date=service_date,
            assessment_date=service_date,
            payment_date=None,  # No payment for rejected claims
            provider_id=self.uniform_int(1, 1000) if claim_type == ClaimType.EXTRAS else None,
            hospital_id=self.uniform_int(1, 200) if claim_type == ClaimType.HOSPITAL else None,
            total_charge=charge_amount,
            total_benefit=Decimal("0"),  # No benefit for rejected claims
            total_gap=charge_amount,  # Member pays full amount (or doesn't proceed)
            excess_applied=Decimal("0"),
            co_payment_applied=Decimal("0"),
            rejection_reason_id=self._get_rejection_reason_id(denial_reason),
            rejection_notes=denial_reason.value,
            claim_channel=claim_channel,
            pay_to="N/A",  # No payment for rejected claims
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

    def _get_rejection_reason_id(self, denial_reason: DenialReason) -> int:
        """
        Get the rejection reason ID for a denial reason.

        Args:
            denial_reason: DenialReason enum value

        Returns:
            Integer ID matching claim_rejection_reason.json
        """
        return self.DENIAL_REASON_IDS.get(denial_reason, 1)

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
                # Normalize weights
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

        probability, prosthesis_types = self.PROSTHESIS_PROCEDURES[drg_prefix]

        # Check if prosthesis is used in this case
        if self.rng.random() > probability:
            return prosthesis_claims, total_charge

        # Select prosthesis type
        prosthesis_type = self.choice(prosthesis_types)

        if prosthesis_type not in self.PROSTHESIS_CATALOG:
            return prosthesis_claims, total_charge

        # Get items for this prosthesis type
        items = self.PROSTHESIS_CATALOG[prosthesis_type]

        # Usually 1-2 items per procedure (e.g., implant + lead for pacemaker)
        num_items = 1 if len(items) == 1 else self.uniform_int(1, min(2, len(items)))
        
        # Select random indices to pick items (avoid numpy array issues with tuples)
        indices = self.rng.choice(len(items), size=num_items, replace=False)
        selected_items = [items[i] for i in indices]

        for idx, (billing_code, description, cost_range) in enumerate(selected_items):
            # Generate charge within range
            min_cost, max_cost = cost_range
            charge_amount = Decimal(str(round(self.rng.uniform(min_cost, max_cost), 2)))

            # Prosthesis benefit is typically the full charge (no-gap arrangement)
            benefit_amount = charge_amount
            gap_amount = Decimal("0")

            prosthesis_claim = ProsthesisClaimCreate(
                prosthesis_claim_id=self.id_generator.generate_uuid(),
                claim_id=claim_id,
                admission_id=admission_id,
                prosthesis_item_id=self.uniform_int(1000, 9999),
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
            mbs_items = self.MBS_ITEMS_BY_PROVIDER.get(provider_type, [])
            if not mbs_items:
                continue

            # Select 1-3 items per provider type
            num_items = self.uniform_int(1, min(3, len(mbs_items)))
            indices = self.rng.choice(len(mbs_items), size=num_items, replace=False)
            selected_items = [mbs_items[i] for i in indices]

            for mbs_item_number, description, fee_range, charge_multiplier in selected_items:
                min_fee, max_fee = fee_range
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

                medical_service = MedicalServiceCreate(
                    medical_service_id=self.id_generator.generate_uuid(),
                    claim_id=claim_id,
                    admission_id=admission_id,
                    mbs_item_number=mbs_item_number,
                    mbs_item_description=description,
                    mbs_schedule_fee=schedule_fee,
                    provider_id=self.uniform_int(1, 5000),
                    provider_type=provider_type,
                    provider_number=f"PRV{self.uniform_int(100000, 999999)}",
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
