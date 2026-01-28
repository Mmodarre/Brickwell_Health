"""
Claims domain models for Brickwell Health Simulator.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from brickwell_health.domain.enums import (
    ClaimType,
    ClaimStatus,
    ClaimChannel,
    AdmissionType,
    AccommodationType,
    DentalServiceType,
)


class ClaimCreate(BaseModel):
    """Model for creating a claim header."""

    claim_id: UUID
    claim_number: str = Field(..., max_length=20)

    policy_id: UUID
    member_id: UUID
    coverage_id: UUID  # Uses placeholder ID for rejected claims without coverage

    claim_type: ClaimType
    claim_status: ClaimStatus = ClaimStatus.SUBMITTED

    service_date: date
    lodgement_date: date
    assessment_date: Optional[date] = None
    payment_date: Optional[date] = None

    provider_id: Optional[int] = None
    hospital_id: Optional[int] = None

    total_charge: Decimal = Field(..., ge=0)
    total_benefit: Optional[Decimal] = None
    total_gap: Optional[Decimal] = None

    excess_applied: Decimal = Field(default=Decimal("0"))
    co_payment_applied: Decimal = Field(default=Decimal("0"))

    rejection_reason_id: Optional[int] = None
    rejection_notes: Optional[str] = None

    claim_channel: ClaimChannel
    pay_to: str = Field(default="Member", max_length=20)

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["claim_type"] = data["claim_type"].value if isinstance(data["claim_type"], ClaimType) else data["claim_type"]
        data["claim_status"] = data["claim_status"].value if isinstance(data["claim_status"], ClaimStatus) else data["claim_status"]
        data["claim_channel"] = data["claim_channel"].value if isinstance(data["claim_channel"], ClaimChannel) else data["claim_channel"]
        return data


class Claim(ClaimCreate):
    """Full claim model with audit fields."""

    modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None

    class Config:
        from_attributes = True


class ClaimLineCreate(BaseModel):
    """Model for creating a claim line item."""

    claim_line_id: UUID
    claim_id: UUID

    line_number: int = Field(..., ge=1)

    item_code: str = Field(..., max_length=20)
    item_description: Optional[str] = Field(None, max_length=500)
    clinical_category_id: Optional[int] = None
    benefit_category_id: Optional[int] = None

    service_date: date
    quantity: int = Field(default=1, ge=1)

    charge_amount: Decimal = Field(..., ge=0)
    schedule_fee: Optional[Decimal] = None
    benefit_amount: Optional[Decimal] = None
    gap_amount: Optional[Decimal] = None

    line_status: str = Field(default="Pending", max_length=20)
    rejection_reason_id: Optional[int] = None

    provider_id: Optional[int] = None
    provider_number: Optional[str] = Field(None, max_length=20)

    tooth_number: Optional[str] = Field(None, max_length=10)
    body_part: Optional[str] = Field(None, max_length=50)

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class HospitalAdmissionCreate(BaseModel):
    """Model for creating a hospital admission record."""

    admission_id: UUID
    claim_id: UUID

    hospital_id: int
    admission_number: Optional[str] = Field(None, max_length=30)
    admission_date: date
    discharge_date: Optional[date] = None

    admission_type: AdmissionType
    accommodation_type: AccommodationType

    drg_code: Optional[str] = Field(None, max_length=10)
    clinical_category_id: int
    principal_diagnosis: Optional[str] = Field(None, max_length=10)
    principal_procedure: Optional[str] = Field(None, max_length=10)

    length_of_stay: Optional[int] = Field(None, ge=0)
    theatre_minutes: Optional[int] = Field(None, ge=0)

    accommodation_charge: Optional[Decimal] = None
    theatre_charge: Optional[Decimal] = None
    prosthesis_charge: Optional[Decimal] = None
    other_charges: Optional[Decimal] = None

    accommodation_benefit: Optional[Decimal] = None
    theatre_benefit: Optional[Decimal] = None

    excess_applicable: bool = True
    excess_amount: Decimal = Field(default=Decimal("0"))
    co_payment_amount: Decimal = Field(default=Decimal("0"))

    contracted_hospital: bool = True
    informed_financial_consent: bool = True

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        data["admission_type"] = data["admission_type"].value if isinstance(data["admission_type"], AdmissionType) else data["admission_type"]
        data["accommodation_type"] = data["accommodation_type"].value if isinstance(data["accommodation_type"], AccommodationType) else data["accommodation_type"]
        return data


class ExtrasClaimCreate(BaseModel):
    """Model for creating an extras claim detail."""

    extras_claim_id: UUID
    claim_id: UUID
    claim_line_id: UUID

    service_type: str = Field(..., max_length=50)
    dental_service_type: Optional[DentalServiceType] = Field(
        None,
        description="Sub-category for dental claims (Preventative/General/Major)",
    )
    extras_item_id: int

    provider_id: int
    provider_location_id: Optional[int] = None

    service_date: date
    tooth_number: Optional[str] = Field(None, max_length=10)

    charge_amount: Decimal = Field(..., ge=0)
    benefit_amount: Optional[Decimal] = None
    annual_limit_impact: Optional[Decimal] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)

    def model_dump_db(self) -> dict:
        """Convert to dictionary for database insertion."""
        data = self.model_dump()
        if data["dental_service_type"] is not None:
            data["dental_service_type"] = data["dental_service_type"].value if isinstance(data["dental_service_type"], DentalServiceType) else data["dental_service_type"]
        return data


class AmbulanceClaimCreate(BaseModel):
    """Model for creating an ambulance claim."""

    ambulance_claim_id: UUID
    claim_id: UUID

    incident_date: date
    incident_location: Optional[str] = Field(None, max_length=200)
    incident_state: str = Field(..., max_length=3)

    transport_type: str = Field(..., max_length=30)
    pickup_location: Optional[str] = Field(None, max_length=200)
    destination: Optional[str] = Field(None, max_length=200)
    distance_km: Optional[Decimal] = None

    charge_amount: Decimal = Field(..., ge=0)
    benefit_amount: Optional[Decimal] = None
    state_scheme_contribution: Optional[Decimal] = None

    ambulance_provider: Optional[str] = Field(None, max_length=100)
    case_number: Optional[str] = Field(None, max_length=30)

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class ProsthesisClaimCreate(BaseModel):
    """Model for creating a prosthesis claim item."""

    prosthesis_claim_id: UUID
    claim_id: UUID
    admission_id: UUID

    prosthesis_item_id: int
    billing_code: str = Field(..., max_length=20)
    item_description: Optional[str] = Field(None, max_length=200)

    quantity: int = Field(default=1, ge=1)

    charge_amount: Decimal = Field(..., ge=0)
    benefit_amount: Optional[Decimal] = None
    gap_amount: Optional[Decimal] = None

    implant_date: date

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class ClaimAssessmentCreate(BaseModel):
    """Model for creating a claim assessment record."""

    assessment_id: UUID
    claim_id: UUID

    assessment_type: str = Field(..., max_length=30)  # Auto/Manual/Review
    assessment_date: datetime
    assessed_by: str = Field(..., max_length=50)

    original_benefit: Optional[Decimal] = None
    adjusted_benefit: Optional[Decimal] = None
    adjustment_reason: Optional[str] = None

    waiting_period_check: Optional[bool] = None
    benefit_limit_check: Optional[bool] = None
    eligibility_check: Optional[bool] = None

    outcome: str = Field(..., max_length=20)  # Approved/Rejected/PartiallyApproved
    notes: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)


class MedicalServiceCreate(BaseModel):
    """
    Model for tracking individual MBS items billed by doctors.

    Used for hospital claims to track medical services provided by
    treating doctors, anesthetists, assistants, and other specialists.

    Each medical service corresponds to an MBS (Medicare Benefits Schedule)
    item that was billed as part of a hospital admission.
    """

    medical_service_id: UUID
    claim_id: UUID
    admission_id: UUID

    # MBS item details
    mbs_item_number: str = Field(..., max_length=10)
    mbs_item_description: Optional[str] = Field(None, max_length=500)
    mbs_schedule_fee: Optional[Decimal] = None

    # Provider details
    provider_id: int
    provider_type: str = Field(..., max_length=30)  # Surgeon/Anesthetist/Assistant/Physician
    provider_number: Optional[str] = Field(None, max_length=20)

    # Service details
    service_date: date
    service_text: Optional[str] = Field(None, max_length=200)

    # Financial details
    charge_amount: Decimal = Field(..., ge=0)
    medicare_benefit: Optional[Decimal] = None  # Medicare's contribution (75% of MBS fee)
    fund_benefit: Optional[Decimal] = None  # PHI fund's contribution (25% gap cover)
    gap_amount: Optional[Decimal] = None  # Amount patient pays

    # Medical gap cover scheme
    no_gap_indicator: bool = False  # True if doctor accepts known gap
    gap_cover_scheme: Optional[str] = Field(None, max_length=50)  # AccessGap/NoGap/etc.

    # Clinical details
    clinical_category_id: Optional[int] = None
    body_part: Optional[str] = Field(None, max_length=50)
    procedure_laterality: Optional[str] = Field(None, max_length=10)  # Left/Right/Bilateral

    # Multiple service rule
    multiple_service_rule_applied: bool = False
    multiple_service_percentage: Optional[int] = None  # 100/50/25 etc.

    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(default="SIMULATION", max_length=50)
