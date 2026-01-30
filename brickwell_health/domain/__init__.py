"""
Domain models for Brickwell Health Simulator.

Pydantic models representing the core business entities.
"""

from brickwell_health.domain.enums import (
    PolicyStatus,
    PolicyType,
    MemberRole,
    RelationshipType,
    CoverageType,
    CoverageTier,
    ClaimType,
    ClaimStatus,
    WaitingPeriodType,
    WaitingPeriodStatus,
    ApplicationStatus,
    ApplicationType,
    DistributionChannel,
    PaymentMethod,
    PaymentStatus,
    InvoiceStatus,
    SuspensionType,
    SuspensionStatus,
    Gender,
    MaritalStatus,
    MemberChangeType,
)
from brickwell_health.domain.member import MemberCreate, Member, MemberUpdate
from brickwell_health.domain.policy import PolicyCreate, Policy, PolicyMemberCreate
from brickwell_health.domain.application import (
    ApplicationCreate,
    Application,
    ApplicationMemberCreate,
)
from brickwell_health.domain.coverage import CoverageCreate, Coverage, WaitingPeriodCreate
from brickwell_health.domain.claims import (
    ClaimCreate,
    Claim,
    ClaimLineCreate,
    HospitalAdmissionCreate,
    ExtrasClaimCreate,
    AmbulanceClaimCreate,
)
from brickwell_health.domain.billing import (
    InvoiceCreate,
    Invoice,
    PaymentCreate,
    DirectDebitMandateCreate,
    DirectDebitResultCreate,
)

__all__ = [
    # Enums
    "PolicyStatus",
    "PolicyType",
    "MemberRole",
    "RelationshipType",
    "CoverageType",
    "CoverageTier",
    "ClaimType",
    "ClaimStatus",
    "WaitingPeriodType",
    "WaitingPeriodStatus",
    "ApplicationStatus",
    "ApplicationType",
    "DistributionChannel",
    "PaymentMethod",
    "PaymentStatus",
    "InvoiceStatus",
    "SuspensionType",
    "SuspensionStatus",
    "Gender",
    "MaritalStatus",
    "MemberChangeType",
    # Member
    "MemberCreate",
    "Member",
    "MemberUpdate",
    # Policy
    "PolicyCreate",
    "Policy",
    "PolicyMemberCreate",
    # Application
    "ApplicationCreate",
    "Application",
    "ApplicationMemberCreate",
    # Coverage
    "CoverageCreate",
    "Coverage",
    "WaitingPeriodCreate",
    # Claims
    "ClaimCreate",
    "Claim",
    "ClaimLineCreate",
    "HospitalAdmissionCreate",
    "ExtrasClaimCreate",
    "AmbulanceClaimCreate",
    # Billing
    "InvoiceCreate",
    "Invoice",
    "PaymentCreate",
    "DirectDebitMandateCreate",
    "DirectDebitResultCreate",
]
