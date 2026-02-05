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
    # Communication Domain Enums
    CommunicationType,
    CommunicationDeliveryStatus,
    PreferenceType,
    CampaignType,
    CampaignStatus,
    CampaignResponseType,
    ConversionType,
    # Digital Behavior Domain Enums
    DeviceType,
    SessionType,
    DigitalEventType,
    PageCategory,
    TriggerEventType,
    # Survey Domain Enums
    SurveyType,
    NPSCategory,
    CSATLabel,
    SentimentLabel,
    SurveyChannel,
    ProcessingStatus,
)
from brickwell_health.domain.nba import (
    # NBA Domain Enums
    BusinessIssue,
    ActionCategory,
    NBAChannel,
    RecommendationStatus,
    ImmediateResponse,
    ExecutionMethod,
    # NBA Domain Models
    NBAActionCatalogCreate,
    NBAActionCatalog,
    NBARecommendationCreate,
    NBARecommendation,
    NBAExecutionCreate,
    NBAExecution,
    NBAActionWithRecommendation,
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
from brickwell_health.domain.crm import (
    InteractionCreate,
    Interaction,
    CaseCreate,
    Case,
    ComplaintCreate,
    Complaint,
)
from brickwell_health.domain.communication import (
    CommunicationPreferenceCreate,
    CommunicationPreference,
    CampaignCreate,
    Campaign,
    CommunicationCreate,
    Communication,
    CampaignResponseCreate,
    CampaignResponse,
)
from brickwell_health.domain.digital import (
    WebSessionCreate,
    WebSession,
    DigitalEventCreate,
    DigitalEvent,
)
from brickwell_health.domain.survey import (
    NPSSurveyPendingCreate,
    NPSSurveyPending,
    NPSSurveyCreate,
    NPSSurvey,
    CSATSurveyPendingCreate,
    CSATSurveyPending,
    CSATSurveyCreate,
    CSATSurvey,
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
    # Communication Domain Enums
    "CommunicationType",
    "CommunicationDeliveryStatus",
    "PreferenceType",
    "CampaignType",
    "CampaignStatus",
    "CampaignResponseType",
    "ConversionType",
    # Digital Behavior Domain Enums
    "DeviceType",
    "SessionType",
    "DigitalEventType",
    "PageCategory",
    "TriggerEventType",
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
    # CRM
    "InteractionCreate",
    "Interaction",
    "CaseCreate",
    "Case",
    "ComplaintCreate",
    "Complaint",
    # Communication
    "CommunicationPreferenceCreate",
    "CommunicationPreference",
    "CampaignCreate",
    "Campaign",
    "CommunicationCreate",
    "Communication",
    "CampaignResponseCreate",
    "CampaignResponse",
    # Digital Behavior
    "WebSessionCreate",
    "WebSession",
    "DigitalEventCreate",
    "DigitalEvent",
    # Survey Domain Enums
    "SurveyType",
    "NPSCategory",
    "CSATLabel",
    "SentimentLabel",
    "SurveyChannel",
    "ProcessingStatus",
    # Survey Domain Models
    "NPSSurveyPendingCreate",
    "NPSSurveyPending",
    "NPSSurveyCreate",
    "NPSSurvey",
    "CSATSurveyPendingCreate",
    "CSATSurveyPending",
    "CSATSurveyCreate",
    "CSATSurvey",
    # NBA Domain Enums
    "BusinessIssue",
    "ActionCategory",
    "NBAChannel",
    "RecommendationStatus",
    "ImmediateResponse",
    "ExecutionMethod",
    # NBA Domain Models
    "NBAActionCatalogCreate",
    "NBAActionCatalog",
    "NBARecommendationCreate",
    "NBARecommendation",
    "NBAExecutionCreate",
    "NBAExecution",
    "NBAActionWithRecommendation",
]
