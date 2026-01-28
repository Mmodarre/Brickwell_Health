"""
Enumeration types for Brickwell Health Simulator domain models.
"""

from enum import Enum


class Gender(str, Enum):
    """Gender enumeration."""
    MALE = "Male"
    FEMALE = "Female"
    OTHER = "Other"
    UNKNOWN = "Unknown"


class PolicyStatus(str, Enum):
    """Policy status enumeration."""
    ACTIVE = "Active"
    SUSPENDED = "Suspended"
    CANCELLED = "Cancelled"
    LAPSED = "Lapsed"


class CancellationReason(str, Enum):
    """
    Reason for policy cancellation.

    Used by the churn prediction model to track why policies are cancelled.
    Weighted sampling based on policy conditions.
    """
    PRICE = "Price"              # Premium too expensive
    NO_VALUE = "NoValue"         # Not using coverage, low perceived value
    SWITCHING = "Switching"      # Moving to competitor insurer
    LIFE_EVENT = "LifeEvent"     # Job loss, divorce, major income change
    DECEASED = "Deceased"        # Primary member deceased
    OTHER = "Other"              # Other reasons


class PolicyType(str, Enum):
    """Policy type enumeration."""
    SINGLE = "Single"
    COUPLE = "Couple"
    FAMILY = "Family"
    SINGLE_PARENT = "Single Parent"  # Match reference data format


class MemberRole(str, Enum):
    """Member role on a policy."""
    PRIMARY = "Primary"
    PARTNER = "Partner"
    DEPENDENT = "Dependent"


class RelationshipType(str, Enum):
    """Relationship to primary member."""
    SELF = "Self"
    SPOUSE = "Spouse"
    CHILD = "Child"
    OTHER = "Other"


class CoverageType(str, Enum):
    """Type of coverage."""
    HOSPITAL = "Hospital"
    EXTRAS = "Extras"
    AMBULANCE = "Ambulance"


class CoverageTier(str, Enum):
    """Hospital coverage tier."""
    GOLD = "Gold"
    SILVER = "Silver"
    BRONZE = "Bronze"
    BASIC = "Basic"


class ClaimType(str, Enum):
    """Type of claim."""
    HOSPITAL = "Hospital"
    EXTRAS = "Extras"
    AMBULANCE = "Ambulance"


class ClaimStatus(str, Enum):
    """Claim processing status."""
    SUBMITTED = "Submitted"
    ASSESSED = "Assessed"
    APPROVED = "Approved"
    REJECTED = "Rejected"
    PAID = "Paid"
    QUERIED = "Queried"


class DenialReason(str, Enum):
    """
    Claim denial reason codes.

    Deterministic reasons are computed from business rules.
    Stochastic reasons are randomly sampled when stochastic denial occurs.
    """
    # Deterministic reasons (computed from business rules)
    NO_COVERAGE = "NoCoverage"                  # No coverage on policy for this service type
    LIMITS_EXHAUSTED = "LimitsExhausted"        # Annual limits reached (extras only)
    WAITING_PERIOD = "WaitingPeriod"            # Waiting period not satisfied
    MEMBERSHIP_INACTIVE = "MembershipInactive"  # Policy suspended or lapsed

    # Stochastic reasons (randomly sampled)
    POLICY_EXCLUSIONS = "PolicyExclusions"      # Service excluded under policy terms
    PRE_EXISTING = "PreExisting"                # Pre-existing condition exclusion (hospital only)
    PROVIDER_ISSUES = "ProviderIssues"          # Provider not registered or billing issue
    ADMINISTRATIVE = "Administrative"           # Administrative or documentation issue


class WaitingPeriodType(str, Enum):
    """Type of waiting period."""
    GENERAL = "General"
    PRE_EXISTING = "Pre-existing"
    OBSTETRIC = "Obstetric"
    PSYCHIATRIC = "Psychiatric"


class WaitingPeriodStatus(str, Enum):
    """Waiting period status."""
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    WAIVED = "Waived"


class ApplicationStatus(str, Enum):
    """Application processing status."""
    PENDING = "Pending"
    APPROVED = "Approved"
    DECLINED = "Declined"
    WITHDRAWN = "Withdrawn"


class ApplicationType(str, Enum):
    """Type of application."""
    NEW = "New"
    UPGRADE = "Upgrade"
    DOWNGRADE = "Downgrade"
    TRANSFER = "Transfer"


class DistributionChannel(str, Enum):
    """Sales/distribution channel."""
    ONLINE = "Online"
    PHONE = "Phone"
    BROKER = "Broker"
    CORPORATE = "Corporate"
    BRANCH = "Branch"
    COMPARISON = "Comparison"


class PaymentMethod(str, Enum):
    """Payment method."""
    DIRECT_DEBIT = "DirectDebit"
    BPAY = "BPay"
    CARD = "Card"
    EFT = "EFT"
    CASH = "Cash"
    CHEQUE = "Cheque"


class PaymentStatus(str, Enum):
    """Payment status."""
    PENDING = "Pending"
    COMPLETED = "Completed"
    FAILED = "Failed"
    REVERSED = "Reversed"


class InvoiceStatus(str, Enum):
    """Invoice status."""
    ISSUED = "Issued"
    PAID = "Paid"
    PARTIALLY_PAID = "PartiallyPaid"
    OVERDUE = "Overdue"
    CANCELLED = "Cancelled"


class SuspensionType(str, Enum):
    """Type of policy suspension."""
    FINANCIAL_HARDSHIP = "Financial Hardship"
    OVERSEAS_TRAVEL = "Overseas Travel"
    OTHER = "Other"


class SuspensionStatus(str, Enum):
    """Suspension status."""
    ACTIVE = "Active"
    ENDED = "Ended"
    EXTENDED = "Extended"


class AdmissionType(str, Enum):
    """Hospital admission type."""
    ELECTIVE = "Elective"
    EMERGENCY = "Emergency"
    MATERNITY = "Maternity"


class AccommodationType(str, Enum):
    """Hospital accommodation type."""
    PRIVATE_ROOM = "PrivateRoom"
    SHARED_ROOM = "SharedRoom"
    DAY_SURGERY = "DaySurgery"
    ICU = "ICU"


class ClaimChannel(str, Enum):
    """Claim submission channel."""
    ONLINE = "Online"
    HICAPS = "HICAPS"
    PAPER = "Paper"
    HOSPITAL = "Hospital"


class DentalServiceType(str, Enum):
    """Dental service sub-category for extras claims."""
    PREVENTATIVE = "Preventative"  # Check-ups, cleans, x-rays
    GENERAL = "General"            # Fillings, extractions
    MAJOR = "Major"                # Crowns, root canals, implants


class RebateTier(str, Enum):
    """Government rebate income tier."""
    TIER_0 = "Tier 0"
    TIER_1 = "Tier 1"
    TIER_2 = "Tier 2"
    TIER_3 = "Tier 3"


class AgeBracket(str, Enum):
    """Age bracket for rebate calculation."""
    UNDER_65 = "Under 65"
    AGE_65_TO_69 = "65-69"
    AGE_70_PLUS = "70+"
