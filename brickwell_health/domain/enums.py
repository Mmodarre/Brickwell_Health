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


class MaritalStatus(str, Enum):
    """Marital status for members."""
    SINGLE = "Single"
    MARRIED = "Married"
    DE_FACTO = "DeFacto"
    DIVORCED = "Divorced"
    SEPARATED = "Separated"
    WIDOWED = "Widowed"


class MemberChangeType(str, Enum):
    """Type of member change event."""
    ADDRESS_CHANGE = "AddressChange"
    PHONE_CHANGE = "PhoneChange"
    EMAIL_CHANGE = "EmailChange"
    NAME_CHANGE = "NameChange"
    MARITAL_STATUS_CHANGE = "MaritalStatusChange"
    MEDICARE_RENEWAL = "MedicareRenewal"
    PREFERRED_NAME_UPDATE = "PreferredNameUpdate"
    DEATH = "Death"


# ============================================================================
# CRM DOMAIN ENUMS
# ============================================================================

class InteractionChannel(str, Enum):
    """Channel through which member interacts with insurer."""
    PHONE = "Phone"
    EMAIL = "Email"
    CHAT = "Chat"
    BRANCH = "Branch"
    IN_APP = "InApp"


class InteractionDirection(str, Enum):
    """Direction of the interaction."""
    INBOUND = "Inbound"
    OUTBOUND = "Outbound"


class CasePriority(str, Enum):
    """Priority level for service cases."""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    CRITICAL = "Critical"


class CaseStatus(str, Enum):
    """Status of a service case."""
    OPEN = "Open"
    IN_PROGRESS = "InProgress"
    PENDING_MEMBER = "PendingMember"
    PENDING_PROVIDER = "PendingProvider"
    RESOLVED = "Resolved"
    CLOSED = "Closed"


class ComplaintSeverity(str, Enum):
    """Severity level for complaints."""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    CRITICAL = "Critical"


class ComplaintStatus(str, Enum):
    """Status of a complaint."""
    RECEIVED = "Received"
    ACKNOWLEDGED = "Acknowledged"
    INVESTIGATING = "Investigating"
    RESOLVED = "Resolved"
    ESCALATED = "Escalated"
    CLOSED = "Closed"


class ComplaintSource(str, Enum):
    """Source of the complaint."""
    PHONE = "Phone"
    EMAIL = "Email"
    LETTER = "Letter"
    PHIO = "PHIO"
    SOCIAL = "Social"
    IN_APP = "InApp"


class ComplaintResolutionOutcome(str, Enum):
    """Outcome of complaint resolution."""
    UPHELD = "Upheld"
    NOT_UPHELD = "NotUpheld"
    PARTIALLY_UPHELD = "PartiallyUpheld"
    WITHDRAWN = "Withdrawn"


# ============================================================================
# COMMUNICATION DOMAIN ENUMS
# ============================================================================

class CommunicationType(str, Enum):
    """Type/channel of outbound communication."""
    LETTER = "Letter"
    EMAIL = "Email"
    SMS = "SMS"
    PUSH = "Push"
    IN_APP = "InApp"


class CommunicationDeliveryStatus(str, Enum):
    """Delivery status of a communication."""
    PENDING = "Pending"
    SENT = "Sent"
    DELIVERED = "Delivered"
    FAILED = "Failed"
    BOUNCED = "Bounced"


class PreferenceType(str, Enum):
    """Type of communication preference."""
    TRANSACTIONAL = "Transactional"
    MARKETING = "Marketing"
    CLAIMS = "Claims"


class CampaignType(str, Enum):
    """Type of marketing campaign."""
    RETENTION = "Retention"
    ACQUISITION = "Acquisition"
    CROSS_SELL = "CrossSell"
    UPSELL = "Upsell"
    WINBACK = "Winback"
    ENGAGEMENT = "Engagement"


class CampaignStatus(str, Enum):
    """Status of a campaign."""
    DRAFT = "Draft"
    ACTIVE = "Active"
    PAUSED = "Paused"
    COMPLETED = "Completed"
    CANCELLED = "Cancelled"


class CampaignResponseType(str, Enum):
    """Type of response to a campaign."""
    OPENED = "Opened"
    CLICKED = "Clicked"
    CONVERTED = "Converted"
    UNSUBSCRIBED = "Unsubscribed"


class ConversionType(str, Enum):
    """Type of conversion from campaign."""
    RENEWED = "Renewed"
    UPGRADED = "Upgraded"
    ADDED_COVER = "AddedCover"
    REFERRED = "Referred"


# ============================================================================
# DIGITAL BEHAVIOR DOMAIN ENUMS
# ============================================================================

class DeviceType(str, Enum):
    """Type of device used for digital session."""
    DESKTOP = "Desktop"
    MOBILE = "Mobile"
    TABLET = "Tablet"


class SessionType(str, Enum):
    """Type of digital session."""
    WEB = "Web"
    APP = "App"


class DigitalEventType(str, Enum):
    """Type of digital event."""
    PAGE_VIEW = "PageView"
    CLICK = "Click"
    SEARCH = "Search"
    FORM_START = "FormStart"
    FORM_SUBMIT = "FormSubmit"
    DOWNLOAD = "Download"


class PageCategory(str, Enum):
    """Category of page visited."""
    HOME = "Home"
    DASHBOARD = "Dashboard"
    CLAIMS = "Claims"
    BILLING = "Billing"
    PRODUCTS = "Products"
    SUPPORT = "Support"
    FAQ = "FAQ"
    ACCOUNT = "Account"
    CANCEL = "Cancel"
    UPGRADE = "Upgrade"
    COMPARE = "Compare"


# ============================================================================
# SURVEY DOMAIN ENUMS
# ============================================================================

class SurveyType(str, Enum):
    """Type of survey sent."""
    POST_CLAIM = "PostClaim"
    POST_INTERACTION = "PostInteraction"
    POST_COMPLAINT_RESOLUTION = "PostComplaintResolution"
    ANNUAL = "Annual"
    RELATIONSHIP = "Relationship"


class NPSCategory(str, Enum):
    """NPS score category."""
    PROMOTER = "Promoter"
    PASSIVE = "Passive"
    DETRACTOR = "Detractor"


class CSATLabel(str, Enum):
    """CSAT score label."""
    VERY_SATISFIED = "VerySatisfied"
    SATISFIED = "Satisfied"
    NEUTRAL = "Neutral"
    DISSATISFIED = "Dissatisfied"
    VERY_DISSATISFIED = "VeryDissatisfied"


class SentimentLabel(str, Enum):
    """Sentiment analysis label."""
    POSITIVE = "Positive"
    NEUTRAL = "Neutral"
    NEGATIVE = "Negative"


class SurveyChannel(str, Enum):
    """Channel through which survey was delivered."""
    EMAIL = "Email"
    SMS = "SMS"
    IN_APP = "InApp"


class ProcessingStatus(str, Enum):
    """Status of deferred LLM processing."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


# ============================================================================
# TRIGGER EVENT TYPES
# ============================================================================

class TriggerEventType(str, Enum):
    """Types of events that trigger CRM actions."""
    # Claims triggers
    CLAIM_SUBMITTED = "ClaimSubmitted"
    CLAIM_REJECTED = "ClaimRejected"
    CLAIM_DELAYED = "ClaimDelayed"
    CLAIM_PAID = "ClaimPaid"
    CLAIM_LIMIT_EXHAUSTED = "ClaimLimitExhausted"
    
    # Billing triggers
    INVOICE_ISSUED = "InvoiceIssued"
    PAYMENT_DUE = "PaymentDue"
    PAYMENT_RECEIVED = "PaymentReceived"
    PAYMENT_FAILED = "PaymentFailed"
    ARREARS_CREATED = "ArrearsCreated"
    POLICY_SUSPENDED = "PolicySuspended"
    POLICY_LAPSED = "PolicyLapsed"
    
    # Policy triggers
    RENEWAL_REMINDER = "RenewalReminder"
    POLICY_ANNIVERSARY = "PolicyAnniversary"
    
    # CRM triggers
    INTERACTION_COMPLETED = "InteractionCompleted"
    CASE_RESOLVED = "CaseResolved"
    COMPLAINT_RESOLVED = "ComplaintResolved"
