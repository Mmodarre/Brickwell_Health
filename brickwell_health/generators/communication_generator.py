"""
Communication Domain Generators for Brickwell Health Simulator.

Generators for Communication, Communication Preference, Campaign, and Campaign Response.
"""

from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Any, Optional, TYPE_CHECKING
from uuid import UUID

from brickwell_health.domain.communication import (
    CommunicationCreate,
    CommunicationPreferenceCreate,
    CampaignCreate,
    CampaignResponseCreate,
)
from brickwell_health.domain.enums import (
    CampaignResponseType,
    CampaignStatus,
    CampaignType,
    CommunicationDeliveryStatus,
    CommunicationType,
    ConversionType,
    PreferenceType,
    TriggerEventType,
)
from brickwell_health.generators.base import BaseGenerator

if TYPE_CHECKING:
    from brickwell_health.generators.id_generator import IDGenerator
    from brickwell_health.core.environment import SimulationEnvironment
    from brickwell_health.reference.loader import ReferenceDataLoader


class CommunicationPreferenceGenerator(BaseGenerator[CommunicationPreferenceCreate]):
    """
    Generator for communication preferences.

    Creates default opt-in/opt-out preferences for members across all channels.
    """

    def __init__(
        self,
        rng,
        reference: "ReferenceDataLoader",
        id_generator: "IDGenerator",
        sim_env: "SimulationEnvironment",
        config: dict | None = None,
    ):
        """
        Initialize the preference generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator for UUIDs
            sim_env: Simulation environment
            config: Optional preference config
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

    def generate(
        self,
        member_id: UUID,
        policy_id: UUID,
        preference_type: PreferenceType,
        channel: str,
        **kwargs: Any,
    ) -> CommunicationPreferenceCreate:
        """
        Generate a single communication preference.

        Args:
            member_id: Member ID
            policy_id: Policy ID
            preference_type: Type of preference (transactional/marketing/claims)
            channel: Communication channel (Email/SMS/Post/Phone)

        Returns:
            CommunicationPreferenceCreate instance
        """
        preference_id = self.id_generator.generate_uuid()
        current_date = self.get_current_date()

        # Default opt-in rates by type and channel
        opt_in_rate = self._get_opt_in_rate(preference_type, channel)
        is_opted_in = self.rng.random() < opt_in_rate

        # Sample preferred time
        preferred_time = self._sample_preferred_time()

        return CommunicationPreferenceCreate(
            preference_id=preference_id,
            member_id=member_id,
            policy_id=policy_id,
            preference_type=preference_type,
            channel=channel,
            is_opted_in=is_opted_in,
            opt_in_date=current_date if is_opted_in else None,
            opt_out_date=None if is_opted_in else current_date,
            preferred_time=preferred_time,
        )

    def generate_default_preferences(
        self,
        member_id: UUID,
        policy_id: UUID,
    ) -> list[CommunicationPreferenceCreate]:
        """
        Generate default communication preferences for a new member.

        Creates preferences for all type/channel combinations.

        Args:
            member_id: Member ID
            policy_id: Policy ID

        Returns:
            List of CommunicationPreferenceCreate instances
        """
        preferences = []
        channels = ["Email", "SMS", "Post", "Phone"]

        for preference_type in PreferenceType:
            for channel in channels:
                preference = self.generate(
                    member_id=member_id,
                    policy_id=policy_id,
                    preference_type=preference_type,
                    channel=channel,
                )
                preferences.append(preference)

        return preferences

    def _get_opt_in_rate(self, preference_type: PreferenceType, channel: str) -> float:
        """Get opt-in rate for a type/channel combination."""
        default_rates = {
            (PreferenceType.TRANSACTIONAL, "Email"): 1.00,
            (PreferenceType.TRANSACTIONAL, "SMS"): 0.95,
            (PreferenceType.TRANSACTIONAL, "Post"): 1.00,
            (PreferenceType.TRANSACTIONAL, "Phone"): 0.80,
            (PreferenceType.MARKETING, "Email"): 0.85,
            (PreferenceType.MARKETING, "SMS"): 0.70,
            (PreferenceType.MARKETING, "Post"): 0.60,
            (PreferenceType.MARKETING, "Phone"): 0.40,
            (PreferenceType.CLAIMS, "Email"): 1.00,
            (PreferenceType.CLAIMS, "SMS"): 0.90,
            (PreferenceType.CLAIMS, "Post"): 1.00,
            (PreferenceType.CLAIMS, "Phone"): 0.75,
        }
        return default_rates.get((preference_type, channel), 0.80)

    def _sample_preferred_time(self) -> str:
        """Sample preferred contact time."""
        times = ["Morning", "Afternoon", "Evening", "Anytime"]
        weights = [0.25, 0.30, 0.20, 0.25]
        return self.choice(times, weights)


class CommunicationGenerator(BaseGenerator[CommunicationCreate]):
    """
    Generator for communication records.

    Generates transactional and marketing communications with delivery
    status and engagement tracking.
    """

    def __init__(
        self,
        rng,
        reference: "ReferenceDataLoader",
        id_generator: "IDGenerator",
        sim_env: "SimulationEnvironment",
        config: dict | None = None,
    ):
        """
        Initialize the communication generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator for UUIDs and reference numbers
            sim_env: Simulation environment
            config: Optional communication config
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

        # Load communication templates from reference data
        try:
            self.templates = {
                t["template_code"]: t
                for t in reference.get_communication_templates()
            }
        except (FileNotFoundError, KeyError, AttributeError):
            self.templates = {}

    def generate(
        self,
        policy_id: UUID,
        member_id: UUID,
        template_code: str,
        trigger_event_type: Optional[TriggerEventType] = None,
        trigger_event_id: Optional[UUID] = None,
        claim_id: Optional[UUID] = None,
        invoice_id: Optional[UUID] = None,
        interaction_id: Optional[UUID] = None,
        campaign_id: Optional[UUID] = None,
        recipient_email: Optional[str] = None,
        recipient_phone: Optional[str] = None,
        **kwargs: Any,
    ) -> CommunicationCreate:
        """
        Generate a communication record.

        Args:
            policy_id: Policy ID
            member_id: Member ID
            template_code: Template code from reference data
            trigger_event_type: What triggered this communication
            trigger_event_id: ID of triggering entity
            claim_id: Related claim ID
            invoice_id: Related invoice ID
            interaction_id: Related interaction ID
            campaign_id: Campaign ID if marketing
            recipient_email: Email address
            recipient_phone: Phone number

        Returns:
            CommunicationCreate instance
        """
        communication_id = self.id_generator.generate_uuid()
        communication_reference = self.id_generator.generate_communication_reference()

        # Get template details
        template = self.templates.get(template_code, {})
        default_channel = template.get("default_channel", "Email")
        subject_template = template.get(
            "subject_template", "Communication from Brickwell Health"
        )

        # Determine communication type from channel
        comm_type = self._get_communication_type(default_channel)

        # Set timing
        current_datetime = self.get_current_datetime()
        sent_date = current_datetime

        # Determine delivery status
        is_marketing = campaign_id is not None
        delivery_status, delivery_status_date = self._sample_delivery_status(
            comm_type, is_marketing
        )

        return CommunicationCreate(
            communication_id=communication_id,
            communication_reference=communication_reference,
            policy_id=policy_id,
            member_id=member_id,
            campaign_id=campaign_id,
            communication_type=comm_type,
            template_code=template_code,
            subject=subject_template,
            recipient_email=recipient_email,
            recipient_phone=recipient_phone,
            scheduled_date=None,
            sent_date=sent_date,
            delivery_status=delivery_status,
            delivery_status_date=delivery_status_date,
            opened_date=None,  # Set later in lifecycle
            clicked_date=None,  # Set later in lifecycle
            trigger_event_type=trigger_event_type,
            trigger_event_id=trigger_event_id,
            claim_id=claim_id,
            invoice_id=invoice_id,
            interaction_id=interaction_id,
        )

    def _get_communication_type(self, channel: str) -> CommunicationType:
        """Convert channel to CommunicationType."""
        mapping = {
            "Email": CommunicationType.EMAIL,
            "SMS": CommunicationType.SMS,
            "Letter": CommunicationType.LETTER,
            "Push": CommunicationType.PUSH,
            "InApp": CommunicationType.IN_APP,
        }
        return mapping.get(channel, CommunicationType.EMAIL)

    def _sample_delivery_status(
        self,
        comm_type: CommunicationType,
        is_marketing: bool = False,
    ) -> tuple[CommunicationDeliveryStatus, datetime]:
        """Sample delivery status and timestamp."""
        current = self.get_current_datetime()

        # Get config rates
        if is_marketing:
            config_key = "marketing"
        else:
            config_key = "transactional"

        if comm_type == CommunicationType.SMS:
            config_key = "sms"

        engagement_config = self.config.get(config_key, {})
        delivery_rate = engagement_config.get("delivery_rate", 0.97)

        if self.rng.random() < delivery_rate:
            return CommunicationDeliveryStatus.DELIVERED, current
        elif self.rng.random() < 0.5:
            return CommunicationDeliveryStatus.BOUNCED, current
        else:
            return CommunicationDeliveryStatus.FAILED, current

    def sample_engagement(
        self,
        comm_type: CommunicationType,
        delivery_status: CommunicationDeliveryStatus,
        sent_date: datetime,
        is_marketing: bool = False,
    ) -> tuple[bool, Optional[datetime], bool, Optional[datetime]]:
        """
        Sample engagement (open/click) for a communication.

        Returns tuple of (will_open, open_date, will_click, click_date).
        Used by CommunicationProcess to schedule delayed responses.
        """
        if delivery_status != CommunicationDeliveryStatus.DELIVERED:
            return False, None, False, None

        # Get engagement rates from config
        if is_marketing:
            config_key = "marketing"
        else:
            config_key = "transactional"

        if comm_type == CommunicationType.SMS:
            config_key = "sms"

        engagement_config = self.config.get(config_key, {})
        open_rate = engagement_config.get("open_rate", 0.60 if not is_marketing else 0.20)
        click_rate = engagement_config.get("click_rate", 0.15 if not is_marketing else 0.03)

        will_open = False
        open_date = None
        will_click = False
        click_date = None

        # Sample open
        if self.rng.random() < open_rate:
            will_open = True
            # Open time: exponential distribution, median ~30 minutes
            open_delay_minutes = self.rng.exponential(30)
            open_date = sent_date + timedelta(minutes=open_delay_minutes)

            # Sample click (conditional on open)
            if self.rng.random() < click_rate / open_rate:
                will_click = True
                click_delay_minutes = self.rng.exponential(5)
                click_date = open_date + timedelta(minutes=click_delay_minutes)

        return will_open, open_date, will_click, click_date


class CampaignGenerator(BaseGenerator[CampaignCreate]):
    """
    Generator for marketing campaigns.

    Generates campaigns with type-specific parameters, durations, and targets.
    """

    def __init__(
        self,
        rng,
        reference: "ReferenceDataLoader",
        id_generator: "IDGenerator",
        sim_env: "SimulationEnvironment",
        config: dict | None = None,
    ):
        """
        Initialize the campaign generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
            sim_env: Simulation environment
            config: Optional campaign config
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

        # Load campaign types from reference data
        try:
            self.campaign_types = {
                t["type_code"]: t for t in reference.get_campaign_types()
            }
        except (FileNotFoundError, KeyError, AttributeError):
            self.campaign_types = {}

    def generate(
        self,
        campaign_type: CampaignType,
        start_date: Optional[date] = None,
        **kwargs: Any,
    ) -> CampaignCreate:
        """
        Generate a campaign.

        Args:
            campaign_type: Type of campaign
            start_date: Campaign start date (defaults to current date)

        Returns:
            CampaignCreate instance
        """
        campaign_id = self.id_generator.generate_uuid()
        type_value = (
            campaign_type.value
            if hasattr(campaign_type, "value")
            else str(campaign_type)
        )
        campaign_code = self.id_generator.generate_campaign_code(type_value)

        # Get type details
        type_info = self.campaign_types.get(type_value, {})
        typical_duration_weeks = type_info.get("typical_duration_weeks", 4)
        target_response_rate = Decimal(str(type_info.get("target_response_rate", 0.05)))

        # Set dates
        if start_date is None:
            start_date = self.get_current_date()

        end_date = start_date + timedelta(weeks=typical_duration_weeks)

        # Generate name and description
        campaign_name = self._generate_campaign_name(campaign_type)
        description = self._generate_description(campaign_type)
        target_audience = self._generate_target_audience(campaign_type)

        return CampaignCreate(
            campaign_id=campaign_id,
            campaign_code=campaign_code,
            campaign_name=campaign_name,
            campaign_type=campaign_type,
            description=description,
            start_date=start_date,
            end_date=end_date,
            status=CampaignStatus.ACTIVE,
            target_audience=target_audience,
            target_segment=None,
            budget=None,
            actual_spend=None,
            target_response_rate=target_response_rate,
            actual_response_rate=None,
            target_conversion_rate=target_response_rate * Decimal("0.2"),
            actual_conversion_rate=None,
            members_targeted=0,
            communications_sent=0,
            responses_received=0,
            conversions=0,
            owner="Marketing Team",
        )

    def _generate_campaign_name(self, campaign_type: CampaignType) -> str:
        """Generate a campaign name."""
        month = self.get_current_date().strftime("%B")
        year = self.get_current_date().year

        names = {
            CampaignType.RETENTION: f"{month} {year} Retention Campaign",
            CampaignType.UPSELL: f"{month} {year} Upgrade Offer",
            CampaignType.CROSS_SELL: f"{month} {year} Add Extras Campaign",
            CampaignType.ENGAGEMENT: f"{month} {year} Member Engagement",
            CampaignType.WINBACK: f"{month} {year} Winback Campaign",
            CampaignType.ACQUISITION: f"{month} {year} New Member Campaign",
        }
        return names.get(campaign_type, f"{month} {year} Campaign")

    def _generate_description(self, campaign_type: CampaignType) -> str:
        """Generate campaign description."""
        descriptions = {
            CampaignType.RETENTION: "Target at-risk members with retention offers",
            CampaignType.UPSELL: "Encourage members to upgrade their coverage tier",
            CampaignType.CROSS_SELL: "Promote extras cover to hospital-only members",
            CampaignType.ENGAGEMENT: "Increase member engagement with health content",
            CampaignType.WINBACK: "Re-engage lapsed members with competitive offers",
            CampaignType.ACQUISITION: "Attract new members to Brickwell Health",
        }
        return descriptions.get(campaign_type, "Marketing campaign")

    def _generate_target_audience(self, campaign_type: CampaignType) -> str:
        """Generate target audience description."""
        audiences = {
            CampaignType.RETENTION: "Members with high churn risk indicators",
            CampaignType.UPSELL: "Bronze/Silver tier members with tenure > 12 months",
            CampaignType.CROSS_SELL: "Hospital-only members without extras cover",
            CampaignType.ENGAGEMENT: "All active members",
            CampaignType.WINBACK: "Members who lapsed in past 12 months",
            CampaignType.ACQUISITION: "Prospects from partner channels",
        }
        return audiences.get(campaign_type, "Target members")


class CampaignResponseGenerator(BaseGenerator[CampaignResponseCreate]):
    """
    Generator for campaign responses.

    Generates response records for the engagement funnel (open -> click -> convert).
    """

    def __init__(
        self,
        rng,
        reference: "ReferenceDataLoader",
        id_generator: "IDGenerator",
        sim_env: "SimulationEnvironment",
        config: dict | None = None,
    ):
        """
        Initialize the campaign response generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
            sim_env: Simulation environment
            config: Optional config
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

    def generate(
        self,
        campaign_id: UUID,
        member_id: UUID,
        policy_id: UUID,
        communication_id: UUID,
        response_type: CampaignResponseType,
        response_date: Optional[datetime] = None,
        response_channel: str = "Email",
        conversion_type: Optional[ConversionType] = None,
        **kwargs: Any,
    ) -> CampaignResponseCreate:
        """
        Generate a campaign response.

        Args:
            campaign_id: Campaign ID
            member_id: Member ID
            policy_id: Policy ID
            communication_id: Related communication ID
            response_type: Type of response (opened/clicked/converted)
            response_date: When response occurred
            response_channel: Channel of response
            conversion_type: Type of conversion (if converted)

        Returns:
            CampaignResponseCreate instance
        """
        response_id = self.id_generator.generate_uuid()

        # Use provided date or current datetime
        if response_date is None:
            response_date = self.get_current_datetime()

        # Calculate conversion value if converted
        conversion_value = None
        if response_type == CampaignResponseType.CONVERTED and conversion_type:
            conversion_value = self._calculate_conversion_value(conversion_type)

        return CampaignResponseCreate(
            response_id=response_id,
            campaign_id=campaign_id,
            member_id=member_id,
            policy_id=policy_id,
            communication_id=communication_id,
            response_type=response_type,
            response_date=response_date,
            conversion_type=conversion_type,
            conversion_value=conversion_value,
            response_channel=response_channel,
        )

    def _calculate_conversion_value(self, conversion_type: ConversionType) -> Decimal:
        """Calculate conversion value based on type."""
        values = {
            ConversionType.RENEWED: Decimal("500"),  # Annual premium retention
            ConversionType.UPGRADED: Decimal("200"),  # Premium increase
            ConversionType.ADDED_COVER: Decimal("150"),  # Extras premium
            ConversionType.REFERRED: Decimal("100"),  # Referral bonus
        }
        base_value = values.get(conversion_type, Decimal("100"))

        # Add some variance (0.8 to 1.2x)
        variance = self.rng.uniform(0.8, 1.2)
        return (base_value * Decimal(str(round(variance, 2)))).quantize(Decimal("0.01"))

    def get_conversion_type_for_campaign(
        self, campaign_type: CampaignType
    ) -> ConversionType:
        """Get appropriate conversion type for a campaign type."""
        mapping = {
            CampaignType.RETENTION: ConversionType.RENEWED,
            CampaignType.UPSELL: ConversionType.UPGRADED,
            CampaignType.CROSS_SELL: ConversionType.ADDED_COVER,
            CampaignType.ENGAGEMENT: ConversionType.RENEWED,
            CampaignType.WINBACK: ConversionType.RENEWED,
            CampaignType.ACQUISITION: ConversionType.RENEWED,
        }
        return mapping.get(campaign_type, ConversionType.RENEWED)
