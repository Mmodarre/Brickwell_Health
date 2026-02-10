"""
Communication Process for Brickwell Health Simulator.

Handles generation of transactional and marketing communications,
campaign management, and delayed response lifecycle processing.
"""

from datetime import date, timedelta
from typing import Any, Generator, Optional, TYPE_CHECKING
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.communication import (
    CommunicationCreate,
    CampaignCreate,
    CampaignResponseCreate,
)
from brickwell_health.domain.enums import (
    CampaignResponseType,
    CampaignStatus,
    CampaignType,
    CommunicationDeliveryStatus,
    ConversionType,
    PreferenceType,
    TriggerEventType,
)
from brickwell_health.generators.communication_generator import (
    CommunicationGenerator,
    CampaignGenerator,
    CampaignResponseGenerator,
)

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class CommunicationProcess(BaseProcess):
    """
    Communication process for generating outbound messages.

    This process:
    1. Consumes events from communication_event_queue (from CRMProcess)
    2. Sends transactional communications for events
    3. Creates and manages marketing campaigns
    4. Tracks campaign responses and processes lifecycle (INSERT-then-UPDATE)
    5. Enforces fatigue rules
    """

    def __init__(
        self,
        *args: Any,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """
        Initialize the communication process.

        Args:
            shared_state: Shared state for cross-process communication
        """
        super().__init__(*args, **kwargs)
        self.shared_state = shared_state

        # Get communication configuration
        comm_config = getattr(self.config, "communication", None)
        if comm_config is None:
            comm_config = type(
                "CommunicationConfig",
                (),
                {
                    "transactional": {},
                    "marketing": {},
                    "sms": {},
                    "fatigue": {},
                },
            )()

        # Get fatigue config
        fatigue_config = getattr(comm_config, "fatigue", None)
        if fatigue_config and hasattr(fatigue_config, "model_dump"):
            self.fatigue_config = fatigue_config.model_dump()
        elif fatigue_config and hasattr(fatigue_config, "__dict__"):
            self.fatigue_config = vars(fatigue_config)
        else:
            self.fatigue_config = {}

        # Get config dicts
        def get_config_dict(cfg):
            if cfg is None:
                return {}
            if hasattr(cfg, "model_dump"):
                return cfg.model_dump()
            if hasattr(cfg, "__dict__"):
                return vars(cfg)
            return {}

        comm_config_dict = {
            "transactional": get_config_dict(getattr(comm_config, "transactional", None)),
            "marketing": get_config_dict(getattr(comm_config, "marketing", None)),
            "sms": get_config_dict(getattr(comm_config, "sms", None)),
        }

        # Get campaign configuration
        campaign_config = getattr(self.config, "campaign", None)
        campaign_config_dict = get_config_dict(campaign_config) if campaign_config else {}

        # Initialize generators
        self.communication_gen = CommunicationGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
            config=comm_config_dict,
        )
        self.campaign_gen = CampaignGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
            config=campaign_config_dict,
        )
        self.response_gen = CampaignResponseGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
        )

        # Track active campaigns
        self.active_campaigns: dict[UUID, CampaignCreate] = {}

        # Track recent communications per member (for fatigue)
        self.recent_comms: dict[UUID, list[dict]] = {}

        # Configuration values
        self.max_marketing_per_week = self.fatigue_config.get("max_marketing_per_week", 2)
        self.suppress_after_complaint_days = self.fatigue_config.get(
            "suppress_after_complaint_days", 30
        )

        # Campaign config
        self.campaigns_per_year = campaign_config_dict.get("campaigns_per_year", 6)
        self.type_distribution = campaign_config_dict.get(
            "type_distribution",
            self.reference.get_campaign_type_distribution(),
        )
        self.response_rates = campaign_config_dict.get("response_rates", {})

        # Statistics
        self._stats = {
            "transactional_sent": 0,
            "marketing_sent": 0,
            "campaigns_created": 0,
            "campaigns_completed": 0,
            "responses_opened": 0,
            "responses_clicked": 0,
            "responses_converted": 0,
        }

    def run(self) -> Generator:
        """Main communication process loop."""
        logger.info(
            "communication_process_started",
            worker_id=self.worker_id,
        )

        # Wait for warmup period
        while self.sim_env.now < 30:
            yield self.env.timeout(1.0)

        while True:
            current_date = self.sim_env.current_date

            # 1. Process transactional communication triggers from CRM
            self._process_communication_events(current_date)

            # 2. Manage marketing campaigns
            self._manage_campaigns(current_date)

            # 3. Process delayed campaign response lifecycle (INSERT-then-UPDATE)
            self._process_response_lifecycle(current_date)

            # Wait for next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _process_communication_events(self, current_date: date) -> None:
        """Process communication events from CRMProcess."""
        if not self.shared_state:
            return

        events = self.shared_state.get_communication_events()

        for event in events:
            event_type = event.get("event_type", "")

            if event_type == "interaction_completed":
                self._send_transactional_communication(event)
            elif event_type == "nba_communication":
                self._send_nba_communication(event)

    def _send_nba_communication(self, event: dict) -> None:
        """
        Send communication for NBA action (Email/SMS/InApp).

        Called when NBAActionProcess emits an 'nba_communication' event
        for non-Phone channel NBA actions.

        Args:
            event: Event dictionary with member_id, policy_id, channel, details
        """
        member_id = event.get("member_id")
        policy_id = event.get("policy_id")
        channel = event.get("channel")  # Email, SMS, InApp
        details = event.get("details", {})
        action_category = details.get("action_category", "Service")

        if not member_id or not policy_id:
            return

        # Map action category to template code
        template_map = {
            "Retention": "nba_retention_offer",
            "Upsell": "nba_upgrade_offer",
            "CrossSell": "nba_addon_offer",
            "Service": "nba_service_outreach",
            "Wellness": "nba_wellness_reminder",
        }
        template_code = template_map.get(action_category, "nba_general")

        # Check communication preferences based on channel
        # NBA communications are considered marketing-type for opt-in purposes
        channel_lower = channel.lower() if channel else "email"
        if channel_lower in ("email", "sms"):
            if not self._is_opted_in(member_id, PreferenceType.MARKETING, channel_lower.capitalize()):
                self._stats["nba_suppressed_opt_out"] = self._stats.get(
                    "nba_suppressed_opt_out", 0
                ) + 1
                return

        # Generate communication
        communication = self.communication_gen.generate(
            policy_id=policy_id,
            member_id=member_id,
            template_code=template_code,
            trigger_event_type=None,  # NBA-driven, not event-triggered
        )

        # Write to batch
        self.batch_writer.add("communication.communication", communication.model_dump_db())
        self._stats["nba_communications_sent"] = self._stats.get(
            "nba_communications_sent", 0
        ) + 1

        # Track for fatigue (treat as marketing)
        self._track_communication(member_id, communication, is_marketing=True)

        logger.debug(
            "nba_communication_sent",
            communication_id=str(communication.communication_id),
            member_id=str(member_id),
            channel=channel,
            action_code=details.get("action_code"),
            template_code=template_code,
        )

    def _send_transactional_communication(self, event: dict) -> None:
        """Send a transactional communication based on a trigger event."""
        policy_id = event.get("policy_id")
        member_id = event.get("member_id")
        trigger_event_type_str = event.get("trigger_event_type")

        if not policy_id or not member_id:
            return

        # Get template for this trigger type
        template_code = self._get_template_for_trigger(trigger_event_type_str)
        if not template_code:
            return

        # Check preferences
        if not self._is_opted_in(member_id, PreferenceType.TRANSACTIONAL, "Email"):
            return

        # Map trigger string to enum
        trigger_type = self._map_trigger_type(trigger_event_type_str)

        communication = self.communication_gen.generate(
            policy_id=policy_id,
            member_id=member_id,
            template_code=template_code,
            trigger_event_type=trigger_type,
            trigger_event_id=event.get("claim_id") or event.get("invoice_id"),
            claim_id=event.get("claim_id"),
            invoice_id=event.get("invoice_id"),
            interaction_id=event.get("interaction_id"),
        )

        self.batch_writer.add("communication.communication", communication.model_dump_db())
        self._stats["transactional_sent"] += 1

        # Track for fatigue
        self._track_communication(member_id, communication, is_marketing=False)

    def _manage_campaigns(self, current_date: date) -> None:
        """Manage marketing campaigns."""
        # Check if we need to create a new campaign
        self._maybe_create_campaign(current_date)

        # Process active campaigns
        for campaign_id, campaign in list(self.active_campaigns.items()):
            # Check if campaign has ended
            if campaign.end_date and current_date > campaign.end_date:
                self._close_campaign(campaign_id)
                continue

            # Send campaign communications (weekly on Mondays)
            if current_date.weekday() == 0:
                self._send_campaign_communications(campaign, current_date)

    def _maybe_create_campaign(self, current_date: date) -> None:
        """Create a new campaign if needed."""
        # Calculate expected campaigns per month
        campaigns_per_month = self.campaigns_per_year / 12

        # Create at start of month with probability based on rate
        if current_date.day == 1:
            if self.rng.random() < campaigns_per_month:
                self._create_new_campaign(current_date)

    def _create_new_campaign(self, start_date: date) -> None:
        """Create a new marketing campaign."""
        # Sample campaign type
        type_names = list(self.type_distribution.keys())
        type_probs = list(self.type_distribution.values())

        # Normalize probabilities
        total = sum(type_probs)
        type_probs = [p / total for p in type_probs]

        selected_type_name = self.rng.choice(type_names, p=type_probs)

        # Map to CampaignType enum
        try:
            campaign_type = CampaignType(selected_type_name)
        except ValueError:
            campaign_type = CampaignType.ENGAGEMENT

        campaign = self.campaign_gen.generate(
            campaign_type=campaign_type,
            start_date=start_date,
        )

        self.batch_writer.add("communication.campaign", campaign.model_dump_db())
        self.active_campaigns[campaign.campaign_id] = campaign
        self._stats["campaigns_created"] += 1

    def _send_campaign_communications(
        self,
        campaign: CampaignCreate,
        current_date: date,
    ) -> None:
        """Send communications for an active campaign."""
        if not self.shared_state:
            return

        # Get eligible members for this campaign
        eligible_members = self._get_eligible_campaign_members(campaign)

        # Sample members to contact this week (cap at 100/week)
        target_weekly = min(len(eligible_members), 100)
        if target_weekly == 0:
            return

        selected_indices = self.rng.choice(
            len(eligible_members),
            size=min(target_weekly, len(eligible_members)),
            replace=False,
        )
        selected_members = [eligible_members[i] for i in selected_indices]

        for member_data in selected_members:
            member_id = member_data.get("member_id")
            policy_id = member_data.get("policy_id")

            if not member_id or not policy_id:
                continue

            # Check preferences
            if not self._is_opted_in(member_id, PreferenceType.MARKETING, "Email"):
                continue

            # Check fatigue
            if self._is_fatigued(member_id):
                continue

            # Send communication
            communication = self.communication_gen.generate(
                policy_id=policy_id,
                member_id=member_id,
                template_code="MARKETING_CAMPAIGN",
                campaign_id=campaign.campaign_id,
            )

            self.batch_writer.add("communication.communication", communication.model_dump_db())
            self._stats["marketing_sent"] += 1

            # Track for fatigue
            self._track_communication(member_id, communication, is_marketing=True)

            # Schedule potential response based on engagement sampling
            self._schedule_campaign_response(
                campaign, communication, member_id, policy_id
            )

    def _schedule_campaign_response(
        self,
        campaign: CampaignCreate,
        communication: CommunicationCreate,
        member_id: UUID,
        policy_id: UUID,
    ) -> None:
        """Schedule a potential campaign response for delayed processing."""
        if not self.shared_state:
            return

        # Only track responses for delivered communications
        if communication.delivery_status != CommunicationDeliveryStatus.DELIVERED:
            return

        # Sample engagement
        is_marketing = communication.campaign_id is not None
        will_open, open_date, will_click, click_date = self.communication_gen.sample_engagement(
            communication.communication_type,
            communication.delivery_status,
            communication.sent_date,
            is_marketing=is_marketing,
        )

        if not will_open:
            return

        # Get conversion rate for this campaign type
        campaign_type_str = (
            campaign.campaign_type.value
            if hasattr(campaign.campaign_type, "value")
            else str(campaign.campaign_type)
        )
        target_response_rate = self.response_rates.get(campaign_type_str, 0.05)

        # Will convert if clicked and passes conversion check
        will_convert = will_click and (self.rng.random() < target_response_rate * 0.2)

        # Create initial response record (pending_open state) via INSERT
        response = self.response_gen.generate(
            campaign_id=campaign.campaign_id,
            member_id=member_id,
            policy_id=policy_id,
            communication_id=communication.communication_id,
            response_type=CampaignResponseType.OPENED,  # Will be updated
            response_date=open_date,  # Predicted open date
            response_channel="Email",
        )

        # INSERT initial response
        self.batch_writer.add("communication.campaign_response", response.model_dump_db())

        # Track in pending for lifecycle processing
        self.shared_state.add_pending_campaign_response(
            response.response_id,
            {
                "campaign_id": campaign.campaign_id,
                "communication_id": communication.communication_id,
                "member_id": member_id,
                "policy_id": policy_id,
                "sent_date": communication.sent_date,
                "predicted_open_date": open_date,
                "predicted_click_date": click_date,
                "will_click": will_click,
                "will_convert": will_convert,
                "campaign_type": campaign.campaign_type,
                "status": "pending_open",
            },
        )

    def _process_response_lifecycle(self, current_date: date) -> None:
        """Process due campaign responses - UPDATE after delay."""
        if not self.shared_state:
            return

        current_datetime = self.sim_env.current_datetime
        due_responses = self.shared_state.get_due_campaign_responses(current_datetime)

        for response_id, response_data in due_responses:
            status = response_data.get("status", "")

            if status == "pending_open":
                # UPDATE response to OPENED
                self._update_response_opened(response_id, response_data)
                self._stats["responses_opened"] += 1

                # Schedule click if applicable
                if response_data.get("will_click"):
                    self.shared_state.update_pending_campaign_response(
                        response_id,
                        {"status": "pending_click"},
                    )
                else:
                    # No click - remove from pending
                    self.shared_state.remove_pending_campaign_response(response_id)

            elif status == "pending_click":
                # UPDATE response to CLICKED
                self._update_response_clicked(response_id, response_data)
                self._stats["responses_clicked"] += 1

                # Schedule conversion if applicable
                if response_data.get("will_convert"):
                    # Add conversion delay (1-3 days after click)
                    convert_delay_days = self.rng.uniform(1, 3)
                    convert_date = current_datetime + timedelta(days=convert_delay_days)
                    self.shared_state.update_pending_campaign_response(
                        response_id,
                        {
                            "status": "pending_convert",
                            "predicted_convert_date": convert_date,
                        },
                    )
                else:
                    # No conversion - remove from pending
                    self.shared_state.remove_pending_campaign_response(response_id)

            elif status == "pending_convert":
                # UPDATE response to CONVERTED
                self._update_response_converted(response_id, response_data)
                self._stats["responses_converted"] += 1
                # Lifecycle complete
                self.shared_state.remove_pending_campaign_response(response_id)

    def _update_response_opened(
        self, response_id: UUID, response_data: dict
    ) -> None:
        """UPDATE response to OPENED status."""
        # Flush INSERT before UPDATE for CDC visibility
        self.batch_writer.flush_for_cdc("campaign_response", "response_id", response_id)

        updates = {
            "response_type": CampaignResponseType.OPENED.value,
            "response_date": response_data["predicted_open_date"].isoformat(),
        }

        self.batch_writer.update_record(
            "campaign_response", "response_id", response_id, updates
        )

    def _update_response_clicked(
        self, response_id: UUID, response_data: dict
    ) -> None:
        """UPDATE response to CLICKED status."""
        self.batch_writer.flush_for_cdc("campaign_response", "response_id", response_id)

        updates = {
            "response_type": CampaignResponseType.CLICKED.value,
            "response_date": response_data["predicted_click_date"].isoformat(),
        }

        self.batch_writer.update_record(
            "campaign_response", "response_id", response_id, updates
        )

    def _update_response_converted(
        self, response_id: UUID, response_data: dict
    ) -> None:
        """UPDATE response to CONVERTED status."""
        self.batch_writer.flush_for_cdc("campaign_response", "response_id", response_id)

        # Get conversion type for campaign
        campaign_type = response_data.get("campaign_type")
        conversion_type = self.response_gen.get_conversion_type_for_campaign(
            campaign_type
        )

        updates = {
            "response_type": CampaignResponseType.CONVERTED.value,
            "response_date": self.sim_env.current_datetime.isoformat(),
            "conversion_type": conversion_type.value,
            "conversion_value": str(
                self.response_gen._calculate_conversion_value(conversion_type)
            ),
        }

        self.batch_writer.update_record(
            "campaign_response", "response_id", response_id, updates
        )

    def _get_eligible_campaign_members(
        self,
        campaign: CampaignCreate,
    ) -> list[dict]:
        """Get members eligible for a campaign."""
        if not self.shared_state:
            return []

        eligible = []

        for pm_id, member_data in self.shared_state.policy_members.items():
            policy = member_data.get("policy")
            member = member_data.get("member")

            if not policy or not member:
                continue

            # Check policy status
            policy_data = self.shared_state.active_policies.get(policy.policy_id, {})
            status = policy_data.get("status", "")
            if status in ("Suspended", "Lapsed", "Cancelled"):
                continue

            eligible.append(
                {
                    "member_id": member.member_id,
                    "policy_id": policy.policy_id,
                }
            )

        return eligible

    def _close_campaign(self, campaign_id: UUID) -> None:
        """Close a completed campaign."""
        self.batch_writer.flush_for_cdc("campaign", "campaign_id", campaign_id)

        updates = {
            "status": CampaignStatus.COMPLETED.value,
            "modified_at": self.sim_env.current_datetime.isoformat(),
            "modified_by": "SIMULATION",
        }

        self.batch_writer.update_record("campaign", "campaign_id", campaign_id, updates)
        del self.active_campaigns[campaign_id]
        self._stats["campaigns_completed"] += 1

    def _is_opted_in(
        self,
        member_id: UUID,
        preference_type: PreferenceType,
        channel: str,
    ) -> bool:
        """Check if member is opted in for a preference type and channel."""
        if self.shared_state:
            pref_type_str = (
                preference_type.value
                if hasattr(preference_type, "value")
                else str(preference_type)
            )
            return self.shared_state.is_opted_in(member_id, pref_type_str, channel)
        return True  # Default to opted in if no shared state

    def _is_fatigued(self, member_id: UUID) -> bool:
        """Check if member is fatigued (too many recent communications)."""
        if member_id not in self.recent_comms:
            return False

        recent = self.recent_comms[member_id]

        # Count marketing communications in last 7 days
        cutoff = self.sim_env.current_datetime - timedelta(days=7)
        marketing_count = sum(
            1
            for c in recent
            if c.get("is_marketing") and c.get("sent_date", cutoff) > cutoff
        )

        return marketing_count >= self.max_marketing_per_week

    def _track_communication(
        self,
        member_id: UUID,
        communication: CommunicationCreate,
        is_marketing: bool,
    ) -> None:
        """Track a communication for fatigue management."""
        if member_id not in self.recent_comms:
            self.recent_comms[member_id] = []

        self.recent_comms[member_id].append(
            {
                "communication_id": communication.communication_id,
                "sent_date": communication.sent_date,
                "is_marketing": is_marketing,
            }
        )

        # Keep only last 30 days
        cutoff = self.sim_env.current_datetime - timedelta(days=30)
        self.recent_comms[member_id] = [
            c
            for c in self.recent_comms[member_id]
            if c.get("sent_date", cutoff) > cutoff
        ]

    def _get_template_for_trigger(self, trigger_type: Optional[str]) -> Optional[str]:
        """Get communication template for a trigger type."""
        if not trigger_type:
            return None

        # Try reference lookup first (with normalized matching)
        template = self.reference.get_communication_template_by_trigger_normalized(trigger_type)
        if template:
            return template["template_code"]

        # Fallback to hardcoded mapping if reference lookup fails
        fallback_mapping = {
            "claim_submitted": "CLAIM_RECEIVED",
            "claim_paid": "CLAIM_PAID",
            "claim_rejected": "CLAIM_REJECTED",
            "ClaimSubmitted": "CLAIM_RECEIVED",
            "ClaimPaid": "CLAIM_PAID",
            "ClaimRejected": "CLAIM_REJECTED",
            "invoice_issued": "INVOICE_NOTIFICATION",
            "payment_failed": "PAYMENT_FAILED",
            "arrears_created": "ARREARS_NOTICE",
            "policy_suspended": "SUSPENSION_NOTICE",
            "InvoiceIssued": "INVOICE_NOTIFICATION",
            "PaymentFailed": "PAYMENT_FAILED",
            "ArrearsCreated": "ARREARS_NOTICE",
            "PolicySuspended": "SUSPENSION_NOTICE",
        }
        return fallback_mapping.get(trigger_type)

    def _map_trigger_type(self, trigger_type_str: Optional[str]) -> Optional[TriggerEventType]:
        """Map trigger string to TriggerEventType enum."""
        if not trigger_type_str:
            return None

        mapping = {
            "claim_submitted": TriggerEventType.CLAIM_SUBMITTED,
            "claim_paid": TriggerEventType.CLAIM_PAID,
            "claim_rejected": TriggerEventType.CLAIM_REJECTED,
            "ClaimSubmitted": TriggerEventType.CLAIM_SUBMITTED,
            "ClaimPaid": TriggerEventType.CLAIM_PAID,
            "ClaimRejected": TriggerEventType.CLAIM_REJECTED,
            "invoice_issued": TriggerEventType.INVOICE_ISSUED,
            "payment_failed": TriggerEventType.PAYMENT_FAILED,
            "arrears_created": TriggerEventType.ARREARS_CREATED,
            "policy_suspended": TriggerEventType.POLICY_SUSPENDED,
            "InvoiceIssued": TriggerEventType.INVOICE_ISSUED,
            "PaymentFailed": TriggerEventType.PAYMENT_FAILED,
            "ArrearsCreated": TriggerEventType.ARREARS_CREATED,
            "PolicySuspended": TriggerEventType.POLICY_SUSPENDED,
        }
        return mapping.get(trigger_type_str)

    def _log_progress(self) -> None:
        """Log process statistics."""
        logger.info(
            "communication_process_progress",
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            transactional_sent=self._stats["transactional_sent"],
            marketing_sent=self._stats["marketing_sent"],
            campaigns_created=self._stats["campaigns_created"],
            campaigns_completed=self._stats["campaigns_completed"],
            responses_opened=self._stats["responses_opened"],
            responses_clicked=self._stats["responses_clicked"],
            responses_converted=self._stats["responses_converted"],
            active_campaigns=len(self.active_campaigns),
        )
