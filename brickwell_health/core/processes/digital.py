"""
Digital Behavior Process for Brickwell Health Simulator.

Generates web sessions and digital events for member behavioral analytics.
"""

from datetime import date
from typing import Any, Generator, Optional, TYPE_CHECKING
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.digital import WebSessionCreate, DigitalEventCreate
from brickwell_health.domain.enums import TriggerEventType
from brickwell_health.generators.digital_generator import DigitalBehaviorGenerator

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class DigitalBehaviorProcess(BaseProcess):
    """
    Digital behavior process for generating web sessions and events.

    This process:
    1. Generates baseline sessions based on member engagement levels
    2. Generates trigger-based sessions for events (claim submitted, invoice, etc.)
    3. Tracks high-value intent signals (cancel page views)
    4. Emits cancel_page_viewed events to CRM queue for churn tracking
    """

    def __init__(
        self,
        *args: Any,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """
        Initialize the digital behavior process.

        Args:
            shared_state: Shared state for cross-process communication
        """
        super().__init__(*args, **kwargs)
        self.shared_state = shared_state

        # Get digital configuration
        digital_config = getattr(self.config, "digital", None)

        # Extract config as dict
        if digital_config is None:
            digital_config_dict = {}
        elif hasattr(digital_config, "model_dump"):
            digital_config_dict = digital_config.model_dump()
        elif hasattr(digital_config, "__dict__"):
            digital_config_dict = vars(digital_config)
        else:
            digital_config_dict = {}

        # Initialize generator
        self.digital_gen = DigitalBehaviorGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
            config=digital_config_dict,
        )

        # Configuration
        self.sessions_per_month = digital_config_dict.get(
            "sessions_per_month", {"high": 8.0, "medium": 2.5, "low": 0.5}
        )
        self.engagement_distribution = digital_config_dict.get(
            "engagement_distribution", {"high": 0.15, "medium": 0.35, "low": 0.50}
        )

        # Trigger session probabilities
        self.trigger_session_probs = {
            "claim_submitted": 0.40,
            "claim_rejected": 0.50,
            "claim_paid": 0.25,
            "invoice_issued": 0.30,
            "payment_failed": 0.50,
            "renewal_reminder": 0.35,
        }

        # Track processed trigger events to avoid duplicates
        self._processed_triggers: set[str] = set()

        # Statistics
        self._stats = {
            "sessions_created": 0,
            "events_created": 0,
            "cancel_page_views": 0,
            "trigger_sessions": 0,
            "baseline_sessions": 0,
        }

    def run(self) -> Generator:
        """Main digital behavior process loop."""
        # Wait for warmup
        while self.sim_env.now < 30:
            yield self.env.timeout(1.0)

        while True:
            current_date = self.sim_env.current_date

            # 1. Generate trigger-based sessions from CRM events
            self._process_trigger_sessions(current_date)

            # 2. Generate baseline sessions for members
            self._generate_baseline_sessions(current_date)

            # Wait for next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _process_trigger_sessions(self, current_date: date) -> None:
        """Generate sessions triggered by events."""
        if not self.shared_state:
            return

        # Peek at CRM events (don't consume - they're for CRM/Communication)
        events = self.shared_state.peek_crm_events()

        for event in events:
            event_type = event.get("event_type", "").lower()

            # Create unique key for this trigger
            event_id = event.get("claim_id") or event.get("invoice_id")
            trigger_key = f"{event_type}:{event_id}"

            # Skip if already processed
            if trigger_key in self._processed_triggers:
                continue

            # Check if this event should trigger a session
            prob = self.trigger_session_probs.get(event_type, 0)
            if prob > 0 and self.rng.random() < prob:
                self._generate_trigger_session(event)
                self._processed_triggers.add(trigger_key)
                self._stats["trigger_sessions"] += 1

    def _generate_trigger_session(self, event: dict) -> None:
        """Generate a session triggered by an event."""
        member_id = event.get("member_id")
        policy_id = event.get("policy_id")

        if not member_id or not policy_id:
            return

        # Map event type to TriggerEventType
        trigger_type = self._map_trigger_type(event.get("event_type", ""))
        trigger_id = event.get("claim_id") or event.get("invoice_id")

        # Get member engagement level
        engagement_level = self._get_engagement_level(member_id)

        # Generate session
        session, events = self.digital_gen.generate_session(
            member_id=member_id,
            policy_id=policy_id,
            trigger_event_type=trigger_type,
            trigger_event_id=trigger_id,
            engagement_level=engagement_level,
        )

        # Write to database
        self._write_session_and_events(session, events)

    def _generate_baseline_sessions(self, current_date: date) -> None:
        """Generate baseline sessions based on member engagement levels."""
        if not self.shared_state:
            return

        for pm_id, member_data in self.shared_state.policy_members.items():
            policy = member_data.get("policy")
            member = member_data.get("member")

            if not policy or not member:
                continue

            # Check policy status
            policy_data = self.shared_state.active_policies.get(policy.policy_id, {})
            policy_status = policy_data.get("status", "")
            if policy_status in ("Suspended", "Lapsed", "Cancelled"):
                continue

            # Get engagement level
            engagement_level = self._get_engagement_level(member.member_id)

            # Calculate daily session probability
            monthly_rate = self.sessions_per_month.get(engagement_level, 2.5)
            daily_rate = monthly_rate / 30

            # Check if member has a session today
            if self.rng.random() < daily_rate:
                session, events = self.digital_gen.generate_session(
                    member_id=member.member_id,
                    policy_id=policy.policy_id,
                    engagement_level=engagement_level,
                )

                self._write_session_and_events(session, events)
                self._stats["baseline_sessions"] += 1

    def _write_session_and_events(
        self, session: WebSessionCreate, events: list[DigitalEventCreate]
    ) -> None:
        """Write session and events to database."""
        # Write session
        self.batch_writer.add("web_session", session.model_dump_db())
        self._stats["sessions_created"] += 1

        # Track cancel page views
        if session.viewed_cancel_page:
            self._stats["cancel_page_views"] += 1

            # Emit event for churn risk tracking
            if self.shared_state:
                self.shared_state.add_crm_event(
                    {
                        "event_type": "cancel_page_viewed",
                        "member_id": session.member_id,
                        "policy_id": session.policy_id,
                        "session_id": session.session_id,
                        "timestamp": session.session_start,
                    }
                )

        # Write events
        for event in events:
            self.batch_writer.add("digital_event", event.model_dump_db())
            self._stats["events_created"] += 1

    def _get_engagement_level(self, member_id: UUID) -> str:
        """Get engagement level for a member from SharedState."""
        if self.shared_state:
            return self.shared_state.get_engagement_level(member_id)
        return "medium"

    def _map_trigger_type(self, event_type: str) -> Optional[TriggerEventType]:
        """Map event type string to TriggerEventType enum."""
        mapping = {
            "claim_submitted": TriggerEventType.CLAIM_SUBMITTED,
            "claim_rejected": TriggerEventType.CLAIM_REJECTED,
            "claim_paid": TriggerEventType.CLAIM_PAID,
            "invoice_issued": TriggerEventType.INVOICE_ISSUED,
            "payment_failed": TriggerEventType.PAYMENT_FAILED,
            "renewal_reminder": TriggerEventType.RENEWAL_REMINDER,
            "arrears_created": TriggerEventType.ARREARS_CREATED,
        }
        return mapping.get(event_type.lower())

    def _log_progress(self) -> None:
        """Log process statistics."""
        logger.info(
            "digital_progress",
            sim_day=int(self.sim_env.now),
            sessions=self._stats["sessions_created"],
            events=self._stats["events_created"],
            cancel_views=self._stats["cancel_page_views"],
            trigger_sessions=self._stats["trigger_sessions"],
            baseline_sessions=self._stats["baseline_sessions"],
        )

    def get_stats(self) -> dict[str, int]:
        """Get process statistics."""
        return self._stats.copy()
