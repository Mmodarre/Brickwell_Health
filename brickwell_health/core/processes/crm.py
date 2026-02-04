"""
CRM Process for Brickwell Health Simulator.

Handles generation of interactions, cases, and complaints based on
trigger events from Claims and Billing processes, plus baseline interactions.
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Generator, Optional, TYPE_CHECKING
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.core.trigger_engine import EventTriggerEngine
from brickwell_health.domain.crm import InteractionCreate, CaseCreate, ComplaintCreate
from brickwell_health.domain.enums import (
    CasePriority,
    CaseStatus,
    ComplaintStatus,
    TriggerEventType,
)
from brickwell_health.generators.crm_generator import (
    InteractionGenerator,
    CaseGenerator,
    ComplaintGenerator,
)

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class CRMProcess(BaseProcess):
    """
    CRM process for generating interactions, cases, and complaints.

    This process:
    1. Consumes trigger events from Claims and Billing processes
    2. Generates interactions based on trigger probabilities
    3. Creates cases for complex issues
    4. Creates complaints for escalations
    5. Processes case/complaint resolution lifecycle (INSERT-then-UPDATE)
    6. Generates baseline interactions independent of triggers
    """

    def __init__(
        self,
        *args: Any,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """
        Initialize the CRM process.

        Args:
            shared_state: Shared state for cross-process communication
        """
        super().__init__(*args, **kwargs)
        self.shared_state = shared_state

        # Get CRM configuration
        crm_config = getattr(self.config, "crm", None)
        if crm_config is None:
            crm_config = type("CRMConfig", (), {
                "interaction": {},
                "case": {},
                "complaint": {},
            })()

        # Get configs as dicts
        interaction_config = (
            crm_config.interaction.model_dump()
            if hasattr(crm_config.interaction, "model_dump")
            else {}
        )
        case_config = (
            crm_config.case.model_dump()
            if hasattr(crm_config.case, "model_dump")
            else {}
        )
        complaint_config = (
            crm_config.complaint.model_dump()
            if hasattr(crm_config.complaint, "model_dump")
            else {}
        )

        # Initialize generators
        self.interaction_gen = InteractionGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
            config=interaction_config,
        )
        self.case_gen = CaseGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
            config=case_config,
        )
        self.complaint_gen = ComplaintGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
            config=complaint_config,
        )

        # Initialize trigger engine
        event_triggers_config = getattr(self.config, "event_triggers", None)
        trigger_config = (
            event_triggers_config.model_dump()
            if event_triggers_config and hasattr(event_triggers_config, "model_dump")
            else {}
        )
        self.trigger_engine = EventTriggerEngine(self.rng, trigger_config)

        # Track pending cases and complaints for lifecycle processing (INSERT-then-UPDATE)
        self.pending_cases: dict[UUID, dict[str, Any]] = {}
        self.pending_complaints: dict[UUID, dict[str, Any]] = {}

        # Track active customer journeys for survey triggering
        # Key: member_id -> journey data
        # Journey structure:
        # {
        #     "member_id": UUID,
        #     "trigger_event": dict,           # Original claim event
        #     "trigger_type": str,             # "claim_paid" or "claim_rejected"
        #     "start_date": date,
        #     "escalation_type": str | None,   # "interaction", "case", "complaint", None
        #     "interactions": list[UUID],      # Interaction IDs created for this journey
        #     "case_id": UUID | None,          # Case ID if case was created
        #     "complaint_id": UUID | None,     # Complaint ID if complaint was created
        #     "timeout_date": date | None,     # For non-escalation journeys (2-day delay)
        #     "first_contact_resolution": bool | None,  # FCR status if interaction occurred
        #     "resolution_outcome": str | None,  # Complaint outcome if applicable
        # }
        self.active_journeys: dict[UUID, dict[str, Any]] = {}

        # Statistics
        self._stats = {
            "interactions_created": 0,
            "cases_created": 0,
            "complaints_created": 0,
            "cases_resolved": 0,
            "complaints_resolved": 0,
            "baseline_interactions": 0,
            "trigger_interactions": 0,
            "journeys_started": 0,
            "journeys_completed": 0,
            "journeys_with_escalation": 0,
            "journeys_no_escalation": 0,
        }

    def run(self) -> Generator:
        """Main CRM process loop."""
        logger.info(
            "crm_process_started",
            worker_id=self.worker_id,
        )

        # Wait for warmup period (claims need to be generated first)
        while self.sim_env.now < 30:
            yield self.env.timeout(1.0)

        while True:
            current_date = self.sim_env.current_date

            # 1. Process trigger events from Claims/Billing
            self._process_event_queue(current_date)

            # 2. Generate baseline interactions (independent of triggers)
            self._generate_baseline_interactions(current_date)

            # 3. Process case lifecycle (resolutions)
            self._process_case_lifecycle(current_date)

            # 4. Process complaint lifecycle (resolutions)
            self._process_complaint_lifecycle(current_date)

            # 5. Process journey timeouts (non-escalation surveys)
            self._process_journey_timeouts(current_date)

            # Wait for next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _process_event_queue(self, current_date: date) -> None:
        """Process CRM trigger events from Claims/Billing processes."""
        if not self.shared_state:
            return

        events = self.shared_state.get_crm_events()

        for event in events:
            event_type = event.get("event_type", "").lower()

            # Handle claim events - start journey and process CRM actions
            if event_type in ("claim_paid", "claim_rejected"):
                self._start_journey(event, current_date)
                # Continue to execute CRM actions below

            # Handle completion events - check if journey should complete
            # These events are emitted by CRM itself after creating interactions/cases/complaints
            elif event_type in ("interaction_completed", "case_resolved", "complaint_resolved"):
                self._check_journey_completion(event, current_date)
                # Continue to process for other purposes (e.g., CSAT survey triggers)

            # Get context for trigger evaluation
            context = {
                "charge_amount": event.get("charge_amount", 0),
                "attempt_number": event.get("attempt_number", 1),
                "first_contact_resolution": event.get("fcr", True),
            }

            # Determine which CRM actions to trigger
            # Note: For claim events, we use the prediction stored in the journey
            # to ensure unified decision-making
            member_id = event.get("member_id")
            if event_type in ("claim_paid", "claim_rejected") and member_id in self.active_journeys:
                journey = self.active_journeys[member_id]
                triggered_actions = journey.get("triggered_actions", [])
            else:
                triggered_actions = self.trigger_engine.get_triggered_events(
                    event_type, context
                )

            # Execute triggered actions
            for action in triggered_actions:
                self._execute_triggered_action(action, event, context)

    def _execute_triggered_action(
        self,
        action: str,
        event: dict,
        context: dict,
    ) -> None:
        """Execute a triggered CRM action."""
        policy_id = event.get("policy_id")
        member_id = event.get("member_id")
        event_type = event.get("event_type", "").lower()

        if not policy_id or not member_id:
            return

        # Map event type to TriggerEventType enum
        trigger_type = self._get_trigger_event_type(event_type)
        trigger_id = event.get("claim_id") or event.get("invoice_id")

        if action == "interaction":
            self._create_interaction(
                policy_id=policy_id,
                member_id=member_id,
                event_type=event_type,
                trigger_type=trigger_type,
                trigger_id=trigger_id,
                claim_id=event.get("claim_id"),
                invoice_id=event.get("invoice_id"),
            )
            self._stats["trigger_interactions"] += 1

        elif action == "case":
            self._create_case(
                policy_id=policy_id,
                member_id=member_id,
                event_type=event_type,
                claim_id=event.get("claim_id"),
                invoice_id=event.get("invoice_id"),
                charge_amount=context.get("charge_amount"),
            )

        elif action == "complaint":
            self._create_complaint(
                policy_id=policy_id,
                member_id=member_id,
                event_type=event_type,
                claim_id=event.get("claim_id"),
                invoice_id=event.get("invoice_id"),
                charge_amount=context.get("charge_amount"),
            )

    # =========================================================================
    # Journey Lifecycle Methods
    # =========================================================================

    def _start_journey(self, event: dict, current_date: date) -> None:
        """
        Start a new customer journey for a claim event.

        Uses the trigger_engine's predict_escalation for a UNIFIED decision -
        the same prediction determines both CRM activity creation AND journey behavior.

        Args:
            event: The claim event (claim_paid or claim_rejected)
            current_date: Current simulation date
        """
        member_id = event.get("member_id")
        event_type = event.get("event_type", "").lower()

        if not member_id:
            return

        # Check if member already has an active journey
        # If so, we extend the existing journey rather than start a new one
        if member_id in self.active_journeys:
            existing_journey = self.active_journeys[member_id]
            # Only extend if within journey window (e.g., multiple claims)
            days_since_start = (current_date - existing_journey["start_date"]).days
            if days_since_start < 30:  # Within 30-day journey window
                # Update the journey with this new claim event
                existing_journey["additional_claims"] = existing_journey.get(
                    "additional_claims", []
                )
                existing_journey["additional_claims"].append(event)
                return

        # Build context for escalation prediction
        context = self._build_journey_context(event)

        # Use predict_escalation for unified decision
        prediction = self.trigger_engine.predict_escalation(event_type, context)

        # Determine highest escalation level from triggered actions
        triggered_actions = prediction.get("triggered_actions", [])
        escalation_type = self._get_highest_escalation_type(triggered_actions)

        # Create journey structure
        journey = {
            "member_id": member_id,
            "trigger_event": event,
            "trigger_type": event_type,
            "start_date": current_date,
            "escalation_type": escalation_type,
            "highest_level": prediction.get("highest_level"),
            "triggered_actions": triggered_actions,
            "interactions": [],
            "case_id": None,
            "complaint_id": None,
            "timeout_date": None,
            "first_contact_resolution": None,
            "resolution_outcome": None,
            "prediction_factors": prediction.get("factors", {}),
        }

        # Set timeout for non-escalation journeys
        # For claim_paid: 2-day delay (low escalation risk)
        # For claim_rejected with no escalation: 2-day delay
        if escalation_type is None:
            if event_type == "claim_paid":
                # Paid claims: simpler 2-day delay
                journey["timeout_date"] = current_date + timedelta(days=2)
            else:
                # Rejected claims with no predicted escalation: 2-day delay
                journey["timeout_date"] = current_date + timedelta(days=2)
            self._stats["journeys_no_escalation"] += 1
        else:
            self._stats["journeys_with_escalation"] += 1

        self.active_journeys[member_id] = journey
        self._stats["journeys_started"] += 1

        logger.debug(
            "journey_started",
            member_id=str(member_id),
            trigger_type=event_type,
            escalation_type=escalation_type,
            timeout_date=str(journey["timeout_date"]) if journey["timeout_date"] else None,
        )

    def _build_journey_context(self, event: dict) -> dict:
        """
        Build context for escalation prediction from event and member data.

        Includes full factor model:
        - charge_amount
        - denial_reason
        - member_tenure_days
        - prior_claim_rejections
        - digital_engagement

        Args:
            event: The claim event

        Returns:
            Context dictionary for trigger_engine.predict_escalation()
        """
        context = {
            "charge_amount": event.get("charge_amount", 0),
            "denial_reason": event.get("denial_reason"),
        }

        # Try to get member context from shared state
        member_id = event.get("member_id")
        policy_id = event.get("policy_id")

        if self.shared_state and member_id:
            # Get member tenure
            policy_data = self.shared_state.active_policies.get(policy_id)
            if policy_data:
                policy = policy_data.get("policy")
                if policy and hasattr(policy, "start_date"):
                    tenure_days = (self.sim_env.current_date - policy.start_date).days
                    context["member_tenure_days"] = max(0, tenure_days)

            # Get digital engagement level
            engagement = self.shared_state.get_engagement_level(member_id)
            context["digital_engagement"] = engagement or "medium"

            # Get prior claim rejections from recent interactions
            # (This is a simplification - in reality would query claim history)
            recent_interactions = self.shared_state.get_recent_interactions(
                member_id, days=365
            )
            rejection_interactions = [
                i for i in recent_interactions
                if i.get("type") == "CLAIM_DISPUTE"
            ]
            context["prior_claim_rejections"] = len(rejection_interactions)

        return context

    def _get_highest_escalation_type(self, triggered_actions: list[str]) -> str | None:
        """
        Determine the highest escalation type from triggered actions.

        Hierarchy: complaint > case > interaction

        Args:
            triggered_actions: List of triggered CRM actions

        Returns:
            Highest escalation type or None if no escalation
        """
        if "complaint" in triggered_actions:
            return "complaint"
        elif "case" in triggered_actions:
            return "case"
        elif "interaction" in triggered_actions:
            return "interaction"
        return None

    def _check_journey_completion(
        self,
        event: dict,
        current_date: date,
    ) -> None:
        """
        Check if a journey should complete based on a CRM completion event.

        Uses hierarchical completion logic:
        - If complaint was created → wait for complaint_resolved
        - If case was created (but no complaint) → wait for case_resolved
        - If only interaction was created → complete on interaction_completed

        This ensures surveys are sent at the END of the customer journey,
        not at intermediate points.

        Args:
            event: The completion event (interaction_completed, case_resolved, complaint_resolved)
            current_date: Current simulation date
        """
        member_id = event.get("member_id")
        event_type = event.get("event_type", "").lower()

        if not member_id or member_id not in self.active_journeys:
            return

        journey = self.active_journeys[member_id]
        escalation_type = journey.get("escalation_type")

        # Track completion context for survey
        if event_type == "interaction_completed":
            # Store FCR status
            journey["first_contact_resolution"] = event.get("fcr", True)

            # Check if this completes the journey
            # Only complete if interaction was the highest expected escalation
            if escalation_type == "interaction":
                self._complete_journey(member_id, current_date)

        elif event_type == "case_resolved":
            # Store case resolution info
            journey["case_sla_breached"] = event.get("sla_breached", False)

            # Complete if case was the highest expected escalation
            # (no complaint was created)
            if escalation_type == "case":
                self._complete_journey(member_id, current_date)

        elif event_type == "complaint_resolved":
            # Store complaint resolution outcome
            journey["resolution_outcome"] = event.get("resolution_outcome")
            journey["phio_escalated"] = event.get("phio_escalated", False)

            # Complaint is always the highest level, so complete the journey
            if escalation_type == "complaint":
                self._complete_journey(member_id, current_date)

    def _complete_journey(self, member_id: UUID, current_date: date) -> None:
        """
        Complete a journey and emit a survey-ready event with full context.

        This method:
        1. Removes the journey from active tracking
        2. Builds a comprehensive survey event with all journey context
        3. Emits the event to the CRM queue for Survey process to consume

        The emitted event contains everything Survey needs to create an NPS survey
        with rich context for LLM prompt generation.

        Args:
            member_id: The member whose journey is completing
            current_date: Current simulation date
        """
        if member_id not in self.active_journeys:
            return

        journey = self.active_journeys.pop(member_id)
        trigger_event = journey.get("trigger_event", {})

        # Calculate journey duration
        start_date = journey.get("start_date", current_date)
        days_to_resolution = (current_date - start_date).days

        # Build detailed survey event with full context
        survey_event = {
            "event_type": "journey_completed",
            "survey_type": "nps",
            "timestamp": self.sim_env.current_datetime,

            # Member/Policy identifiers
            "member_id": member_id,
            "policy_id": trigger_event.get("policy_id"),

            # Original trigger context
            "trigger_type": journey.get("trigger_type"),
            "claim_id": trigger_event.get("claim_id"),
            "charge_amount": trigger_event.get("charge_amount"),
            "denial_reason": trigger_event.get("denial_reason"),

            # Journey context
            "escalated": journey.get("escalation_type") is not None,
            "escalation_type": journey.get("escalation_type"),
            "highest_level": journey.get("highest_level"),
            "interactions_count": len(journey.get("interactions", [])),
            "had_case": journey.get("case_id") is not None,
            "had_complaint": journey.get("complaint_id") is not None,
            "days_to_resolution": days_to_resolution,

            # Resolution context (from CRM activity)
            "first_contact_resolution": journey.get("first_contact_resolution"),
            "resolution_outcome": journey.get("resolution_outcome"),
            "case_sla_breached": journey.get("case_sla_breached"),
            "phio_escalated": journey.get("phio_escalated"),

            # Escalation prediction factors (for transparency/debugging)
            "prediction_factors": journey.get("prediction_factors", {}),

            # Additional claims if multiple in journey
            "additional_claims_count": len(journey.get("additional_claims", [])),
        }

        # Emit to CRM queue for Survey process to consume
        if self.shared_state:
            self.shared_state.add_crm_event(survey_event)

        self._stats["journeys_completed"] += 1

        logger.debug(
            "journey_completed",
            member_id=str(member_id),
            trigger_type=journey.get("trigger_type"),
            escalation_type=journey.get("escalation_type"),
            days_to_resolution=days_to_resolution,
            resolution_outcome=journey.get("resolution_outcome"),
        )

    def _process_journey_timeouts(self, current_date: date) -> None:
        """
        Process journeys that have timed out (no escalation occurred).

        For journeys where no CRM activity was predicted (escalation_type is None),
        we set a 2-day timeout. After the timeout, if no unexpected escalation
        occurred, we complete the journey and emit the survey event.

        This handles the case where:
        - Claim was paid → member didn't contact CRM → survey after 2 days
        - Claim was rejected but low escalation probability → no contact → survey after 2 days

        Args:
            current_date: Current simulation date
        """
        for member_id, journey in list(self.active_journeys.items()):
            timeout_date = journey.get("timeout_date")

            # Check if journey has timed out
            if timeout_date and current_date >= timeout_date:
                # Check if any unexpected escalation occurred
                # (member contacted CRM even though we didn't predict it)
                if journey.get("interactions") or journey.get("case_id") or journey.get("complaint_id"):
                    # Unexpected escalation - extend the journey
                    # Recalculate expected completion based on what was created
                    if journey.get("complaint_id"):
                        journey["escalation_type"] = "complaint"
                    elif journey.get("case_id"):
                        journey["escalation_type"] = "case"
                    elif journey.get("interactions"):
                        journey["escalation_type"] = "interaction"

                    # Remove timeout - will complete via normal completion flow
                    journey["timeout_date"] = None
                    logger.debug(
                        "journey_timeout_extended",
                        member_id=str(member_id),
                        new_escalation_type=journey.get("escalation_type"),
                    )
                else:
                    # No escalation occurred - complete the journey
                    self._complete_journey(member_id, current_date)

    def _create_interaction(
        self,
        policy_id: UUID,
        member_id: UUID,
        event_type: str,
        trigger_type: Optional[TriggerEventType],
        trigger_id: Optional[UUID],
        claim_id: Optional[UUID] = None,
        invoice_id: Optional[UUID] = None,
    ) -> InteractionCreate:
        """Create an interaction record."""
        interaction_type_code = self.trigger_engine.get_interaction_type_for_trigger(
            event_type
        )

        interaction = self.interaction_gen.generate(
            policy_id=policy_id,
            member_id=member_id,
            interaction_type_code=interaction_type_code,
            trigger_event_type=trigger_type,
            trigger_event_id=trigger_id,
            claim_id=claim_id,
            invoice_id=invoice_id,
        )

        # INSERT to database
        self.batch_writer.add("interaction", interaction.model_dump_db())
        self._stats["interactions_created"] += 1

        # Link to active journey if one exists
        if member_id in self.active_journeys:
            journey = self.active_journeys[member_id]
            journey["interactions"].append(interaction.interaction_id)
            # Store FCR for journey context
            journey["first_contact_resolution"] = interaction.first_contact_resolution

        # Track in shared state for survey triggering
        if self.shared_state:
            self.shared_state.add_interaction(
                member_id,
                {
                    "interaction_id": interaction.interaction_id,
                    "timestamp": interaction.start_datetime,
                    "fcr": interaction.first_contact_resolution,
                    "type": interaction_type_code,
                },
            )

            # Emit event for potential CSAT survey (CRM queue)
            self.shared_state.add_crm_event(
                {
                    "event_type": "interaction_completed",
                    "interaction_id": interaction.interaction_id,
                    "policy_id": policy_id,
                    "member_id": member_id,
                    "fcr": interaction.first_contact_resolution,
                    "timestamp": interaction.start_datetime,
                }
            )

            # Emit event for transactional communications (Communication queue)
            # Include original trigger so CommunicationProcess knows which template to use
            trigger_type_str = (
                trigger_type.value if trigger_type and hasattr(trigger_type, "value")
                else str(trigger_type) if trigger_type else None
            )
            self.shared_state.add_communication_event(
                {
                    "event_type": "interaction_completed",
                    "interaction_id": interaction.interaction_id,
                    "trigger_event_type": trigger_type_str,
                    "policy_id": policy_id,
                    "member_id": member_id,
                    "claim_id": claim_id,
                    "invoice_id": invoice_id,
                    "timestamp": interaction.start_datetime,
                }
            )

        return interaction

    def _create_case(
        self,
        policy_id: UUID,
        member_id: UUID,
        event_type: str,
        claim_id: Optional[UUID] = None,
        invoice_id: Optional[UUID] = None,
        charge_amount: Optional[Decimal] = None,
        source_interaction_id: Optional[UUID] = None,
    ) -> CaseCreate:
        """Create a service case."""
        case_type_code = self.trigger_engine.get_case_type_for_trigger(event_type)

        # Determine priority based on charge amount
        priority = None
        if charge_amount and charge_amount > Decimal("1000"):
            priority = CasePriority.HIGH

        case = self.case_gen.generate(
            policy_id=policy_id,
            member_id=member_id,
            case_type_code=case_type_code,
            source_interaction_id=source_interaction_id,
            related_claim_id=claim_id,
            related_invoice_id=invoice_id,
            priority_override=priority,
        )

        # INSERT to database (initial state: OPEN)
        self.batch_writer.add("service_case", case.model_dump_db())
        self._stats["cases_created"] += 1

        # Link to active journey if one exists
        if member_id in self.active_journeys:
            journey = self.active_journeys[member_id]
            journey["case_id"] = case.case_id

        # Track for lifecycle processing (will UPDATE to RESOLVED later)
        self._schedule_case_resolution(case)

        return case

    def _create_complaint(
        self,
        policy_id: UUID,
        member_id: UUID,
        event_type: str,
        claim_id: Optional[UUID] = None,
        invoice_id: Optional[UUID] = None,
        charge_amount: Optional[Decimal] = None,
    ) -> ComplaintCreate:
        """Create a complaint record."""
        category_code = self.trigger_engine.get_complaint_category_for_trigger(
            event_type
        )

        complaint = self.complaint_gen.generate(
            policy_id=policy_id,
            member_id=member_id,
            category_code=category_code,
            related_claim_id=claim_id,
            related_invoice_id=invoice_id,
            charge_amount=Decimal(str(charge_amount)) if charge_amount else None,
        )

        # INSERT to database (initial state: RECEIVED)
        self.batch_writer.add("complaint", complaint.model_dump_db())
        self._stats["complaints_created"] += 1

        # Link to active journey if one exists
        if member_id in self.active_journeys:
            journey = self.active_journeys[member_id]
            journey["complaint_id"] = complaint.complaint_id

        # Track for lifecycle processing (will UPDATE through states)
        self._schedule_complaint_resolution(complaint)

        return complaint

    def _generate_baseline_interactions(self, current_date: date) -> None:
        """Generate baseline interactions (independent of triggers)."""
        if not self.shared_state:
            return

        # Get baseline rate from config (2.5 contacts per member per year)
        crm_config = getattr(self.config, "crm", None)
        if crm_config and hasattr(crm_config, "interaction"):
            interaction_config = crm_config.interaction
            annual_rate = getattr(interaction_config, "baseline_contacts_per_year", 2.5)
        else:
            annual_rate = 2.5

        daily_rate = annual_rate / 365

        # For each active policy member
        for pm_id, member_data in self.shared_state.policy_members.items():
            # Check if member has an interaction today (Poisson process)
            if self.rng.random() < daily_rate:
                policy = member_data.get("policy")
                member = member_data.get("member")

                if policy and member:
                    # Sample interaction type for baseline contact
                    baseline_types = [
                        "GENERAL_INQUIRY",
                        "COVER_INQUIRY",
                        "BENEFIT_INQUIRY",
                        "POLICY_INFO",
                        "MEMBERSHIP_CARD",
                    ]
                    interaction_type = self.rng.choice(baseline_types)

                    interaction = self.interaction_gen.generate(
                        policy_id=policy.policy_id,
                        member_id=member.member_id,
                        interaction_type_code=interaction_type,
                    )

                    self.batch_writer.add("interaction", interaction.model_dump_db())
                    self._stats["interactions_created"] += 1
                    self._stats["baseline_interactions"] += 1

    def _schedule_case_resolution(self, case: CaseCreate) -> None:
        """Schedule a case for future UPDATE to RESOLVED status."""
        # Get case config
        crm_config = getattr(self.config, "crm", None)
        case_config = getattr(crm_config, "case", None) if crm_config else None

        # Get resolution time params
        resolution_params = {}
        if case_config and hasattr(case_config, "resolution_time_params"):
            resolution_params = case_config.resolution_time_params

        priority_name = (
            case.priority.value if hasattr(case.priority, "value") else str(case.priority)
        )

        # Get params for this priority
        params = resolution_params.get(priority_name, {})
        if hasattr(params, "mu"):
            mu = params.mu
            sigma = params.sigma
        elif isinstance(params, dict):
            mu = params.get("mu", 3.2)
            sigma = params.get("sigma", 0.7)
        else:
            mu = 3.2
            sigma = 0.7

        # Sample resolution time (lognormal hours)
        resolution_hours = self.rng.lognormal(mu, sigma)
        resolution_hours = min(resolution_hours, 720)  # Cap at 30 days

        resolution_date = self.sim_env.current_datetime + timedelta(hours=resolution_hours)

        # Check SLA breach
        sla_breach_rates = {}
        if case_config and hasattr(case_config, "sla_breach_rates"):
            sla_breach_rates = case_config.sla_breach_rates

        breach_rate = sla_breach_rates.get(priority_name, 0.08)
        sla_breached = self.rng.random() < breach_rate

        self.pending_cases[case.case_id] = {
            "case": case,
            "resolution_date": resolution_date.date(),
            "sla_breached": sla_breached,
        }

    def _schedule_complaint_resolution(self, complaint: ComplaintCreate) -> None:
        """Schedule a complaint for future UPDATE through states."""
        # Get complaint config
        crm_config = getattr(self.config, "crm", None)
        complaint_config = getattr(crm_config, "complaint", None) if crm_config else None

        # Sample resolution time (lognormal, median ~30 days)
        resolution_days = self.rng.lognormal(3.40, 0.69)
        resolution_days = min(resolution_days, 180)  # Cap at 6 months

        resolution_date = self.sim_env.current_date + timedelta(days=int(resolution_days))

        # Sample resolution outcome
        outcomes = {"NotUpheld": 0.45, "PartiallyUpheld": 0.30, "Upheld": 0.20, "Withdrawn": 0.05}
        if complaint_config and hasattr(complaint_config, "resolution_outcomes"):
            outcomes = complaint_config.resolution_outcomes

        outcome_names = list(outcomes.keys())
        outcome_probs = list(outcomes.values())
        # Normalize probabilities
        total = sum(outcome_probs)
        outcome_probs = [p / total for p in outcome_probs]
        outcome = self.rng.choice(outcome_names, p=outcome_probs)

        # Determine PHIO escalation
        phio_rate = 0.08
        if complaint_config and hasattr(complaint_config, "phio_escalation_rate"):
            phio_rate = complaint_config.phio_escalation_rate
        phio_escalated = self.rng.random() < phio_rate

        self.pending_complaints[complaint.complaint_id] = {
            "complaint": complaint,
            "resolution_date": resolution_date,
            "resolution_outcome": outcome,
            "phio_escalated": phio_escalated,
            "acknowledged": False,
        }

    def _process_case_lifecycle(self, current_date: date) -> None:
        """Process case resolutions (UPDATE cases to RESOLVED)."""
        for case_id, data in list(self.pending_cases.items()):
            resolution_date = data["resolution_date"]

            if current_date >= resolution_date:
                # Resolve the case
                self._resolve_case(case_id, data)
                del self.pending_cases[case_id]

    def _resolve_case(self, case_id: UUID, data: dict) -> None:
        """UPDATE case status to RESOLVED."""
        # Flush if still in buffer to ensure INSERT is committed for CDC
        self.batch_writer.flush_for_cdc("service_case", "case_id", case_id)

        updates = {
            "status": CaseStatus.RESOLVED.value,
            "resolution_date": self.sim_env.current_datetime.isoformat(),
            "sla_breached": data["sla_breached"],
            "modified_at": self.sim_env.current_datetime.isoformat(),
            "modified_by": "SIMULATION",
        }

        self.batch_writer.update_record("service_case", "case_id", case_id, updates)
        self._stats["cases_resolved"] += 1

        # Emit event for CSAT survey
        if self.shared_state:
            case = data["case"]
            self.shared_state.add_crm_event(
                {
                    "event_type": "case_resolved",
                    "case_id": case_id,
                    "policy_id": case.policy_id,
                    "member_id": case.member_id,
                    "sla_breached": data["sla_breached"],
                    "timestamp": self.sim_env.current_datetime,
                }
            )

    def _process_complaint_lifecycle(self, current_date: date) -> None:
        """Process complaint lifecycle (RECEIVED -> ACKNOWLEDGED -> RESOLVED)."""
        for complaint_id, data in list(self.pending_complaints.items()):
            complaint = data["complaint"]

            # Acknowledge after 1-2 days
            if not data["acknowledged"]:
                days_since_received = (current_date - complaint.received_date).days
                if days_since_received >= 1:
                    self._acknowledge_complaint(complaint_id)
                    data["acknowledged"] = True

            # Resolve on resolution date
            if current_date >= data["resolution_date"]:
                self._resolve_complaint(complaint_id, data)
                del self.pending_complaints[complaint_id]

    def _acknowledge_complaint(self, complaint_id: UUID) -> None:
        """UPDATE complaint status to ACKNOWLEDGED."""
        self.batch_writer.flush_for_cdc("complaint", "complaint_id", complaint_id)

        updates = {
            "status": ComplaintStatus.ACKNOWLEDGED.value,
            "acknowledged_date": self.sim_env.current_date.isoformat(),
            "modified_at": self.sim_env.current_datetime.isoformat(),
            "modified_by": "SIMULATION",
        }

        self.batch_writer.update_record("complaint", "complaint_id", complaint_id, updates)

    def _resolve_complaint(self, complaint_id: UUID, data: dict) -> None:
        """UPDATE complaint status to RESOLVED."""
        self.batch_writer.flush_for_cdc("complaint", "complaint_id", complaint_id)

        updates = {
            "status": ComplaintStatus.RESOLVED.value,
            "resolution_date": self.sim_env.current_date.isoformat(),
            "resolution_outcome": data["resolution_outcome"],
            "phio_escalated": data["phio_escalated"],
            "modified_at": self.sim_env.current_datetime.isoformat(),
            "modified_by": "SIMULATION",
        }

        if data["phio_escalated"]:
            complaint = data["complaint"]
            escalation_date = complaint.received_date + timedelta(days=14)
            updates["phio_escalation_date"] = escalation_date.isoformat()

        self.batch_writer.update_record("complaint", "complaint_id", complaint_id, updates)
        self._stats["complaints_resolved"] += 1

        # Emit event for NPS survey
        if self.shared_state:
            complaint = data["complaint"]
            self.shared_state.add_crm_event(
                {
                    "event_type": "complaint_resolved",
                    "complaint_id": complaint_id,
                    "policy_id": complaint.policy_id,
                    "member_id": complaint.member_id,
                    "resolution_outcome": data["resolution_outcome"],
                    "timestamp": self.sim_env.current_datetime,
                }
            )

    def _get_trigger_event_type(self, event_type: str) -> Optional[TriggerEventType]:
        """Convert string event type to TriggerEventType enum."""
        mapping = {
            "claim_submitted": TriggerEventType.CLAIM_SUBMITTED,
            "claim_rejected": TriggerEventType.CLAIM_REJECTED,
            "claim_delayed": TriggerEventType.CLAIM_DELAYED,
            "claim_paid": TriggerEventType.CLAIM_PAID,
            "payment_failed": TriggerEventType.PAYMENT_FAILED,
            "arrears_created": TriggerEventType.ARREARS_CREATED,
            "policy_suspended": TriggerEventType.POLICY_SUSPENDED,
        }
        return mapping.get(event_type)

    def _log_progress(self) -> None:
        """Log process statistics."""
        logger.info(
            "crm_process_progress",
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            interactions_created=self._stats["interactions_created"],
            cases_created=self._stats["cases_created"],
            complaints_created=self._stats["complaints_created"],
            cases_resolved=self._stats["cases_resolved"],
            complaints_resolved=self._stats["complaints_resolved"],
            baseline_interactions=self._stats["baseline_interactions"],
            trigger_interactions=self._stats["trigger_interactions"],
            pending_cases=len(self.pending_cases),
            pending_complaints=len(self.pending_complaints),
            journeys_started=self._stats["journeys_started"],
            journeys_completed=self._stats["journeys_completed"],
            journeys_with_escalation=self._stats["journeys_with_escalation"],
            journeys_no_escalation=self._stats["journeys_no_escalation"],
            active_journeys=len(self.active_journeys),
        )
