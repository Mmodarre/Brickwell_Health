"""
Survey Process for Brickwell Health Simulator.

Generates pending NPS and CSAT surveys based on trigger events.
NO LLM calls - surveys are populated post-simulation using Databricks ai_query.
"""

from datetime import date, datetime, timedelta
from typing import Any, Generator, Optional, TYPE_CHECKING
from uuid import UUID

import structlog
from sqlalchemy import text

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.survey import NPSSurveyPendingCreate, CSATSurveyPendingCreate
from brickwell_health.domain.enums import SurveyType
from brickwell_health.generators.survey_generator import SurveyGenerator

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class SurveyProcess(BaseProcess):
    """
    Survey process for generating pending NPS and CSAT surveys.

    This process:
    1. Consumes CRM events for survey triggers (per design decision #2)
    2. Predicts member response probability
    3. Pre-calculates response timing (per design decision #4)
    4. Builds LLM context from simulation state
    5. Writes to pending tables for post-simulation LLM processing

    NO LLM calls are made during simulation.
    """

    def __init__(
        self,
        *args: Any,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """
        Initialize the survey process.

        Args:
            shared_state: Shared state for cross-process communication
        """
        super().__init__(*args, **kwargs)
        self.shared_state = shared_state

        # Get survey configuration
        survey_config = getattr(self.config, "survey", None)
        llm_config = getattr(self.config, "llm", None)

        # Extract config as dict
        if survey_config is None:
            survey_config_dict = {}
        elif hasattr(survey_config, "model_dump"):
            survey_config_dict = survey_config.model_dump()
        elif hasattr(survey_config, "__dict__"):
            survey_config_dict = vars(survey_config)
        else:
            survey_config_dict = {}

        if llm_config is None:
            llm_config_dict = {}
        elif hasattr(llm_config, "model_dump"):
            llm_config_dict = llm_config.model_dump()
        elif hasattr(llm_config, "__dict__"):
            llm_config_dict = vars(llm_config)
        else:
            llm_config_dict = {}

        # Merge configs for generator
        generator_config = {
            "nps": survey_config_dict.get("nps", {}),
            "csat": survey_config_dict.get("csat", {}),
            "llm": llm_config_dict,
        }

        # Initialize generator
        self.survey_gen = SurveyGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            self.sim_env,
            config=generator_config,
        )

        # Configuration - extract trigger configs
        nps_config = survey_config_dict.get("nps", {})
        csat_config = survey_config_dict.get("csat", {})

        self.nps_triggers = nps_config.get("triggers", {})
        self.csat_triggers = csat_config.get("triggers", {})

        # Track surveys per member to prevent fatigue
        self.surveys_sent: dict[UUID, list[dict]] = {}

        # Statistics
        self._stats = {
            "nps_pending_created": 0,
            "csat_pending_created": 0,
            "nps_will_respond": 0,
            "csat_will_respond": 0,
            "surveys_suppressed_fatigue": 0,
            "anniversary_surveys": 0,
            "journey_nps_created": 0,
            "journey_claim_paid": 0,
            "journey_claim_rejected": 0,
            "journey_with_escalation": 0,
            "journey_no_escalation": 0,
        }

    def run(self) -> Generator:
        """Main survey process loop."""
        # Wait for warmup (60 days for CRM events to exist)
        while self.sim_env.now < 60:
            yield self.env.timeout(1.0)

        while True:
            current_date = self.sim_env.current_date

            # Process survey triggers from CRM events (CONSUME events per decision #2)
            self._process_survey_triggers(current_date)

            # Generate anniversary surveys (monthly check)
            self._generate_anniversary_surveys(current_date)

            # Wait for next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _process_survey_triggers(self, current_date: date) -> None:
        """Process CRM events that trigger surveys."""
        if not self.shared_state:
            return

        # CONSUME CRM events (per design decision #2)
        events = self.shared_state.get_crm_events()

        for event in events:
            event_type = event.get("event_type", "")

            # Journey-based NPS surveys (claim events with full context)
            # This is the NEW path for claim_paid/claim_rejected surveys
            if event_type == "journey_completed":
                self._create_journey_nps_survey(event)
                continue  # Journey events don't trigger CSAT

            # NPS Survey triggers (legacy path for non-journey events)
            self._maybe_create_nps_survey(event, event_type)

            # CSAT Survey triggers
            self._maybe_create_csat_survey(event, event_type)

    def _maybe_create_nps_survey(self, event: dict, event_type: str) -> None:
        """Maybe create a pending NPS survey based on event."""
        # Map event type to trigger config key
        trigger_key = self._get_nps_trigger_key(event_type)
        if not trigger_key:
            return

        trigger_config = self.nps_triggers.get(trigger_key, {})

        # Handle both dict config and simple float
        if isinstance(trigger_config, dict):
            send_probability = trigger_config.get("send_probability", 0)
        else:
            send_probability = float(trigger_config) if trigger_config else 0

        # Check if survey should be sent
        if send_probability <= 0 or self.rng.random() >= send_probability:
            return

        member_id = event.get("member_id")
        policy_id = event.get("policy_id")

        if not member_id or not policy_id:
            return

        # Check survey fatigue (max 2 per 30 days)
        if self._is_survey_fatigued(member_id):
            self._stats["surveys_suppressed_fatigue"] += 1
            return

        # Get member and policy data
        member_data = self._get_member_data(member_id)
        policy_data = self._get_policy_data(policy_id)

        if not member_data or not policy_data:
            return

        # Get trigger entity (claim, etc.)
        trigger_entity = self._get_trigger_entity(event)

        # Get history for context
        claims_history = self._get_claims_history(member_id)
        interaction_history = self._get_interaction_history(member_id)
        billing_status = self._get_billing_status(policy_id)
        digital_engagement = self._get_digital_engagement(member_id)

        # Determine survey type
        survey_type = self._get_nps_survey_type(event_type)

        # Generate pending survey
        coverages = policy_data.get("coverages") if policy_data else None
        pending_survey = self.survey_gen.generate_nps_pending(
            member_data=member_data,
            policy_data=policy_data,
            survey_type=survey_type,
            trigger_event=event_type,
            trigger_entity=trigger_entity,
            claim_id=event.get("claim_id"),
            interaction_id=event.get("interaction_id"),
            claims_history=claims_history,
            interaction_history=interaction_history,
            billing_status=billing_status,
            digital_engagement=digital_engagement,
            coverages=coverages,
            active_policies=self.shared_state.active_policies if self.shared_state else None,
            policy_id=policy_id,
        )

        # Write to database
        self.batch_writer.add("nps_survey_pending", pending_survey.model_dump_db())
        self._stats["nps_pending_created"] += 1

        if pending_survey.will_respond:
            self._stats["nps_will_respond"] += 1

        # Track for fatigue
        self._track_survey_sent(member_id, "NPS")

    def _maybe_create_csat_survey(self, event: dict, event_type: str) -> None:
        """Maybe create a pending CSAT survey based on event."""
        # CSAT surveys triggered by interaction/case events
        if event_type not in ["interaction_completed", "case_resolved"]:
            return

        # Get trigger probability
        trigger_probability = self.csat_triggers.get(event_type, 0)
        if isinstance(trigger_probability, dict):
            trigger_probability = trigger_probability.get("send_probability", 0)

        if trigger_probability <= 0 or self.rng.random() >= trigger_probability:
            return

        member_id = event.get("member_id")
        policy_id = event.get("policy_id")

        if not member_id or not policy_id:
            return

        # Check survey fatigue
        if self._is_survey_fatigued(member_id):
            self._stats["surveys_suppressed_fatigue"] += 1
            return

        # Get data
        member_data = self._get_member_data(member_id)
        policy_data = self._get_policy_data(policy_id)

        if not member_data or not policy_data:
            return

        # Get interaction/case data
        interaction_data = {
            "interaction_id": event.get("interaction_id"),
            "fcr": event.get("fcr", True),
            "first_contact_resolution": event.get("first_contact_resolution", event.get("fcr", True)),
            "channel": event.get("channel"),
            "type": event.get("interaction_type"),
            "interaction_type": event.get("interaction_type"),
            "duration_seconds": event.get("duration_seconds", 300),
            "wait_time_seconds": event.get("wait_time_seconds"),
            "trigger_event_type": event.get("trigger_event_type"),
        }

        case_data = None
        if event_type == "case_resolved":
            case_data = {
                "case_id": event.get("case_id"),
                "resolved": True,
                "sla_breached": event.get("sla_breached", False),
                "case_type": event.get("case_type"),
            }

        # Determine survey type
        survey_type = (
            SurveyType.POST_INTERACTION
            if event_type == "interaction_completed"
            else SurveyType.POST_COMPLAINT_RESOLUTION
        )

        # Generate pending survey
        pending_survey = self.survey_gen.generate_csat_pending(
            member_data=member_data,
            policy_data=policy_data,
            survey_type=survey_type,
            interaction_data=interaction_data,
            case_data=case_data,
        )

        # Write to database
        self.batch_writer.add("csat_survey_pending", pending_survey.model_dump_db())
        self._stats["csat_pending_created"] += 1

        if pending_survey.will_respond:
            self._stats["csat_will_respond"] += 1

        # Track for fatigue
        self._track_survey_sent(member_id, "CSAT")

    def _create_journey_nps_survey(self, event: dict) -> None:
        """
        Create an NPS survey from a completed journey event.

        Journey events contain rich context about the full customer journey:
        - Original trigger (claim_paid/claim_rejected)
        - Whether escalation occurred (interaction, case, complaint)
        - Resolution outcome if applicable
        - Days to resolution
        - FCR status

        This provides much richer context for LLM prompt generation than
        raw claim events would.

        For Family policies, NPS surveys are sent to the primary member
        (policy holder), not the dependent who made the claim, as NPS
        measures overall satisfaction with the insurance company.

        Args:
            event: Journey completed event from CRM process
        """
        claim_member_id = event.get("member_id")  # Member who made the claim
        policy_id = event.get("policy_id")
        trigger_type = event.get("trigger_type", "")

        if not claim_member_id or not policy_id:
            return

        # Get policy data to check if it's a Family policy
        policy_data = self._get_policy_data(policy_id)
        if not policy_data:
            return

        policy = policy_data.get("policy")
        policy_type = policy.policy_type.value if policy and hasattr(policy, "policy_type") else None

        # For Family/Couple policies, use primary member for NPS surveys
        # NPS surveys measure overall satisfaction, which is the policy holder's responsibility
        if policy_type in ("Family", "Couple", "Single Parent"):
            member_id = self._get_primary_member_id(policy_id)
            if not member_id:
                # Fallback to claim member if primary not found
                member_id = claim_member_id
                logger.warning(
                    "primary_member_not_found",
                    policy_id=str(policy_id),
                    claim_member_id=str(claim_member_id),
                    policy_type=policy_type,
                )
        else:
            # Single policy - use the claim member
            member_id = claim_member_id

        # Check survey fatigue (max 2 per 30 days) - check primary member for Family policies
        if self._is_survey_fatigued(member_id):
            self._stats["surveys_suppressed_fatigue"] += 1
            return

        # Get NPS trigger config based on original trigger type
        trigger_key = trigger_type.lower().replace("_", "_")  # claim_paid, claim_rejected
        trigger_config = self.nps_triggers.get(trigger_key, {})

        # Handle both dict config and simple float
        if isinstance(trigger_config, dict):
            send_probability = trigger_config.get("send_probability", 0.30)
        else:
            send_probability = float(trigger_config) if trigger_config else 0.30

        # Check if survey should be sent
        if send_probability <= 0 or self.rng.random() >= send_probability:
            return

        # Get member and policy data
        member_data = self._get_member_data(member_id)
        policy_data = self._get_policy_data(policy_id)

        if not member_data or not policy_data:
            return

        # Build rich trigger entity from journey context
        trigger_entity = self._build_journey_trigger_entity(event)

        # Get history for context
        claims_history = self._get_claims_history(member_id)
        interaction_history = self._get_interaction_history(member_id)
        billing_status = self._get_billing_status(policy_id)
        digital_engagement = self._get_digital_engagement(member_id)

        # Determine survey type based on original trigger
        if trigger_type == "claim_paid":
            survey_type = SurveyType.POST_CLAIM
        elif trigger_type == "claim_rejected":
            survey_type = SurveyType.POST_CLAIM
        else:
            survey_type = SurveyType.POST_CLAIM

        # Generate pending survey with journey context
        # Journey context is merged into trigger_entity for LLM prompt generation
        coverages = policy_data.get("coverages") if policy_data else None
        pending_survey = self.survey_gen.generate_nps_pending(
            member_data=member_data,
            policy_data=policy_data,
            survey_type=survey_type,
            trigger_event=trigger_type,
            trigger_entity=trigger_entity,
            claim_id=event.get("claim_id"),
            interaction_id=None,  # Journey may have multiple interactions
            claims_history=claims_history,
            interaction_history=interaction_history,
            billing_status=billing_status,
            digital_engagement=digital_engagement,
            coverages=coverages,
            active_policies=self.shared_state.active_policies if self.shared_state else None,
            policy_id=policy_id,
        )

        # Write to database
        self.batch_writer.add("nps_survey_pending", pending_survey.model_dump_db())
        self._stats["nps_pending_created"] += 1
        self._stats["journey_nps_created"] += 1

        # Track trigger type
        if trigger_type == "claim_paid":
            self._stats["journey_claim_paid"] += 1
        elif trigger_type == "claim_rejected":
            self._stats["journey_claim_rejected"] += 1

        # Track escalation
        if event.get("escalated"):
            self._stats["journey_with_escalation"] += 1
        else:
            self._stats["journey_no_escalation"] += 1

        if pending_survey.will_respond:
            self._stats["nps_will_respond"] += 1

        # Track for fatigue
        self._track_survey_sent(member_id, "NPS")

        logger.debug(
            "journey_nps_survey_created",
            member_id=str(member_id),
            trigger_type=trigger_type,
            escalated=event.get("escalated"),
            days_to_resolution=event.get("days_to_resolution"),
        )

    def _build_journey_trigger_entity(self, event: dict) -> dict:
        """
        Build a rich trigger entity from journey completed event.

        This provides more context than the simple trigger entity from
        raw claim events. Maps fields to match what LLMContextBuilder._format_trigger_entity()
        expects.

        Handles both claim-based journeys (claim_paid/claim_rejected) and
        interaction-based journeys (interaction_completed).

        Note: Some fields (service_type, clinical_category, benefit_paid, gap_amount, date)
        may be None because they're not included in the journey event. These could be populated
        by looking up the claim from the database using claim_id, but that would require
        a database query during simulation which may impact performance.

        Args:
            event: Journey completed event

        Returns:
            Trigger entity dict for LLM context with all expected fields
        """
        trigger_type = event.get("trigger_type", "")
        claim_id = event.get("claim_id")
        interaction_id = event.get("interaction_id")

        # Handle interaction-based triggers
        if trigger_type == "interaction_completed" or interaction_id:
            return {
                "id": interaction_id,
                "interaction_date": event.get("interaction_date") or event.get("date"),
                "date": event.get("interaction_date") or event.get("date"),
                "type": event.get("interaction_type"),
                "interaction_type": event.get("interaction_type"),
                "channel": event.get("channel"),
                "duration_minutes": (event.get("duration_seconds", 0) or 0) / 60.0 if event.get("duration_seconds") else None,
                "wait_time_minutes": (event.get("wait_time_seconds", 0) or 0) / 60.0 if event.get("wait_time_seconds") else None,
                "resolved": event.get("first_contact_resolution") or event.get("fcr"),
                "first_contact_resolution": event.get("first_contact_resolution") or event.get("fcr"),
                "related_to": event.get("trigger_event_type"),
                "trigger_event_type": event.get("trigger_event_type"),
            }

        # Handle claim-based triggers
        # Try to look up claim details from batch writer buffer if available
        claim_details = self._lookup_claim_details(claim_id) if claim_id else {}

        # Build base entity with fields expected by LLMContextBuilder._format_trigger_entity()
        entity = {
            # Core claim fields (from LLMContextBuilder expectations)
            "id": claim_id,
            "date": claim_details.get("service_date") or claim_details.get("date"),
            "service_date": claim_details.get("service_date") or claim_details.get("date"),
            "service_type": claim_details.get("service_type") or claim_details.get("claim_type"),
            "clinical_category": claim_details.get("clinical_category"),
            "total_charge": event.get("charge_amount") or claim_details.get("total_charge"),
            "charge_amount": event.get("charge_amount") or claim_details.get("total_charge"),
            "benefit_paid": claim_details.get("benefit_paid") or claim_details.get("benefit_amount"),
            "gap_amount": claim_details.get("gap_amount"),
            "status": "Paid" if trigger_type == "claim_paid" else ("Rejected" if trigger_type == "claim_rejected" else None),
            "processing_days": event.get("days_to_resolution"),
            "rejection_reason": event.get("denial_reason") if trigger_type == "claim_rejected" else None,
            
            # Journey-specific context (additional fields for richer LLM prompts)
            "customer_escalated": event.get("escalated", False),
        }

        # Add journey escalation context if escalated
        if event.get("escalated"):
            entity["escalation_type"] = event.get("escalation_type")

            if event.get("had_complaint"):
                entity["complaint_outcome"] = event.get("resolution_outcome")
                entity["phio_escalated"] = event.get("phio_escalated")

            if event.get("had_case"):
                entity["case_sla_breached"] = event.get("case_sla_breached")

            entity["interactions_count"] = event.get("interactions_count", 0)
            entity["first_contact_resolution"] = event.get("first_contact_resolution")

        return entity

    def _lookup_claim_details(self, claim_id: UUID) -> dict:
        """
        Look up claim details from batch writer buffer if available.

        This is a best-effort lookup - claims may not be in the buffer if they've
        already been flushed to the database. For a complete solution, we would
        need to query the database, but that would impact simulation performance.

        Args:
            claim_id: The claim UUID to look up

        Returns:
            Dictionary with claim details, or empty dict if not found
        """
        if not claim_id or not hasattr(self.batch_writer, 'get_record'):
            return {}

        # Try to get claim from buffer (if batch_writer supports lookup)
        # Note: BatchWriter doesn't currently have a lookup method, so this
        # would need to be implemented or we'd need to query the database
        # For now, return empty dict - fields will be None
        return {}

    def _generate_anniversary_surveys(self, current_date: date) -> None:
        """Generate annual NPS surveys for policy anniversaries."""
        if not self.shared_state:
            return

        # Only check on first of month
        if current_date.day != 1:
            return

        for policy_id, policy_data in self.shared_state.active_policies.items():
            start_date = policy_data.get("start_date")
            if not start_date:
                # Try to get from policy object
                policy = policy_data.get("policy")
                if policy and hasattr(policy, "start_date"):
                    start_date = policy.start_date
                else:
                    continue

            # Check if this month is anniversary
            if start_date.month != current_date.month:
                continue

            # Get primary member
            primary_member_id = policy_data.get("primary_member_id")
            if not primary_member_id:
                # Try to find primary member
                for pm_id, pm_data in self.shared_state.policy_members.items():
                    pm_policy = pm_data.get("policy")
                    if pm_policy and pm_policy.policy_id == policy_id:
                        member = pm_data.get("member")
                        if member:
                            primary_member_id = member.member_id
                            break

            if not primary_member_id:
                continue

            # Check send probability
            trigger_config = self.nps_triggers.get("policy_anniversary", {})
            send_prob = (
                trigger_config.get("send_probability", 0.50)
                if isinstance(trigger_config, dict)
                else 0.50
            )

            if self.rng.random() >= send_prob:
                continue

            # Check fatigue
            if self._is_survey_fatigued(primary_member_id):
                self._stats["surveys_suppressed_fatigue"] += 1
                continue

            # Create survey
            member_data = self._get_member_data(primary_member_id)
            policy_data_full = self._get_policy_data(policy_id)

            if not member_data or not policy_data_full:
                continue

            pending_survey = self.survey_gen.generate_nps_pending(
                member_data=member_data,
                policy_data=policy_data_full,
                survey_type=SurveyType.ANNUAL,
                trigger_event="PolicyAnniversary",
                claims_history=self._get_claims_history(primary_member_id),
                interaction_history=self._get_interaction_history(primary_member_id),
                billing_status=self._get_billing_status(policy_id),
                digital_engagement=self._get_digital_engagement(primary_member_id),
            )

            self.batch_writer.add("nps_survey_pending", pending_survey.model_dump_db())
            self._stats["nps_pending_created"] += 1
            self._stats["anniversary_surveys"] += 1

            if pending_survey.will_respond:
                self._stats["nps_will_respond"] += 1

            self._track_survey_sent(primary_member_id, "NPS")

    def _get_nps_trigger_key(self, event_type: str) -> Optional[str]:
        """Map event type to NPS trigger config key using reference data lookup.

        Validates that the event_type is a known survey trigger by looking it up
        in the survey_type reference data.

        Args:
            event_type: The event type (e.g., "claim_paid", "ClaimPaid")

        Returns:
            The normalized trigger key (lowercase with underscores) if found, None otherwise.
        """
        survey_type_data = self.reference.get_survey_type_by_trigger_event(event_type)
        if survey_type_data:
            # Return the normalized event type as the trigger key
            return event_type.lower()
        return None

    def _get_nps_survey_type(self, event_type: str) -> SurveyType:
        """Map event type to NPS survey type using reference data lookup.

        Looks up the survey type from reference data and converts the type_code
        to the corresponding SurveyType enum value.

        Args:
            event_type: The event type (e.g., "claim_paid", "ClaimPaid")

        Returns:
            The corresponding SurveyType enum value, or POST_CLAIM as default.
        """
        survey_type_data = self.reference.get_survey_type_by_trigger_event(event_type)
        if survey_type_data:
            type_code = survey_type_data.get("type_code", "")
            # Map JSON type_code to SurveyType enum
            # JSON uses codes like POST_CLAIM, POST_COMPLAINT, POST_INTERACTION
            # Enum uses POST_CLAIM, POST_COMPLAINT_RESOLUTION, POST_INTERACTION
            type_code_to_enum = {
                "POST_CLAIM": SurveyType.POST_CLAIM,
                "POST_CLAIM_REJECTED": SurveyType.POST_CLAIM,
                "POST_INTERACTION": SurveyType.POST_INTERACTION,
                "POST_COMPLAINT": SurveyType.POST_COMPLAINT_RESOLUTION,
                "POST_CASE": SurveyType.POST_INTERACTION,  # Case resolved maps to post-interaction
                "ANNUAL": SurveyType.ANNUAL,
            }
            return type_code_to_enum.get(type_code, SurveyType.POST_CLAIM)
        return SurveyType.POST_CLAIM

    def _is_survey_fatigued(self, member_id: UUID) -> bool:
        """Check if member has received too many surveys recently (max 2 per 30 days)."""
        if member_id not in self.surveys_sent:
            return False

        recent = self.surveys_sent[member_id]
        cutoff = self.sim_env.current_datetime - timedelta(days=30)
        recent_count = sum(1 for s in recent if s.get("sent_date", cutoff) > cutoff)

        return recent_count >= 2

    def _track_survey_sent(self, member_id: UUID, survey_class: str) -> None:
        """Track that a survey was sent to a member."""
        if member_id not in self.surveys_sent:
            self.surveys_sent[member_id] = []

        self.surveys_sent[member_id].append(
            {
                "sent_date": self.sim_env.current_datetime,
                "survey_class": survey_class,
            }
        )

        # Keep only last 6 months
        cutoff = self.sim_env.current_datetime - timedelta(days=180)
        self.surveys_sent[member_id] = [
            s for s in self.surveys_sent[member_id] if s.get("sent_date", cutoff) > cutoff
        ]

    def _get_member_data(self, member_id: UUID) -> Optional[dict]:
        """Get member data from shared state."""
        if not self.shared_state:
            return None

        for pm_id, data in self.shared_state.policy_members.items():
            member = data.get("member")
            if member and member.member_id == member_id:
                return data

        return None

    def _get_policy_data(self, policy_id: UUID) -> Optional[dict]:
        """Get policy data from shared state, including coverage objects."""
        if not self.shared_state:
            return None

        policy_data = self.shared_state.active_policies.get(policy_id)
        if not policy_data:
            return None

        # Find full policy object and include coverages
        result_data = None
        for pm_id, data in self.shared_state.policy_members.items():
            policy = data.get("policy")
            if policy and policy.policy_id == policy_id:
                result_data = data.copy()
                break

        if not result_data:
            result_data = {"policy": None, **policy_data}

        # Add coverage objects from active_policies if available
        coverages = policy_data.get("coverages")
        if coverages:
            result_data["coverages"] = coverages

        return result_data

    def _get_primary_member_id(self, policy_id: UUID) -> Optional[UUID]:
        """
        Get the primary member ID for a policy.

        For Family/Couple policies, NPS surveys should go to the primary member
        (policy holder), not dependents who may have made claims.

        This method queries the database to find the member with member_role = 'Primary'
        for the given policy.

        Args:
            policy_id: The policy UUID

        Returns:
            Primary member UUID, or None if not found
        """
        if not self.shared_state:
            return None

        # Try to get from active_policies first (if stored)
        policy_data = self.shared_state.active_policies.get(policy_id)
        if policy_data and policy_data.get("primary_member_id"):
            return policy_data.get("primary_member_id")

        # Query database to find primary member
        # Use batch_writer's engine to execute a query
        try:
            with self.batch_writer.engine.connect() as conn:
                result = conn.execute(
                    text(
                        """
                        SELECT member_id 
                        FROM policy_member 
                        WHERE policy_id = :policy_id AND member_role = 'Primary'
                        LIMIT 1
                        """
                    ),
                    {"policy_id": str(policy_id)}
                )
                row = result.fetchone()
                if row:
                    return row[0]  # Already a UUID object from database
        except Exception as e:
            logger.warning(
                "primary_member_query_failed",
                policy_id=str(policy_id),
                error=str(e),
            )

        # Fallback: find first member in SharedState (matches creation order where primary is first)
        for pm_id, data in self.shared_state.policy_members.items():
            policy = data.get("policy")
            if policy and policy.policy_id == policy_id:
                member = data.get("member")
                if member:
                    return member.member_id

        return None

    def _get_trigger_entity(self, event: dict) -> Optional[dict]:
        """Get trigger entity details (claim, invoice, etc.)."""
        if event.get("claim_id"):
            return {
                "id": event.get("claim_id"),
                "service_type": event.get("service_type"),
                "total_charge": event.get("charge_amount"),
                "status": "Paid" if event.get("event_type") == "claim_paid" else "Rejected",
                "rejection_reason": event.get("rejection_reason"),
                "processing_days": event.get("processing_days"),
            }
        return None

    def _get_claims_history(self, member_id: UUID) -> list[dict]:
        """Get recent claims history for member."""
        # Would query from shared state or batch writer buffer
        # For now, return empty list
        return []

    def _get_interaction_history(self, member_id: UUID) -> list[dict]:
        """Get recent interaction history for member."""
        if self.shared_state:
            return self.shared_state.get_recent_interactions(member_id, days=180)
        return []

    def _get_billing_status(self, policy_id: UUID) -> dict:
        """Get billing status for policy."""
        if not self.shared_state:
            return {}

        policy_data = self.shared_state.active_policies.get(policy_id, {})
        return {
            "payment_method": policy_data.get("payment_method", "Direct Debit"),
            "in_arrears": policy_data.get("status") == "InArrears",
        }

    def _get_digital_engagement(self, member_id: UUID) -> dict:
        """Get digital engagement for member."""
        if self.shared_state:
            level = self.shared_state.get_engagement_level(member_id)
            return {"engagement_level": level or "medium"}
        return {"engagement_level": "medium"}

    def _log_progress(self) -> None:
        """Log process statistics."""
        logger.info(
            "survey_progress",
            sim_day=int(self.sim_env.now),
            nps_pending=self._stats["nps_pending_created"],
            nps_will_respond=self._stats["nps_will_respond"],
            csat_pending=self._stats["csat_pending_created"],
            csat_will_respond=self._stats["csat_will_respond"],
            suppressed_fatigue=self._stats["surveys_suppressed_fatigue"],
            anniversary=self._stats["anniversary_surveys"],
            journey_nps=self._stats["journey_nps_created"],
            journey_claim_paid=self._stats["journey_claim_paid"],
            journey_claim_rejected=self._stats["journey_claim_rejected"],
            journey_with_escalation=self._stats["journey_with_escalation"],
            journey_no_escalation=self._stats["journey_no_escalation"],
        )

    def get_stats(self) -> dict[str, int]:
        """Get process statistics."""
        return self._stats.copy()
