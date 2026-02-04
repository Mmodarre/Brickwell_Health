"""
Survey Domain Generator for Brickwell Health Simulator.

Generates pending NPS and CSAT surveys for deferred LLM processing.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional, TYPE_CHECKING
from uuid import UUID

from brickwell_health.domain.survey import (
    NPSSurveyPendingCreate,
    CSATSurveyPendingCreate,
)
from brickwell_health.domain.enums import (
    ProcessingStatus,
    SurveyChannel,
    SurveyType,
)
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.statistics.survey_models import (
    SurveyResponsePredictor,
    CRMStatisticalModels,
)
from brickwell_health.statistics.llm_context import LLMContextBuilder

if TYPE_CHECKING:
    from brickwell_health.generators.id_generator import IDGenerator
    from brickwell_health.core.environment import SimulationEnvironment
    from brickwell_health.reference.loader import ReferenceDataLoader


class SurveyGenerator(BaseGenerator[NPSSurveyPendingCreate]):
    """
    Generator for pending survey records.

    Generates NPS and CSAT surveys with:
    - Response prediction (will member respond?)
    - Pre-calculated response timing
    - LLM context for deferred processing

    NO LLM calls are made during simulation.
    """

    def __init__(
        self,
        rng,
        reference: "ReferenceDataLoader",
        id_generator: "IDGenerator",
        sim_env: "SimulationEnvironment",
        config: Optional[dict] = None,
    ):
        """
        Initialize the survey generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator for UUIDs and references
            sim_env: Simulation environment
            config: Optional survey configuration
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

        # Initialize helpers
        self.response_predictor = SurveyResponsePredictor(rng, config)
        self.context_builder = LLMContextBuilder(config.get("llm", {}))
        self.stats_models = CRMStatisticalModels(rng, config)

    def generate(self, **kwargs: Any) -> NPSSurveyPendingCreate:
        """
        Generate a pending NPS survey (default implementation).

        Use generate_nps_pending() or generate_csat_pending() for specific types.
        """
        return self.generate_nps_pending(**kwargs)

    def generate_nps_pending(
        self,
        member_data: dict,
        policy_data: dict,
        survey_type: SurveyType,
        trigger_event: str,
        trigger_entity: Optional[dict] = None,
        claim_id: Optional[UUID] = None,
        interaction_id: Optional[UUID] = None,
        claims_history: Optional[list[dict]] = None,
        interaction_history: Optional[list[dict]] = None,
        billing_status: Optional[dict] = None,
        digital_engagement: Optional[dict] = None,
        coverages: Optional[list[Any]] = None,
        active_policies: Optional[dict] = None,
        policy_id: Optional[UUID] = None,
    ) -> NPSSurveyPendingCreate:
        """
        Generate a pending NPS survey.

        During simulation, we:
        1. Predict if member will respond (statistical model)
        2. Pre-calculate response timing if will respond
        3. Build LLM context from current state
        4. Store in pending table for post-simulation LLM processing

        NO LLM calls are made during simulation.

        Args:
            member_data: Member data dictionary with "member" key
            policy_data: Policy data dictionary with "policy" key
            survey_type: Type of NPS survey
            trigger_event: Event that triggered the survey
            trigger_entity: Optional trigger entity details
            claim_id: Optional related claim ID
            interaction_id: Optional related interaction ID
            claims_history: Recent claims for context
            interaction_history: Recent interactions for context
            billing_status: Current billing status
            digital_engagement: Digital engagement metrics

        Returns:
            NPSSurveyPendingCreate model
        """
        pending_id = self.id_generator.generate_uuid()
        survey_reference = self.id_generator.generate_nps_survey_reference()

        member = member_data.get("member") if member_data else None
        policy = policy_data.get("policy") if policy_data else None

        # Get trigger entity ID
        trigger_entity_id = None
        if trigger_entity:
            trigger_entity_id = trigger_entity.get("id") or trigger_entity.get("claim_id")

        # Get member context for response prediction
        prediction_context = {
            "survey_type": survey_type.value if hasattr(survey_type, "value") else str(survey_type),
            "tenure_months": self._calculate_tenure(
                policy.start_date if policy and hasattr(policy, "start_date") else None
            ),
            "member_age": self._calculate_age(
                member.date_of_birth if member and hasattr(member, "date_of_birth") else None
            ),
            "recent_claim_rejected": trigger_event == "ClaimRejected",
            "recent_complaint": billing_status.get("recent_complaint", False) if billing_status else False,
            "engagement_level": digital_engagement.get("engagement_level", "medium")
            if digital_engagement
            else "medium",
            "surveys_received_6mo": 0,  # Could track this in SharedState
        }

        # Predict response
        will_respond, response_probability = self.response_predictor.predict_nps_response(
            prediction_context
        )

        # Pre-calculate response timing if will respond (per design decision #4)
        sent_datetime = self.get_current_datetime()
        completed_datetime = None
        response_time_minutes = None

        if will_respond:
            response_time_minutes = self.stats_models.sample_response_time_minutes("nps")
            completed_datetime = sent_datetime + timedelta(minutes=response_time_minutes)

        # Build LLM context
        # Extract coverages from policy_data if not explicitly provided
        if coverages is None:
            coverages = policy_data.get("coverages")
        
        # Extract policy_id from policy object if not provided
        if policy_id is None:
            policy = policy_data.get("policy") if policy_data else None
            if policy and hasattr(policy, "policy_id"):
                policy_id = policy.policy_id

        llm_context = self.context_builder.build_nps_context(
            member_data=member_data,
            policy_data=policy_data,
            trigger_event=trigger_event,
            trigger_entity=trigger_entity,
            claims_history=claims_history or [],
            interaction_history=interaction_history or [],
            billing_status=billing_status or {},
            digital_engagement=digital_engagement or {},
            simulation_date=self.get_current_date(),
            coverages=coverages,
            active_policies=active_policies,
            policy_id=policy_id,
            reference=self.reference,
        )

        return NPSSurveyPendingCreate(
            pending_id=pending_id,
            survey_reference=survey_reference,
            member_id=member.member_id if member else None,
            policy_id=policy.policy_id if policy else None,
            survey_type=survey_type,
            trigger_event=trigger_event,
            trigger_entity_id=trigger_entity_id,
            claim_id=claim_id,
            interaction_id=interaction_id,
            simulation_date=self.get_current_date(),
            sent_datetime=sent_datetime,
            will_respond=will_respond,
            response_probability=Decimal(str(round(response_probability, 4))),
            completed_datetime=completed_datetime,
            response_time_minutes=response_time_minutes,
            llm_context=llm_context,
            processing_status=ProcessingStatus.PENDING,
        )

    def generate_csat_pending(
        self,
        member_data: dict,
        policy_data: dict,
        survey_type: SurveyType,
        interaction_data: dict,
        case_data: Optional[dict] = None,
    ) -> CSATSurveyPendingCreate:
        """
        Generate a pending CSAT survey.

        Args:
            member_data: Member data dictionary
            policy_data: Policy data dictionary
            survey_type: Type of CSAT survey
            interaction_data: Interaction that triggered the survey
            case_data: Optional case data for case resolution surveys

        Returns:
            CSATSurveyPendingCreate model
        """
        pending_id = self.id_generator.generate_uuid()
        survey_reference = self.id_generator.generate_csat_survey_reference()

        member = member_data.get("member") if member_data else None
        policy = policy_data.get("policy") if policy_data else None

        # Predict response
        prediction_context = {
            "first_contact_resolution": interaction_data.get("fcr", False)
            or interaction_data.get("first_contact_resolution", False),
            "hours_since_interaction": 1,  # Sent shortly after
            "sla_breached": case_data.get("sla_breached") if case_data else False,
        }

        will_respond, response_probability = self.response_predictor.predict_csat_response(
            prediction_context
        )

        # Pre-calculate response timing if will respond (per design decision #4)
        sent_datetime = self.get_current_datetime()
        completed_datetime = None
        response_time_minutes = None

        if will_respond:
            response_time_minutes = self.stats_models.sample_response_time_minutes("csat")
            completed_datetime = sent_datetime + timedelta(minutes=response_time_minutes)

        # Build LLM context
        llm_context = self.context_builder.build_csat_context(
            member_data=member_data,
            policy_data=policy_data,
            interaction_data=interaction_data,
            case_data=case_data,
            simulation_date=self.get_current_date(),
        )

        return CSATSurveyPendingCreate(
            pending_id=pending_id,
            survey_reference=survey_reference,
            member_id=member.member_id if member else None,
            policy_id=policy.policy_id if policy else None,
            survey_type=survey_type,
            interaction_id=interaction_data.get("interaction_id"),
            case_id=case_data.get("case_id") if case_data else None,
            simulation_date=self.get_current_date(),
            sent_datetime=sent_datetime,
            will_respond=will_respond,
            response_probability=Decimal(str(round(response_probability, 4))),
            completed_datetime=completed_datetime,
            response_time_minutes=response_time_minutes,
            llm_context=llm_context,
            processing_status=ProcessingStatus.PENDING,
        )

    def _calculate_age(self, dob: Optional[date]) -> int:
        """Calculate age from date of birth."""
        if not dob:
            return 40  # Default
        current = self.get_current_date()
        age = current.year - dob.year
        if (current.month, current.day) < (dob.month, dob.day):
            age -= 1
        return max(0, age)

    def _calculate_tenure(self, start_date: Optional[date]) -> int:
        """Calculate tenure in months."""
        if not start_date:
            return 12  # Default
        current = self.get_current_date()
        months = (current.year - start_date.year) * 12 + (current.month - start_date.month)
        return max(0, months)
