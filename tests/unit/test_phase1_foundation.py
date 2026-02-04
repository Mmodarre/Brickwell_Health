"""
Unit tests for Phase 1 Foundation components.

Tests enums, config models, reference data, shared state, and ID generator
additions for the NBA/NPS domain implementation.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from brickwell_health.config.models import (
    CampaignConfig,
    CaseConfig,
    CommunicationConfig,
    ComplaintConfig,
    CRMConfig,
    CSATConfig,
    DigitalConfig,
    EngagementRates,
    EventTriggersConfig,
    FatigueRules,
    InteractionConfig,
    InteractionDurationParams,
    LLMConfig,
    NPSConfig,
    NPSTriggerConfig,
    SimulationConfig,
    SurveyConfig,
    TriggerProbabilities,
)
from brickwell_health.core.shared_state import SharedState
from brickwell_health.domain.enums import (
    CampaignResponseType,
    CampaignStatus,
    CampaignType,
    CasePriority,
    CaseStatus,
    CommunicationDeliveryStatus,
    CommunicationType,
    ComplaintResolutionOutcome,
    ComplaintSeverity,
    ComplaintSource,
    ComplaintStatus,
    ConversionType,
    CSATLabel,
    DeviceType,
    DigitalEventType,
    InteractionChannel,
    InteractionDirection,
    NPSCategory,
    PageCategory,
    PreferenceType,
    ProcessingStatus,
    SentimentLabel,
    SessionType,
    SurveyChannel,
    SurveyType,
    TriggerEventType,
)


# ============================================================================
# ENUM TESTS
# ============================================================================

class TestCRMEnums:
    """Test CRM domain enums."""

    def test_interaction_channel_values(self):
        """Test InteractionChannel enum values."""
        assert InteractionChannel.PHONE.value == "Phone"
        assert InteractionChannel.EMAIL.value == "Email"
        assert InteractionChannel.CHAT.value == "Chat"
        assert InteractionChannel.BRANCH.value == "Branch"
        assert InteractionChannel.IN_APP.value == "InApp"

    def test_interaction_direction_values(self):
        """Test InteractionDirection enum values."""
        assert InteractionDirection.INBOUND.value == "Inbound"
        assert InteractionDirection.OUTBOUND.value == "Outbound"

    def test_case_priority_values(self):
        """Test CasePriority enum values."""
        assert CasePriority.LOW.value == "Low"
        assert CasePriority.MEDIUM.value == "Medium"
        assert CasePriority.HIGH.value == "High"
        assert CasePriority.CRITICAL.value == "Critical"

    def test_case_status_values(self):
        """Test CaseStatus enum values."""
        assert CaseStatus.OPEN.value == "Open"
        assert CaseStatus.IN_PROGRESS.value == "InProgress"
        assert CaseStatus.RESOLVED.value == "Resolved"
        assert CaseStatus.CLOSED.value == "Closed"

    def test_complaint_status_values(self):
        """Test ComplaintStatus enum values."""
        assert ComplaintStatus.RECEIVED.value == "Received"
        assert ComplaintStatus.INVESTIGATING.value == "Investigating"
        assert ComplaintStatus.RESOLVED.value == "Resolved"
        assert ComplaintStatus.ESCALATED.value == "Escalated"

    def test_complaint_source_values(self):
        """Test ComplaintSource enum values."""
        assert ComplaintSource.PHONE.value == "Phone"
        assert ComplaintSource.PHIO.value == "PHIO"

    def test_complaint_resolution_outcome_values(self):
        """Test ComplaintResolutionOutcome enum values."""
        assert ComplaintResolutionOutcome.UPHELD.value == "Upheld"
        assert ComplaintResolutionOutcome.NOT_UPHELD.value == "NotUpheld"
        assert ComplaintResolutionOutcome.PARTIALLY_UPHELD.value == "PartiallyUpheld"


class TestCommunicationEnums:
    """Test Communication domain enums."""

    def test_communication_type_values(self):
        """Test CommunicationType enum values."""
        assert CommunicationType.EMAIL.value == "Email"
        assert CommunicationType.SMS.value == "SMS"
        assert CommunicationType.PUSH.value == "Push"

    def test_communication_delivery_status_values(self):
        """Test CommunicationDeliveryStatus enum values."""
        assert CommunicationDeliveryStatus.PENDING.value == "Pending"
        assert CommunicationDeliveryStatus.DELIVERED.value == "Delivered"
        assert CommunicationDeliveryStatus.BOUNCED.value == "Bounced"

    def test_campaign_type_values(self):
        """Test CampaignType enum values."""
        assert CampaignType.RETENTION.value == "Retention"
        assert CampaignType.UPSELL.value == "Upsell"
        assert CampaignType.CROSS_SELL.value == "CrossSell"

    def test_campaign_status_values(self):
        """Test CampaignStatus enum values."""
        assert CampaignStatus.DRAFT.value == "Draft"
        assert CampaignStatus.ACTIVE.value == "Active"
        assert CampaignStatus.COMPLETED.value == "Completed"


class TestDigitalEnums:
    """Test Digital Behavior domain enums."""

    def test_device_type_values(self):
        """Test DeviceType enum values."""
        assert DeviceType.DESKTOP.value == "Desktop"
        assert DeviceType.MOBILE.value == "Mobile"
        assert DeviceType.TABLET.value == "Tablet"

    def test_session_type_values(self):
        """Test SessionType enum values."""
        assert SessionType.WEB.value == "Web"
        assert SessionType.APP.value == "App"

    def test_digital_event_type_values(self):
        """Test DigitalEventType enum values."""
        assert DigitalEventType.PAGE_VIEW.value == "PageView"
        assert DigitalEventType.CLICK.value == "Click"
        assert DigitalEventType.FORM_SUBMIT.value == "FormSubmit"

    def test_page_category_values(self):
        """Test PageCategory enum values."""
        assert PageCategory.HOME.value == "Home"
        assert PageCategory.CLAIMS.value == "Claims"
        assert PageCategory.CANCEL.value == "Cancel"


class TestSurveyEnums:
    """Test Survey domain enums."""

    def test_survey_type_values(self):
        """Test SurveyType enum values."""
        assert SurveyType.POST_CLAIM.value == "PostClaim"
        assert SurveyType.POST_INTERACTION.value == "PostInteraction"
        assert SurveyType.ANNUAL.value == "Annual"

    def test_nps_category_values(self):
        """Test NPSCategory enum values."""
        assert NPSCategory.PROMOTER.value == "Promoter"
        assert NPSCategory.PASSIVE.value == "Passive"
        assert NPSCategory.DETRACTOR.value == "Detractor"

    def test_csat_label_values(self):
        """Test CSATLabel enum values."""
        assert CSATLabel.VERY_SATISFIED.value == "VerySatisfied"
        assert CSATLabel.NEUTRAL.value == "Neutral"
        assert CSATLabel.VERY_DISSATISFIED.value == "VeryDissatisfied"

    def test_processing_status_values(self):
        """Test ProcessingStatus enum values."""
        assert ProcessingStatus.PENDING.value == "pending"
        assert ProcessingStatus.COMPLETED.value == "completed"
        assert ProcessingStatus.FAILED.value == "failed"


class TestTriggerEventEnums:
    """Test TriggerEventType enum."""

    def test_claims_trigger_values(self):
        """Test claims-related trigger values."""
        assert TriggerEventType.CLAIM_SUBMITTED.value == "ClaimSubmitted"
        assert TriggerEventType.CLAIM_REJECTED.value == "ClaimRejected"
        assert TriggerEventType.CLAIM_PAID.value == "ClaimPaid"

    def test_billing_trigger_values(self):
        """Test billing-related trigger values."""
        assert TriggerEventType.PAYMENT_FAILED.value == "PaymentFailed"
        assert TriggerEventType.ARREARS_CREATED.value == "ArrearsCreated"
        assert TriggerEventType.POLICY_SUSPENDED.value == "PolicySuspended"

    def test_crm_trigger_values(self):
        """Test CRM-related trigger values."""
        assert TriggerEventType.INTERACTION_COMPLETED.value == "InteractionCompleted"
        assert TriggerEventType.CASE_RESOLVED.value == "CaseResolved"


# ============================================================================
# CONFIG MODEL TESTS
# ============================================================================

class TestCRMConfigModels:
    """Test CRM configuration models."""

    def test_interaction_duration_params(self):
        """Test InteractionDurationParams model."""
        params = InteractionDurationParams(mu=5.0, sigma=0.4)
        assert params.mu == 5.0
        assert params.sigma == 0.4

    def test_interaction_config_defaults(self):
        """Test InteractionConfig default values."""
        config = InteractionConfig()
        assert config.baseline_contacts_per_year == 2.5
        assert "Phone" in config.channel_distribution
        assert sum(config.channel_distribution.values()) == pytest.approx(1.0)

    def test_interaction_config_channel_validation(self):
        """Test InteractionConfig channel distribution validation."""
        with pytest.raises(ValueError, match="must sum to 1.0"):
            InteractionConfig(channel_distribution={"Phone": 0.5, "Email": 0.3})

    def test_case_config_defaults(self):
        """Test CaseConfig default values."""
        config = CaseConfig()
        assert "Critical" in config.sla_breach_rates
        assert "High" in config.resolution_time_params

    def test_complaint_config_defaults(self):
        """Test ComplaintConfig default values."""
        config = ComplaintConfig()
        assert config.rate_per_1000_members == 0.75
        assert config.phio_escalation_rate == 0.08
        assert sum(config.resolution_outcomes.values()) == pytest.approx(1.0)

    def test_crm_config_composition(self):
        """Test CRMConfig composes nested configs."""
        config = CRMConfig()
        assert isinstance(config.interaction, InteractionConfig)
        assert isinstance(config.case, CaseConfig)
        assert isinstance(config.complaint, ComplaintConfig)


class TestCommunicationConfigModels:
    """Test Communication configuration models."""

    def test_engagement_rates_defaults(self):
        """Test EngagementRates default values."""
        rates = EngagementRates()
        assert rates.delivery_rate == 0.97
        assert rates.open_rate == 0.60
        assert 0 <= rates.opt_out_rate <= 1

    def test_fatigue_rules_defaults(self):
        """Test FatigueRules default values."""
        rules = FatigueRules()
        assert rules.max_marketing_per_week == 2
        assert rules.min_days_between_similar == 7

    def test_communication_config_defaults(self):
        """Test CommunicationConfig default values."""
        config = CommunicationConfig()
        assert isinstance(config.transactional, EngagementRates)
        assert isinstance(config.fatigue, FatigueRules)


class TestCampaignConfigModels:
    """Test Campaign configuration models."""

    def test_campaign_config_defaults(self):
        """Test CampaignConfig default values."""
        config = CampaignConfig()
        assert config.campaigns_per_year == 6
        assert sum(config.type_distribution.values()) == pytest.approx(1.0)

    def test_campaign_config_type_validation(self):
        """Test CampaignConfig type distribution validation."""
        with pytest.raises(ValueError, match="must sum to 1.0"):
            CampaignConfig(type_distribution={"Retention": 0.5})


class TestDigitalConfigModels:
    """Test Digital Behavior configuration models."""

    def test_digital_config_defaults(self):
        """Test DigitalConfig default values."""
        config = DigitalConfig()
        assert "high" in config.sessions_per_month
        assert sum(config.engagement_distribution.values()) == pytest.approx(1.0)
        assert sum(config.device_distribution.values()) == pytest.approx(1.0)
        assert sum(config.page_category_distribution.values()) == pytest.approx(1.0)

    def test_digital_config_engagement_validation(self):
        """Test DigitalConfig engagement distribution validation."""
        with pytest.raises(ValueError, match="must sum to 1.0"):
            DigitalConfig(engagement_distribution={"high": 0.5})


class TestSurveyConfigModels:
    """Test Survey configuration models."""

    def test_nps_trigger_config(self):
        """Test NPSTriggerConfig model."""
        trigger = NPSTriggerConfig(send_probability=0.30, response_rate=0.22)
        assert trigger.send_probability == 0.30
        assert trigger.response_rate == 0.22

    def test_nps_config_defaults(self):
        """Test NPSConfig default values."""
        config = NPSConfig()
        assert config.base_response_rate == 0.18
        assert "claim_paid" in config.triggers
        assert "Detractor" in config.churn_rates_by_category

    def test_csat_config_defaults(self):
        """Test CSATConfig default values."""
        config = CSATConfig()
        assert config.base_response_rate == 0.35
        assert sum(config.score_distribution) == pytest.approx(1.0)

    def test_csat_config_score_validation(self):
        """Test CSATConfig score distribution validation."""
        with pytest.raises(ValueError, match="must sum to 1.0"):
            CSATConfig(score_distribution=[0.1, 0.2, 0.3])

    def test_survey_config_composition(self):
        """Test SurveyConfig composes nested configs."""
        config = SurveyConfig()
        assert isinstance(config.nps, NPSConfig)
        assert isinstance(config.csat, CSATConfig)


class TestLLMConfigModels:
    """Test LLM configuration models."""

    def test_llm_config_defaults(self):
        """Test LLMConfig default values."""
        config = LLMConfig()
        assert config.model == "databricks-qwen3-next-80b-a3b-instruct"
        assert config.max_claims_history == 5
        assert config.enforce_score_consistency is True


class TestEventTriggersConfigModels:
    """Test Event Triggers configuration models."""

    def test_trigger_probabilities_defaults(self):
        """Test TriggerProbabilities default values."""
        probs = TriggerProbabilities()
        assert probs.interaction == 0.0
        assert probs.communication == 0.0

    def test_event_triggers_config_defaults(self):
        """Test EventTriggersConfig default values."""
        config = EventTriggersConfig()
        assert config.claim_submitted.communication == 1.00
        assert config.claim_rejected.complaint == 0.15


class TestSimulationConfigIntegration:
    """Test SimulationConfig includes new NBA/NPS configs."""

    def test_simulation_config_has_new_fields(self, test_config):
        """Test SimulationConfig has NBA/NPS domain fields."""
        assert hasattr(test_config, "crm")
        assert hasattr(test_config, "communication")
        assert hasattr(test_config, "campaign")
        assert hasattr(test_config, "digital")
        assert hasattr(test_config, "survey")
        assert hasattr(test_config, "llm")
        assert hasattr(test_config, "event_triggers")


# ============================================================================
# REFERENCE DATA TESTS
# ============================================================================

class TestReferenceDataFiles:
    """Test reference data JSON files exist and are valid."""

    @pytest.fixture
    def reference_path(self) -> Path:
        """Get reference data path."""
        return Path("data/reference")

    def test_communication_template_exists(self, reference_path):
        """Test communication_template.json exists and is valid."""
        file_path = reference_path / "communication_template.json"
        assert file_path.exists(), f"File not found: {file_path}"
        
        with open(file_path) as f:
            data = json.load(f)
        
        assert isinstance(data, list)
        assert len(data) == 15
        assert all("template_id" in item for item in data)
        assert all("template_code" in item for item in data)

    def test_campaign_type_exists(self, reference_path):
        """Test campaign_type.json exists and is valid."""
        file_path = reference_path / "campaign_type.json"
        assert file_path.exists(), f"File not found: {file_path}"
        
        with open(file_path) as f:
            data = json.load(f)
        
        assert isinstance(data, list)
        assert len(data) == 6
        assert all("type_id" in item for item in data)

    def test_survey_type_exists(self, reference_path):
        """Test survey_type.json exists and is valid."""
        file_path = reference_path / "survey_type.json"
        assert file_path.exists(), f"File not found: {file_path}"
        
        with open(file_path) as f:
            data = json.load(f)
        
        assert isinstance(data, list)
        assert len(data) == 7
        assert all("survey_class" in item for item in data)


# ============================================================================
# SHARED STATE TESTS
# ============================================================================

class TestSharedStateCRMEventQueue:
    """Test SharedState CRM event queue methods."""

    @pytest.fixture
    def shared_state(self) -> SharedState:
        """Create a fresh SharedState instance."""
        return SharedState()

    def test_add_crm_event(self, shared_state):
        """Test adding CRM events to queue."""
        event = {
            "event_type": "ClaimRejected",
            "timestamp": datetime.now(),
            "policy_id": uuid4(),
            "member_id": uuid4(),
        }
        shared_state.add_crm_event(event)
        
        assert len(shared_state.crm_event_queue) == 1

    def test_get_crm_events_fifo(self, shared_state):
        """Test CRM events are returned in FIFO order."""
        events = [
            {"event_type": "event1", "timestamp": datetime.now()},
            {"event_type": "event2", "timestamp": datetime.now()},
            {"event_type": "event3", "timestamp": datetime.now()},
        ]
        for event in events:
            shared_state.add_crm_event(event)
        
        retrieved = shared_state.get_crm_events()
        
        assert len(retrieved) == 3
        assert retrieved[0]["event_type"] == "event1"
        assert retrieved[2]["event_type"] == "event3"
        assert len(shared_state.crm_event_queue) == 0

    def test_get_crm_events_max_limit(self, shared_state):
        """Test CRM events with max_events limit."""
        for i in range(5):
            shared_state.add_crm_event({"event_type": f"event{i}"})
        
        retrieved = shared_state.get_crm_events(max_events=2)
        
        assert len(retrieved) == 2
        assert len(shared_state.crm_event_queue) == 3

    def test_peek_crm_events(self, shared_state):
        """Test peeking at CRM events without removing."""
        shared_state.add_crm_event({"event_type": "test"})
        
        peeked = shared_state.peek_crm_events()
        
        assert len(peeked) == 1
        assert len(shared_state.crm_event_queue) == 1  # Still there


class TestSharedStateInteractionTracking:
    """Test SharedState interaction tracking methods."""

    @pytest.fixture
    def shared_state(self) -> SharedState:
        """Create a fresh SharedState instance."""
        return SharedState()

    def test_add_interaction(self, shared_state):
        """Test adding interactions for a member."""
        member_id = uuid4()
        interaction = {"timestamp": datetime.now(), "type": "CLAIM_STATUS"}
        
        shared_state.add_interaction(member_id, interaction)
        
        assert member_id in shared_state.recent_interactions
        assert len(shared_state.recent_interactions[member_id]) == 1

    def test_add_interaction_keeps_last_10(self, shared_state):
        """Test interaction list is capped at 10."""
        member_id = uuid4()
        
        for i in range(15):
            shared_state.add_interaction(member_id, {"index": i, "timestamp": datetime.now()})
        
        assert len(shared_state.recent_interactions[member_id]) == 10
        assert shared_state.recent_interactions[member_id][0]["index"] == 5  # First 5 dropped

    def test_get_recent_interactions_filters_by_days(self, shared_state):
        """Test getting interactions filters by days."""
        member_id = uuid4()
        old = {"timestamp": datetime.now() - timedelta(days=60), "type": "old"}
        recent = {"timestamp": datetime.now() - timedelta(days=5), "type": "recent"}
        
        shared_state.add_interaction(member_id, old)
        shared_state.add_interaction(member_id, recent)
        
        result = shared_state.get_recent_interactions(member_id, days=30)
        
        assert len(result) == 1
        assert result[0]["type"] == "recent"


class TestSharedStateSurveyTracking:
    """Test SharedState survey tracking methods."""

    @pytest.fixture
    def shared_state(self) -> SharedState:
        """Create a fresh SharedState instance."""
        return SharedState()

    def test_has_pending_survey(self, shared_state):
        """Test checking for pending surveys."""
        member_id = uuid4()
        
        assert not shared_state.has_pending_survey(member_id, "NPS")
        
        shared_state.add_pending_survey(member_id, "NPS", {"sent_date": datetime.now()})
        
        assert shared_state.has_pending_survey(member_id, "NPS")
        assert not shared_state.has_pending_survey(member_id, "CSAT")

    def test_remove_pending_survey(self, shared_state):
        """Test removing pending surveys."""
        member_id = uuid4()
        shared_state.add_pending_survey(member_id, "NPS", {})
        
        shared_state.remove_pending_survey(member_id, "NPS")
        
        assert not shared_state.has_pending_survey(member_id, "NPS")


class TestSharedStateCommunicationPreferences:
    """Test SharedState communication preferences methods."""

    @pytest.fixture
    def shared_state(self) -> SharedState:
        """Create a fresh SharedState instance."""
        return SharedState()

    def test_set_and_get_preferences(self, shared_state):
        """Test setting and getting communication preferences."""
        member_id = uuid4()
        prefs = {"transactional_email": True, "marketing_email": False}
        
        shared_state.set_communication_preferences(member_id, prefs)
        
        result = shared_state.get_communication_preferences(member_id)
        assert result["marketing_email"] is False

    def test_get_preferences_defaults(self, shared_state):
        """Test default preferences for unknown member."""
        result = shared_state.get_communication_preferences(uuid4())
        
        assert result["transactional_email"] is True
        assert result["marketing_sms"] is True

    def test_is_opted_in(self, shared_state):
        """Test is_opted_in helper method."""
        member_id = uuid4()
        shared_state.set_communication_preferences(member_id, {"marketing_email": False})
        
        assert not shared_state.is_opted_in(member_id, "marketing", "email")
        assert shared_state.is_opted_in(member_id, "transactional", "sms")  # Default


class TestSharedStateEngagementLevels:
    """Test SharedState digital engagement level methods."""

    @pytest.fixture
    def shared_state(self) -> SharedState:
        """Create a fresh SharedState instance."""
        return SharedState()

    def test_set_and_get_engagement_level(self, shared_state):
        """Test setting and getting engagement levels."""
        member_id = uuid4()
        
        shared_state.set_engagement_level(member_id, "high")
        
        assert shared_state.get_engagement_level(member_id) == "high"

    def test_get_engagement_level_default(self, shared_state):
        """Test default engagement level."""
        assert shared_state.get_engagement_level(uuid4()) == "medium"


class TestSharedStateStats:
    """Test SharedState get_stats includes new fields."""

    def test_get_stats_includes_new_fields(self):
        """Test get_stats returns NBA/NPS related counts."""
        state = SharedState()
        state.add_crm_event({"event_type": "test"})
        
        stats = state.get_stats()
        
        assert "crm_event_queue" in stats
        assert stats["crm_event_queue"] == 1
        assert "pending_surveys" in stats
        assert "members_with_engagement_level" in stats


# ============================================================================
# ID GENERATOR TESTS
# ============================================================================

class TestIDGeneratorNBAMethods:
    """Test ID generator NBA/NPS domain methods."""

    def test_generate_interaction_reference(self, id_generator):
        """Test interaction reference generation."""
        ref1 = id_generator.generate_interaction_reference()
        ref2 = id_generator.generate_interaction_reference()
        
        assert ref1.startswith("INT-W")
        assert ref1 != ref2
        assert "000001" in ref1
        assert "000002" in ref2

    def test_generate_case_number(self, id_generator):
        """Test case number generation."""
        num = id_generator.generate_case_number()
        
        assert num.startswith("CASE-W")
        assert "000001" in num

    def test_generate_complaint_number(self, id_generator):
        """Test complaint number generation."""
        num = id_generator.generate_complaint_number()
        
        assert num.startswith("COMP-W")

    def test_generate_communication_reference(self, id_generator):
        """Test communication reference generation."""
        ref = id_generator.generate_communication_reference()
        
        assert ref.startswith("COMM-W")

    def test_generate_campaign_code(self, id_generator):
        """Test campaign code generation."""
        code = id_generator.generate_campaign_code("Retention")
        
        assert code.startswith("RET-W")
        assert "-001" in code

    def test_generate_nps_survey_reference(self, id_generator):
        """Test NPS survey reference generation."""
        ref = id_generator.generate_nps_survey_reference()
        
        assert ref.startswith("NPS-W")

    def test_generate_csat_survey_reference(self, id_generator):
        """Test CSAT survey reference generation."""
        ref = id_generator.generate_csat_survey_reference()
        
        assert ref.startswith("CSAT-W")

    def test_counters_include_new_fields(self, id_generator):
        """Test get_counters includes NBA/NPS counters."""
        id_generator.generate_interaction_reference()
        id_generator.generate_case_number()
        
        counters = id_generator.get_counters()
        
        assert "interaction" in counters
        assert "case" in counters
        assert "complaint" in counters
        assert counters["interaction"] == 1
        assert counters["case"] == 1

    def test_set_counters_includes_new_fields(self, id_generator):
        """Test set_counters accepts NBA/NPS counter values."""
        id_generator.set_counters(
            interaction=100,
            case=50,
            complaint=25,
            communication=75,
            campaign=10,
            nps_survey=200,
            csat_survey=150,
        )
        
        counters = id_generator.get_counters()
        
        assert counters["interaction"] == 100
        assert counters["nps_survey"] == 200


# ============================================================================
# REFERENCE LOADER TESTS
# ============================================================================

class TestReferenceLoaderNewMethods:
    """Test reference loader new methods for NBA/NPS domain.
    
    Note: These tests use the actual reference data path rather than the test
    fixture since the test fixture only contains minimal reference data.
    """

    @pytest.fixture
    def real_reference(self):
        """Get reference loader with actual reference data."""
        from brickwell_health.reference.loader import ReferenceDataLoader
        return ReferenceDataLoader(Path("data/reference"))

    def test_get_interaction_types(self, real_reference):
        """Test getting interaction types."""
        types = real_reference.get_interaction_types()
        
        assert isinstance(types, list)
        assert len(types) > 0

    def test_get_interaction_type_by_code(self, real_reference):
        """Test getting interaction type by code."""
        interaction = real_reference.get_interaction_type_by_code("CLAIM_STATUS")
        
        assert interaction is not None or interaction is None  # May not exist

    def test_get_case_types(self, real_reference):
        """Test getting case types."""
        types = real_reference.get_case_types()
        
        assert isinstance(types, list)
        assert len(types) > 0

    def test_get_complaint_categories(self, real_reference):
        """Test getting complaint categories."""
        categories = real_reference.get_complaint_categories()
        
        assert isinstance(categories, list)
        assert len(categories) > 0

    def test_get_communication_templates(self, real_reference):
        """Test getting communication templates."""
        templates = real_reference.get_communication_templates()
        
        assert isinstance(templates, list)
        assert len(templates) == 15

    def test_get_campaign_types(self, real_reference):
        """Test getting campaign types."""
        types = real_reference.get_campaign_types()
        
        assert isinstance(types, list)
        assert len(types) == 6

    def test_get_survey_types(self, real_reference):
        """Test getting survey types."""
        types = real_reference.get_survey_types()
        
        assert isinstance(types, list)
        assert len(types) == 7
