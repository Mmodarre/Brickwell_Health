"""
Unit tests for Survey Domain Generator and Models.

Tests for:
- Domain models and model_dump_db()
- SurveyResponsePredictor
- CRMStatisticalModels
- LLMContextBuilder
- SurveyGenerator
"""

from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.domain.survey import (
    NPSSurveyPendingCreate,
    NPSSurveyCreate,
    CSATSurveyPendingCreate,
    CSATSurveyCreate,
    NPSSurveyLLMResponse,
    CSATSurveyLLMResponse,
)
from brickwell_health.domain.enums import (
    SurveyType,
    NPSCategory,
    CSATLabel,
    SentimentLabel,
    SurveyChannel,
    ProcessingStatus,
)
from brickwell_health.statistics.survey_models import (
    SurveyResponsePredictor,
    CRMStatisticalModels,
)
from brickwell_health.statistics.llm_context import LLMContextBuilder


# ============================================================================
# TEST FIXTURES
# ============================================================================


@pytest.fixture
def rng():
    """Create deterministic RNG for reproducible tests."""
    return np.random.default_rng(42)


@pytest.fixture
def response_predictor(rng):
    """Create response predictor with config."""
    config = {
        "nps": {
            "base_response_rate": 0.18,
            "triggers": {
                "claim_paid": {"send_probability": 0.30, "response_rate": 0.20},
                "claim_rejected": {"send_probability": 0.80, "response_rate": 0.45},
            },
        },
        "csat": {
            "base_response_rate": 0.35,
        },
    }
    return SurveyResponsePredictor(rng, config)


@pytest.fixture
def stats_models(rng):
    """Create CRM statistical models."""
    return CRMStatisticalModels(rng)


@pytest.fixture
def context_builder():
    """Create LLM context builder."""
    return LLMContextBuilder({"max_claims_history": 5, "max_interaction_history": 3})


# ============================================================================
# TEST NPSSurveyPendingCreate
# ============================================================================


class TestNPSSurveyPendingCreate:
    """Tests for NPSSurveyPendingCreate model."""

    def test_create_nps_pending_basic(self):
        """Test basic NPS pending creation."""
        pending = NPSSurveyPendingCreate(
            pending_id=uuid4(),
            survey_reference="NPS-2025-001234",
            member_id=uuid4(),
            policy_id=uuid4(),
            survey_type=SurveyType.POST_CLAIM,
            trigger_event="ClaimPaid",
            simulation_date=date(2025, 6, 15),
            sent_datetime=datetime(2025, 6, 15, 10, 30),
            will_respond=True,
            response_probability=Decimal("0.2345"),
            llm_context={"member_name": "John Smith"},
            processing_status=ProcessingStatus.PENDING,
        )

        assert pending.survey_type == SurveyType.POST_CLAIM
        assert pending.will_respond is True
        assert pending.processing_status == ProcessingStatus.PENDING

    def test_model_dump_db_enum_conversion(self):
        """Test that model_dump_db converts enums to values."""
        pending = NPSSurveyPendingCreate(
            pending_id=uuid4(),
            survey_reference="NPS-2025-001234",
            member_id=uuid4(),
            policy_id=uuid4(),
            survey_type=SurveyType.POST_CLAIM,
            simulation_date=date(2025, 6, 15),
            sent_datetime=datetime(2025, 6, 15, 10, 30),
            will_respond=False,
            llm_context={},
            processing_status=ProcessingStatus.PENDING,
        )

        db_dict = pending.model_dump_db()

        # Enums should be converted to their string values
        assert db_dict["survey_type"] == "PostClaim"
        assert db_dict["processing_status"] == "pending"


# ============================================================================
# TEST NPSSurveyCreate
# ============================================================================


class TestNPSSurveyCreate:
    """Tests for NPSSurveyCreate model."""

    def test_create_nps_survey_with_scores(self):
        """Test NPS survey creation with all scores."""
        survey = NPSSurveyCreate(
            survey_id=uuid4(),
            survey_reference="NPS-2025-001234",
            member_id=uuid4(),
            policy_id=uuid4(),
            survey_type=SurveyType.POST_CLAIM,
            sent_date=datetime(2025, 6, 15, 10, 30),
            completed_date=datetime(2025, 6, 15, 14, 45),
            nps_score=9,
            nps_category=NPSCategory.PROMOTER,
            driver_claims_processing=8,
            driver_customer_service=9,
            driver_value_for_money=7,
            driver_coverage_clarity=8,
            driver_digital_experience=9,
            feedback_text="Great service, very satisfied!",
            sentiment_score=Decimal("0.85"),
            sentiment_label=SentimentLabel.POSITIVE,
            survey_channel=SurveyChannel.EMAIL,
        )

        assert survey.nps_score == 9
        assert survey.nps_category == NPSCategory.PROMOTER
        assert survey.sentiment_label == SentimentLabel.POSITIVE

    def test_nps_score_validation(self):
        """Test that NPS score must be 0-10."""
        with pytest.raises(ValueError):
            NPSSurveyCreate(
                survey_id=uuid4(),
                survey_reference="NPS-2025-001234",
                member_id=uuid4(),
                policy_id=uuid4(),
                survey_type=SurveyType.POST_CLAIM,
                sent_date=datetime(2025, 6, 15, 10, 30),
                nps_score=11,  # Invalid - must be 0-10
            )

    def test_model_dump_db_all_enums(self):
        """Test model_dump_db converts all enum fields."""
        survey = NPSSurveyCreate(
            survey_id=uuid4(),
            survey_reference="NPS-2025-001234",
            member_id=uuid4(),
            policy_id=uuid4(),
            survey_type=SurveyType.ANNUAL,
            sent_date=datetime(2025, 6, 15, 10, 30),
            nps_score=7,
            nps_category=NPSCategory.PASSIVE,
            sentiment_label=SentimentLabel.NEUTRAL,
            survey_channel=SurveyChannel.EMAIL,
        )

        db_dict = survey.model_dump_db()

        assert db_dict["survey_type"] == "Annual"
        assert db_dict["nps_category"] == "Passive"
        assert db_dict["sentiment_label"] == "Neutral"
        assert db_dict["survey_channel"] == "Email"


# ============================================================================
# TEST CSATSurveyPendingCreate
# ============================================================================


class TestCSATSurveyPendingCreate:
    """Tests for CSATSurveyPendingCreate model."""

    def test_create_csat_pending(self):
        """Test basic CSAT pending creation."""
        pending = CSATSurveyPendingCreate(
            pending_id=uuid4(),
            survey_reference="CSAT-2025-001234",
            member_id=uuid4(),
            policy_id=uuid4(),
            survey_type=SurveyType.POST_INTERACTION,
            interaction_id=uuid4(),
            simulation_date=date(2025, 6, 15),
            sent_datetime=datetime(2025, 6, 15, 10, 30),
            will_respond=True,
            llm_context={"interaction_type": "Phone"},
        )

        assert pending.survey_type == SurveyType.POST_INTERACTION
        assert pending.will_respond is True

    def test_model_dump_db_enum_conversion(self):
        """Test enum conversion in model_dump_db."""
        pending = CSATSurveyPendingCreate(
            pending_id=uuid4(),
            survey_reference="CSAT-2025-001234",
            member_id=uuid4(),
            policy_id=uuid4(),
            survey_type=SurveyType.POST_COMPLAINT_RESOLUTION,
            simulation_date=date(2025, 6, 15),
            sent_datetime=datetime(2025, 6, 15, 10, 30),
            will_respond=False,
            llm_context={},
            processing_status=ProcessingStatus.COMPLETED,
        )

        db_dict = pending.model_dump_db()

        assert db_dict["survey_type"] == "PostComplaintResolution"
        assert db_dict["processing_status"] == "completed"


# ============================================================================
# TEST CSATSurveyCreate
# ============================================================================


class TestCSATSurveyCreate:
    """Tests for CSATSurveyCreate model."""

    def test_create_csat_survey(self):
        """Test CSAT survey creation with scores."""
        survey = CSATSurveyCreate(
            survey_id=uuid4(),
            survey_reference="CSAT-2025-001234",
            member_id=uuid4(),
            policy_id=uuid4(),
            survey_type=SurveyType.POST_INTERACTION,
            sent_date=datetime(2025, 6, 15, 10, 30),
            completed_date=datetime(2025, 6, 15, 10, 45),
            csat_score=4,
            csat_label=CSATLabel.SATISFIED,
            effort_score=2,
            recommend_agent=True,
            feedback_text="Quick and helpful.",
        )

        assert survey.csat_score == 4
        assert survey.csat_label == CSATLabel.SATISFIED

    def test_csat_score_validation(self):
        """Test that CSAT score must be 1-5."""
        with pytest.raises(ValueError):
            CSATSurveyCreate(
                survey_id=uuid4(),
                survey_reference="CSAT-2025-001234",
                member_id=uuid4(),
                policy_id=uuid4(),
                survey_type=SurveyType.POST_INTERACTION,
                sent_date=datetime(2025, 6, 15, 10, 30),
                csat_score=6,  # Invalid - must be 1-5
            )


# ============================================================================
# TEST SurveyResponsePredictor
# ============================================================================


class TestSurveyResponsePredictor:
    """Tests for SurveyResponsePredictor."""

    def test_predict_nps_response_returns_tuple(self, response_predictor):
        """Test that prediction returns (bool, float) tuple."""
        context = {
            "survey_type": "PostClaim",
            "tenure_months": 24,
            "member_age": 45,
        }

        will_respond, probability = response_predictor.predict_nps_response(context)

        assert isinstance(will_respond, bool)
        assert isinstance(probability, float)
        assert 0 <= probability <= 1

    def test_nps_higher_response_for_rejected_claims(self, rng):
        """Test that rejected claims increase response probability."""
        predictor = SurveyResponsePredictor(rng, {})

        # Run many predictions to get average probability
        base_probs = []
        rejected_probs = []

        for _ in range(100):
            _, prob = predictor.predict_nps_response(
                {"tenure_months": 12, "member_age": 40}
            )
            base_probs.append(prob)

        rng2 = np.random.default_rng(42)  # Reset
        predictor2 = SurveyResponsePredictor(rng2, {})

        for _ in range(100):
            _, prob = predictor2.predict_nps_response(
                {"tenure_months": 12, "member_age": 40, "recent_claim_rejected": True}
            )
            rejected_probs.append(prob)

        # Rejected claims should have higher average probability
        assert np.mean(rejected_probs) > np.mean(base_probs)

    def test_nps_fatigue_reduces_response(self, rng):
        """Test that survey fatigue reduces response probability."""
        predictor = SurveyResponsePredictor(rng, {})

        # No fatigue
        _, prob_fresh = predictor.predict_nps_response(
            {"tenure_months": 12, "surveys_received_6mo": 0}
        )

        # High fatigue
        predictor2 = SurveyResponsePredictor(np.random.default_rng(42), {})
        _, prob_fatigued = predictor2.predict_nps_response(
            {"tenure_months": 12, "surveys_received_6mo": 5}
        )

        assert prob_fatigued < prob_fresh

    def test_predict_csat_response(self, response_predictor):
        """Test CSAT prediction."""
        context = {
            "first_contact_resolution": True,
            "hours_since_interaction": 1,
        }

        will_respond, probability = response_predictor.predict_csat_response(context)

        assert isinstance(will_respond, bool)
        assert 0 <= probability <= 1


# ============================================================================
# TEST CRMStatisticalModels
# ============================================================================


class TestCRMStatisticalModels:
    """Tests for CRMStatisticalModels."""

    def test_sample_nps_score_in_range(self, stats_models):
        """Test NPS score is always 0-10."""
        for _ in range(100):
            score = stats_models.sample_nps_score()
            assert 0 <= score <= 10

    def test_get_nps_category(self, stats_models):
        """Test NPS category mapping."""
        assert stats_models.get_nps_category(10) == "Promoter"
        assert stats_models.get_nps_category(9) == "Promoter"
        assert stats_models.get_nps_category(8) == "Passive"
        assert stats_models.get_nps_category(7) == "Passive"
        assert stats_models.get_nps_category(6) == "Detractor"
        assert stats_models.get_nps_category(0) == "Detractor"

    def test_sample_csat_score_in_range(self, stats_models):
        """Test CSAT score is always 1-5."""
        for _ in range(100):
            score = stats_models.sample_csat_score()
            assert 1 <= score <= 5

    def test_get_csat_label(self, stats_models):
        """Test CSAT label mapping."""
        assert stats_models.get_csat_label(5) == "VerySatisfied"
        assert stats_models.get_csat_label(4) == "Satisfied"
        assert stats_models.get_csat_label(3) == "Neutral"
        assert stats_models.get_csat_label(2) == "Dissatisfied"
        assert stats_models.get_csat_label(1) == "VeryDissatisfied"

    def test_sample_response_time_positive(self, stats_models):
        """Test response time is always positive."""
        for survey_type in ["nps", "csat"]:
            for _ in range(50):
                time = stats_models.sample_response_time_minutes(survey_type)
                assert time > 0

    def test_csat_response_time_faster_than_nps(self, stats_models):
        """Test CSAT response times are generally faster."""
        nps_times = [stats_models.sample_response_time_minutes("nps") for _ in range(100)]
        csat_times = [stats_models.sample_response_time_minutes("csat") for _ in range(100)]

        assert np.median(csat_times) < np.median(nps_times)

    def test_sample_driver_scores_correlated(self, stats_models):
        """Test driver scores are correlated with NPS score."""
        # High NPS should give high drivers
        high_drivers = stats_models.sample_driver_scores(10)
        assert all(v >= 5 for v in high_drivers.values())

        # Low NPS should give low drivers
        low_drivers = stats_models.sample_driver_scores(2)
        assert all(v <= 8 for v in low_drivers.values())

    def test_sample_sentiment_score_correlated(self, stats_models):
        """Test sentiment correlates with NPS."""
        # Promoter should have positive sentiment
        for _ in range(10):
            sentiment = stats_models.sample_sentiment_score(10)
            assert sentiment > 0

        # Detractor should have negative sentiment
        for _ in range(10):
            sentiment = stats_models.sample_sentiment_score(1)
            assert sentiment < 0

    def test_get_sentiment_label(self, stats_models):
        """Test sentiment label mapping."""
        assert stats_models.get_sentiment_label(0.5) == "Positive"
        assert stats_models.get_sentiment_label(0.0) == "Neutral"
        assert stats_models.get_sentiment_label(-0.5) == "Negative"


# ============================================================================
# TEST LLMContextBuilder
# ============================================================================


class TestLLMContextBuilder:
    """Tests for LLMContextBuilder."""

    def test_build_nps_context_structure(self, context_builder):
        """Test NPS context has required keys."""
        # Create mock member and policy
        class MockMember:
            first_name = "John"
            surname = "Smith"
            date_of_birth = date(1980, 5, 15)
            state = "NSW"

        class MockPolicy:
            start_date = date(2022, 1, 1)
            policy_type = "Family"
            tier = "Medium"
            hospital_product_name = "Bronze Plus"
            extras_product_name = "Core Extras"
            premium_amount = Decimal("250.00")

        context = context_builder.build_nps_context(
            member_data={"member": MockMember()},
            policy_data={"policy": MockPolicy()},
            trigger_event="ClaimPaid",
            trigger_entity=None,
            simulation_date=date(2025, 6, 15),
        )

        # Check required keys exist
        assert "member_name" in context
        assert "member_age" in context
        assert "tenure_months" in context
        assert "trigger_event" in context
        assert "claim_history" in context
        assert "interaction_history" in context

        # Check values
        assert context["member_name"] == "John Smith"
        assert context["trigger_event"] == "ClaimPaid"
        assert context["tenure_months"] > 0

    def test_build_csat_context_structure(self, context_builder):
        """Test CSAT context has required keys."""

        class MockMember:
            first_name = "Jane"
            surname = "Doe"
            date_of_birth = date(1975, 8, 20)

        class MockPolicy:
            start_date = date(2023, 6, 1)

        interaction_data = {
            "interaction_type": "Phone",
            "channel": "Phone",
            "duration_seconds": 300,
            "fcr": True,
        }

        context = context_builder.build_csat_context(
            member_data={"member": MockMember()},
            policy_data={"policy": MockPolicy()},
            interaction_data=interaction_data,
            case_data=None,
            simulation_date=date(2025, 6, 15),
        )

        assert "member_name" in context
        assert "interaction_type" in context
        assert "first_contact_resolution" in context
        assert context["interaction_type"] == "Phone"
        assert context["first_contact_resolution"] is True

    def test_history_limits_applied(self, context_builder):
        """Test that history limits are applied."""
        # Create more claims than the limit
        claims_history = [{"claim_id": i} for i in range(10)]

        context = context_builder.build_nps_context(
            member_data={"member": None},
            policy_data={"policy": None},
            trigger_event="Annual",
            trigger_entity=None,
            claims_history=claims_history,
            simulation_date=date(2025, 6, 15),
        )

        # Should be limited to max_claims_history (5)
        assert len(context["claim_history"]) <= 5


# ============================================================================
# TEST LLM Response Models
# ============================================================================


class TestNPSSurveyLLMResponse:
    """Tests for NPSSurveyLLMResponse."""

    def test_valid_response(self):
        """Test valid LLM response parsing."""
        response = NPSSurveyLLMResponse(
            nps_score=8,
            driver_claims_processing=9,
            driver_customer_service=8,
            driver_value_for_money=7,
            driver_coverage_clarity=8,
            driver_digital_experience=9,
            feedback_text="Very happy with the service.",
            sentiment_score=0.8,
            sentiment_label="Positive",
            feedback_themes=["service", "speed"],
            follow_up_consent=True,
        )

        assert response.nps_score == 8
        assert response.sentiment_label == "Positive"

    def test_score_validation(self):
        """Test score validation."""
        with pytest.raises(ValueError):
            NPSSurveyLLMResponse(
                nps_score=11,  # Invalid
                driver_claims_processing=5,
                driver_customer_service=5,
                driver_value_for_money=5,
                driver_coverage_clarity=5,
                driver_digital_experience=5,
                feedback_text="Test",
                sentiment_score=0.0,
                sentiment_label="Neutral",
                feedback_themes=[],
                follow_up_consent=False,
            )


class TestCSATSurveyLLMResponse:
    """Tests for CSATSurveyLLMResponse."""

    def test_valid_response(self):
        """Test valid CSAT response."""
        response = CSATSurveyLLMResponse(
            csat_score=4,
            effort_score=2,
            recommend_agent=True,
            feedback_text="Quick resolution.",
            sentiment_label="Positive",
        )

        assert response.csat_score == 4
        assert response.recommend_agent is True

    def test_csat_score_range(self):
        """Test CSAT score must be 1-5."""
        with pytest.raises(ValueError):
            CSATSurveyLLMResponse(
                csat_score=0,  # Invalid - min is 1
                effort_score=3,
                recommend_agent=True,
                feedback_text="Test",
                sentiment_label="Neutral",
            )
