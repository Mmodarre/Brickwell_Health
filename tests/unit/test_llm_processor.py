"""
Unit tests for LLM Survey Processor.

Minimal tests focusing on config validation and prompt building.
Full integration tests require Databricks connection.
"""

import pytest
from datetime import date

from brickwell_health.config.models import (
    SimulationConfig,
    LLMConfig,
    DatabricksConfig,
    DatabaseConfig,
    SimulationTimeConfig,
    ScaleConfig,
    ParallelConfig,
)
from brickwell_health.core.llm_processor import (
    clean_json_response,
    get_nps_category,
    get_csat_label,
    LLMSurveyProcessor,
    DEFAULT_NPS_PROMPT,
    DEFAULT_CSAT_PROMPT,
)


class TestCleanJsonResponse:
    """Tests for JSON response cleaning utility."""

    def test_clean_plain_json(self):
        """Plain JSON should pass through unchanged."""
        json_str = '{"nps_score": 8}'
        result = clean_json_response(json_str)
        assert result == '{"nps_score": 8}'

    def test_clean_markdown_json_block(self):
        """JSON wrapped in markdown code block should be unwrapped."""
        json_str = '```json\n{"nps_score": 8}\n```'
        result = clean_json_response(json_str)
        assert result == '{"nps_score": 8}'

    def test_clean_plain_code_block(self):
        """JSON wrapped in plain code block should be unwrapped."""
        json_str = '```\n{"nps_score": 8}\n```'
        result = clean_json_response(json_str)
        assert result == '{"nps_score": 8}'

    def test_clean_with_whitespace(self):
        """Whitespace should be trimmed."""
        json_str = '  \n{"nps_score": 8}\n  '
        result = clean_json_response(json_str)
        assert result == '{"nps_score": 8}'


class TestNpsCategory:
    """Tests for NPS category classification."""

    def test_promoter_score_9(self):
        """Score 9 should be Promoter."""
        assert get_nps_category(9) == "Promoter"

    def test_promoter_score_10(self):
        """Score 10 should be Promoter."""
        assert get_nps_category(10) == "Promoter"

    def test_passive_score_7(self):
        """Score 7 should be Passive."""
        assert get_nps_category(7) == "Passive"

    def test_passive_score_8(self):
        """Score 8 should be Passive."""
        assert get_nps_category(8) == "Passive"

    def test_detractor_score_6(self):
        """Score 6 should be Detractor."""
        assert get_nps_category(6) == "Detractor"

    def test_detractor_score_0(self):
        """Score 0 should be Detractor."""
        assert get_nps_category(0) == "Detractor"


class TestCsatLabel:
    """Tests for CSAT label classification."""

    def test_csat_labels(self):
        """CSAT scores should map to correct labels."""
        assert get_csat_label(1) == "VeryDissatisfied"
        assert get_csat_label(2) == "Dissatisfied"
        assert get_csat_label(3) == "Neutral"
        assert get_csat_label(4) == "Satisfied"
        assert get_csat_label(5) == "VerySatisfied"

    def test_csat_invalid_score(self):
        """Invalid scores should default to Neutral."""
        assert get_csat_label(0) == "Neutral"
        assert get_csat_label(6) == "Neutral"


class TestDatabricksConfigValidation:
    """Tests for Databricks config validation."""

    def test_is_configured_all_fields(self):
        """Should return True when all fields are set."""
        config = DatabricksConfig(
            host="https://workspace.databricks.com",
            token="dapi123456",
            http_path="/sql/1.0/warehouses/abc123",
        )
        assert config.is_configured() is True

    def test_is_configured_missing_host(self):
        """Should return False when host is missing."""
        config = DatabricksConfig(
            host="",
            token="dapi123456",
            http_path="/sql/1.0/warehouses/abc123",
        )
        assert config.is_configured() is False

    def test_is_configured_missing_token(self):
        """Should return False when token is missing."""
        config = DatabricksConfig(
            host="https://workspace.databricks.com",
            token="",
            http_path="/sql/1.0/warehouses/abc123",
        )
        assert config.is_configured() is False

    def test_is_configured_missing_http_path(self):
        """Should return False when http_path is missing."""
        config = DatabricksConfig(
            host="https://workspace.databricks.com",
            token="dapi123456",
            http_path="",
        )
        assert config.is_configured() is False


class TestLLMSurveyProcessorInit:
    """Tests for LLMSurveyProcessor initialization."""

    @pytest.fixture
    def mock_config(self):
        """Create a minimal config for testing."""
        return SimulationConfig(
            simulation=SimulationTimeConfig(
                start_date=date(2020, 1, 1),
                end_date=date(2025, 12, 31),
            ),
            scale=ScaleConfig(target_member_count=1000),
            database=DatabaseConfig(
                host="localhost",
                port=5432,
                database="test_db",
                user="test_user",
                password="test_pass",
            ),
            llm=LLMConfig(
                enabled=True,
                databricks=DatabricksConfig(
                    host="https://workspace.databricks.com",
                    token="dapi123456",
                    http_path="/sql/1.0/warehouses/abc123",
                ),
                model="test-model",
                batch_size=25,
            ),
            parallel=ParallelConfig(num_workers=1),
        )

    def test_processor_init_uses_config_values(self, mock_config):
        """Processor should use config values."""
        processor = LLMSurveyProcessor(mock_config, dry_run=True)

        assert processor.batch_size == 25
        assert processor.llm_model == "test-model"
        assert processor.databricks_host == "workspace.databricks.com"
        assert processor.dry_run is True

    def test_processor_uses_default_prompts(self, mock_config):
        """Processor should use default prompts when not provided in config."""
        processor = LLMSurveyProcessor(mock_config, dry_run=True)

        assert processor.nps_prompt_template == DEFAULT_NPS_PROMPT
        assert processor.csat_prompt_template == DEFAULT_CSAT_PROMPT

    def test_processor_uses_config_prompts(self, mock_config):
        """Processor should use config prompts when provided."""
        mock_config.llm.prompts = {
            "nps_survey": "Custom NPS prompt: {member_context}",
            "csat_survey": "Custom CSAT prompt: {interaction_context}",
        }
        processor = LLMSurveyProcessor(mock_config, dry_run=True)

        assert processor.nps_prompt_template == "Custom NPS prompt: {member_context}"
        assert processor.csat_prompt_template == "Custom CSAT prompt: {interaction_context}"


class TestPromptBuilding:
    """Tests for prompt building functionality."""

    @pytest.fixture
    def mock_config(self):
        """Create a minimal config for testing."""
        return SimulationConfig(
            simulation=SimulationTimeConfig(
                start_date=date(2020, 1, 1),
                end_date=date(2025, 12, 31),
            ),
            scale=ScaleConfig(target_member_count=1000),
            database=DatabaseConfig(
                host="localhost",
                port=5432,
                database="test_db",
                user="test_user",
                password="test_pass",
            ),
            llm=LLMConfig(
                enabled=True,
                databricks=DatabricksConfig(
                    host="https://workspace.databricks.com",
                    token="dapi123456",
                    http_path="/sql/1.0/warehouses/abc123",
                ),
            ),
            parallel=ParallelConfig(num_workers=1),
        )

    def test_build_prior_surveys_section_empty(self, mock_config):
        """Empty prior context should return empty string."""
        processor = LLMSurveyProcessor(mock_config, dry_run=True)

        result = processor._build_prior_surveys_section(None)
        assert result == ""

        result = processor._build_prior_surveys_section({})
        assert result == ""

        result = processor._build_prior_surveys_section({"prior_surveys": []})
        assert result == ""

    def test_build_prior_surveys_section_with_data(self, mock_config):
        """Prior context should be formatted correctly."""
        processor = LLMSurveyProcessor(mock_config, dry_run=True)

        prior_context = {
            "prior_surveys": [
                {
                    "survey_date": "2024-01-15",
                    "nps_score": 6,
                    "nps_category": "Detractor",
                    "trigger_event": "ClaimRejection",
                    "feedback_summary": "Frustrated with claim process.",
                }
            ],
            "trajectory": "declining",
            "average_prior_nps": 6.0,
            "survey_count": 1,
        }

        result = processor._build_prior_surveys_section(prior_context)

        assert "=== PRIOR NPS SURVEYS ===" in result
        assert "Survey History: 1 prior surveys" in result
        assert "NPS Trajectory: declining" in result
        assert "Average Prior NPS: 6.0" in result
        assert "NPS: 6 (Detractor)" in result
        assert "Trigger: ClaimRejection" in result
        assert "Frustrated with claim process" in result
