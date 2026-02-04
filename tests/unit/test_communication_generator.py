"""
Unit tests for Communication Domain Generators.

Tests for CommunicationPreferenceGenerator, CommunicationGenerator,
CampaignGenerator, CampaignResponseGenerator, and communication domain models.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.domain.communication import (
    CommunicationPreferenceCreate,
    CampaignCreate,
    CommunicationCreate,
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
from brickwell_health.generators.communication_generator import (
    CommunicationPreferenceGenerator,
    CommunicationGenerator,
    CampaignGenerator,
    CampaignResponseGenerator,
)
from brickwell_health.generators.id_generator import IDGenerator


# =============================================================================
# DOMAIN MODEL TESTS
# =============================================================================


class TestCommunicationPreferenceCreate:
    """Tests for CommunicationPreferenceCreate domain model."""

    def test_preference_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        preference = CommunicationPreferenceCreate(
            preference_id=uuid4(),
            member_id=uuid4(),
            policy_id=uuid4(),
            preference_type=PreferenceType.MARKETING,
            channel="Email",
            is_opted_in=True,
        )

        data = preference.model_dump_db()

        assert data["preference_type"] == "Marketing"
        assert data["channel"] == "Email"
        assert data["is_opted_in"] is True

    def test_preference_opt_out(self):
        """Test preference with opt-out."""
        preference = CommunicationPreferenceCreate(
            preference_id=uuid4(),
            member_id=uuid4(),
            policy_id=uuid4(),
            preference_type=PreferenceType.TRANSACTIONAL,
            channel="SMS",
            is_opted_in=False,
            opt_out_date=date.today(),
        )

        assert preference.is_opted_in is False
        assert preference.opt_out_date == date.today()


class TestCampaignCreate:
    """Tests for CampaignCreate domain model."""

    def test_campaign_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        campaign = CampaignCreate(
            campaign_id=uuid4(),
            campaign_code="RET-2024-001",
            campaign_name="Q1 Retention Campaign",
            campaign_type=CampaignType.RETENTION,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 3, 31),
            status=CampaignStatus.ACTIVE,
        )

        data = campaign.model_dump_db()

        assert data["campaign_type"] == "Retention"
        assert data["status"] == "Active"

    def test_campaign_with_metrics(self):
        """Test campaign with performance metrics."""
        campaign = CampaignCreate(
            campaign_id=uuid4(),
            campaign_code="UP-2024-001",
            campaign_name="Upsell Campaign",
            campaign_type=CampaignType.UPSELL,
            start_date=date(2024, 1, 1),
            status=CampaignStatus.COMPLETED,
            target_response_rate=Decimal("0.05"),
            actual_response_rate=Decimal("0.06"),
            communications_sent=1000,
            responses_received=60,
        )

        assert campaign.communications_sent == 1000
        assert campaign.responses_received == 60
        assert campaign.actual_response_rate > campaign.target_response_rate


class TestCommunicationCreate:
    """Tests for CommunicationCreate domain model."""

    def test_communication_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        communication = CommunicationCreate(
            communication_id=uuid4(),
            communication_reference="COM-W0-2024-000001",
            policy_id=uuid4(),
            member_id=uuid4(),
            communication_type=CommunicationType.EMAIL,
            delivery_status=CommunicationDeliveryStatus.DELIVERED,
        )

        data = communication.model_dump_db()

        assert data["communication_type"] == "Email"
        assert data["delivery_status"] == "Delivered"

    def test_communication_with_trigger(self):
        """Test communication with trigger context."""
        claim_id = uuid4()
        communication = CommunicationCreate(
            communication_id=uuid4(),
            communication_reference="COM-W0-2024-000002",
            policy_id=uuid4(),
            member_id=uuid4(),
            communication_type=CommunicationType.EMAIL,
            delivery_status=CommunicationDeliveryStatus.SENT,
            trigger_event_type=TriggerEventType.CLAIM_PAID,
            trigger_event_id=claim_id,
            claim_id=claim_id,
        )

        data = communication.model_dump_db()

        assert data["trigger_event_type"] == "ClaimPaid"
        assert data["claim_id"] == claim_id


class TestCampaignResponseCreate:
    """Tests for CampaignResponseCreate domain model."""

    def test_response_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        response = CampaignResponseCreate(
            response_id=uuid4(),
            campaign_id=uuid4(),
            member_id=uuid4(),
            policy_id=uuid4(),
            response_type=CampaignResponseType.OPENED,
            response_date=datetime.now(),
        )

        data = response.model_dump_db()

        assert data["response_type"] == "Opened"

    def test_response_with_conversion(self):
        """Test response with conversion."""
        response = CampaignResponseCreate(
            response_id=uuid4(),
            campaign_id=uuid4(),
            member_id=uuid4(),
            policy_id=uuid4(),
            response_type=CampaignResponseType.CONVERTED,
            response_date=datetime.now(),
            conversion_type=ConversionType.UPGRADED,
            conversion_value=Decimal("250.00"),
        )

        data = response.model_dump_db()

        assert data["response_type"] == "Converted"
        assert data["conversion_type"] == "Upgraded"
        assert data["conversion_value"] == Decimal("250.00")


# =============================================================================
# COMMUNICATION PREFERENCE GENERATOR TESTS
# =============================================================================


class TestCommunicationPreferenceGenerator:
    """Tests for CommunicationPreferenceGenerator."""

    @pytest.fixture
    def preference_generator(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Create a preference generator for testing."""
        return CommunicationPreferenceGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

    def test_generate_single_preference(self, preference_generator):
        """Test generating a single preference."""
        member_id = uuid4()
        policy_id = uuid4()

        preference = preference_generator.generate(
            member_id=member_id,
            policy_id=policy_id,
            preference_type=PreferenceType.MARKETING,
            channel="Email",
        )

        assert preference.member_id == member_id
        assert preference.policy_id == policy_id
        assert preference.preference_type == PreferenceType.MARKETING
        assert preference.channel == "Email"
        assert preference.preference_id is not None

    def test_generate_default_preferences(self, preference_generator):
        """Test generating default preferences for all types/channels."""
        member_id = uuid4()
        policy_id = uuid4()

        preferences = preference_generator.generate_default_preferences(
            member_id=member_id,
            policy_id=policy_id,
        )

        # Should have 12 preferences: 3 types x 4 channels
        assert len(preferences) == 12

        # Verify all combinations are present
        types_seen = set()
        channels_seen = set()
        for pref in preferences:
            types_seen.add(pref.preference_type)
            channels_seen.add(pref.channel)

        assert len(types_seen) == 3  # TRANSACTIONAL, MARKETING, CLAIMS
        assert len(channels_seen) == 4  # Email, SMS, Post, Phone


# =============================================================================
# COMMUNICATION GENERATOR TESTS
# =============================================================================


class TestCommunicationGenerator:
    """Tests for CommunicationGenerator."""

    @pytest.fixture
    def communication_generator(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Create a communication generator for testing."""
        return CommunicationGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

    def test_generate_basic_communication(self, communication_generator):
        """Test generating a basic communication."""
        policy_id = uuid4()
        member_id = uuid4()

        communication = communication_generator.generate(
            policy_id=policy_id,
            member_id=member_id,
            template_code="CLAIM_PAID",
        )

        assert communication.policy_id == policy_id
        assert communication.member_id == member_id
        assert communication.communication_id is not None
        assert communication.communication_reference.startswith("COM-")
        assert communication.communication_type in CommunicationType
        assert communication.delivery_status in CommunicationDeliveryStatus

    def test_generate_communication_with_trigger(self, communication_generator):
        """Test generating a communication with trigger context."""
        claim_id = uuid4()

        communication = communication_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            template_code="CLAIM_PAID",
            trigger_event_type=TriggerEventType.CLAIM_PAID,
            trigger_event_id=claim_id,
            claim_id=claim_id,
        )

        assert communication.trigger_event_type == TriggerEventType.CLAIM_PAID
        assert communication.claim_id == claim_id

    def test_generate_marketing_communication(self, communication_generator):
        """Test generating a marketing communication."""
        campaign_id = uuid4()

        communication = communication_generator.generate(
            policy_id=uuid4(),
            member_id=uuid4(),
            template_code="MARKETING_CAMPAIGN",
            campaign_id=campaign_id,
        )

        assert communication.campaign_id == campaign_id


# =============================================================================
# CAMPAIGN GENERATOR TESTS
# =============================================================================


class TestCampaignGenerator:
    """Tests for CampaignGenerator."""

    @pytest.fixture
    def campaign_generator(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Create a campaign generator for testing."""
        return CampaignGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

    def test_generate_basic_campaign(self, campaign_generator):
        """Test generating a basic campaign."""
        campaign = campaign_generator.generate(
            campaign_type=CampaignType.RETENTION,
        )

        assert campaign.campaign_id is not None
        assert campaign.campaign_code is not None
        assert campaign.campaign_type == CampaignType.RETENTION
        assert campaign.status == CampaignStatus.ACTIVE
        assert campaign.start_date is not None
        assert campaign.end_date is not None
        assert campaign.end_date > campaign.start_date

    def test_generate_campaign_with_start_date(self, campaign_generator):
        """Test generating a campaign with specific start date."""
        start = date(2024, 3, 1)

        campaign = campaign_generator.generate(
            campaign_type=CampaignType.UPSELL,
            start_date=start,
        )

        assert campaign.start_date == start
        assert campaign.campaign_type == CampaignType.UPSELL

    def test_campaign_has_name_and_description(self, campaign_generator):
        """Test that campaign has name and description."""
        campaign = campaign_generator.generate(
            campaign_type=CampaignType.CROSS_SELL,
        )

        assert campaign.campaign_name is not None
        assert len(campaign.campaign_name) > 0
        assert campaign.description is not None


# =============================================================================
# CAMPAIGN RESPONSE GENERATOR TESTS
# =============================================================================


class TestCampaignResponseGenerator:
    """Tests for CampaignResponseGenerator."""

    @pytest.fixture
    def response_generator(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Create a campaign response generator for testing."""
        return CampaignResponseGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

    def test_generate_basic_response(self, response_generator):
        """Test generating a basic response."""
        campaign_id = uuid4()
        member_id = uuid4()
        policy_id = uuid4()
        communication_id = uuid4()

        response = response_generator.generate(
            campaign_id=campaign_id,
            member_id=member_id,
            policy_id=policy_id,
            communication_id=communication_id,
            response_type=CampaignResponseType.OPENED,
        )

        assert response.campaign_id == campaign_id
        assert response.member_id == member_id
        assert response.policy_id == policy_id
        assert response.communication_id == communication_id
        assert response.response_type == CampaignResponseType.OPENED
        assert response.response_id is not None

    def test_generate_converted_response(self, response_generator):
        """Test generating a converted response."""
        response = response_generator.generate(
            campaign_id=uuid4(),
            member_id=uuid4(),
            policy_id=uuid4(),
            communication_id=uuid4(),
            response_type=CampaignResponseType.CONVERTED,
            conversion_type=ConversionType.UPGRADED,
        )

        assert response.response_type == CampaignResponseType.CONVERTED
        assert response.conversion_type == ConversionType.UPGRADED
        assert response.conversion_value is not None
        assert response.conversion_value > 0

    def test_get_conversion_type_for_campaign(self, response_generator):
        """Test getting conversion type for campaign type."""
        assert response_generator.get_conversion_type_for_campaign(
            CampaignType.RETENTION
        ) == ConversionType.RENEWED

        assert response_generator.get_conversion_type_for_campaign(
            CampaignType.UPSELL
        ) == ConversionType.UPGRADED

        assert response_generator.get_conversion_type_for_campaign(
            CampaignType.CROSS_SELL
        ) == ConversionType.ADDED_COVER
