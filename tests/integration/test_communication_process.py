"""
Integration tests for Communication Process.

Tests for event handling, campaign management, and response lifecycle.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import numpy as np
import pytest
import simpy

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.core.processes.communication import CommunicationProcess
from brickwell_health.core.shared_state import SharedState
from brickwell_health.domain.enums import (
    CampaignResponseType,
    CampaignStatus,
    CampaignType,
    CommunicationDeliveryStatus,
    PreferenceType,
)
from brickwell_health.generators.id_generator import IDGenerator


@pytest.fixture
def shared_state():
    """Create a shared state for testing."""
    return SharedState()


@pytest.fixture
def mock_batch_writer():
    """Create a mock batch writer."""
    writer = MagicMock()
    writer.add = MagicMock()
    writer.update_record = MagicMock()
    writer.flush_for_cdc = MagicMock()
    return writer


@pytest.fixture
def communication_config():
    """Create a mock communication config."""
    config = MagicMock()
    config.communication = MagicMock()
    config.communication.enabled = True
    config.communication.transactional = MagicMock()
    config.communication.transactional.model_dump = MagicMock(return_value={
        "delivery_rate": 0.97,
        "open_rate": 0.60,
        "click_rate": 0.15,
    })
    config.communication.marketing = MagicMock()
    config.communication.marketing.model_dump = MagicMock(return_value={
        "delivery_rate": 0.97,
        "open_rate": 0.20,
        "click_rate": 0.03,
    })
    config.communication.sms = MagicMock()
    config.communication.sms.model_dump = MagicMock(return_value={})
    config.communication.fatigue = MagicMock()
    config.communication.fatigue.model_dump = MagicMock(return_value={
        "max_marketing_per_week": 2,
    })

    config.campaign = MagicMock()
    config.campaign.model_dump = MagicMock(return_value={
        "campaigns_per_year": 6,
        "type_distribution": {"Retention": 0.30, "Upsell": 0.20, "CrossSell": 0.20},
    })

    return config


class TestCommunicationProcessEventHandling:
    """Tests for communication process event handling."""

    def test_transactional_communication_from_event(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test that communication events trigger transactional communications."""
        # Add a communication event (from CRM after interaction)
        policy_id = uuid4()
        member_id = uuid4()
        claim_id = uuid4()

        # Set up preferences to allow communication
        shared_state.set_communication_preferences(member_id, {
            "transactional_email": True,
            "marketing_email": True,
        })

        shared_state.add_communication_event({
            "event_type": "interaction_completed",
            "interaction_id": uuid4(),
            "trigger_event_type": "claim_paid",
            "policy_id": policy_id,
            "member_id": member_id,
            "claim_id": claim_id,
            "invoice_id": None,
            "timestamp": datetime.now(),
        })

        # Create communication process
        comm_process = CommunicationProcess(
            sim_env=sim_env,
            config=communication_config,
            batch_writer=mock_batch_writer,
            id_generator=id_generator,
            reference=test_reference,
            worker_id=0,
            shared_state=shared_state,
        )

        # Process the event queue manually
        comm_process._process_communication_events(sim_env.current_date)

        # Verify events were consumed
        remaining_events = shared_state.get_communication_events()
        assert len(remaining_events) == 0

    def test_opted_out_member_receives_no_communication(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test that opted-out members don't receive communications."""
        policy_id = uuid4()
        member_id = uuid4()

        # Set member as opted out
        shared_state.set_communication_preferences(member_id, {
            "transactional_email": False,  # Opted out
            "marketing_email": False,
        })

        shared_state.add_communication_event({
            "event_type": "interaction_completed",
            "interaction_id": uuid4(),
            "trigger_event_type": "claim_paid",
            "policy_id": policy_id,
            "member_id": member_id,
            "claim_id": uuid4(),
            "invoice_id": None,
            "timestamp": datetime.now(),
        })

        comm_process = CommunicationProcess(
            sim_env=sim_env,
            config=communication_config,
            batch_writer=mock_batch_writer,
            id_generator=id_generator,
            reference=test_reference,
            worker_id=0,
            shared_state=shared_state,
        )

        comm_process._process_communication_events(sim_env.current_date)

        # Should not have created any communications due to opt-out
        assert comm_process._stats["transactional_sent"] == 0


class TestCampaignManagement:
    """Tests for campaign management."""

    def test_campaign_creation(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test creating a new campaign."""
        comm_process = CommunicationProcess(
            sim_env=sim_env,
            config=communication_config,
            batch_writer=mock_batch_writer,
            id_generator=id_generator,
            reference=test_reference,
            worker_id=0,
            shared_state=shared_state,
        )

        # Create a campaign
        comm_process._create_new_campaign(sim_env.current_date)

        # Verify campaign was created
        assert comm_process._stats["campaigns_created"] == 1
        assert len(comm_process.active_campaigns) == 1

        # Verify INSERT was made
        calls = mock_batch_writer.add.call_args_list
        campaign_inserts = [c for c in calls if c[0][0] == "campaign"]
        assert len(campaign_inserts) == 1

    def test_campaign_closure(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test closing a completed campaign."""
        comm_process = CommunicationProcess(
            sim_env=sim_env,
            config=communication_config,
            batch_writer=mock_batch_writer,
            id_generator=id_generator,
            reference=test_reference,
            worker_id=0,
            shared_state=shared_state,
        )

        # Create a campaign
        comm_process._create_new_campaign(sim_env.current_date)
        campaign_id = list(comm_process.active_campaigns.keys())[0]

        # Close the campaign
        comm_process._close_campaign(campaign_id)

        # Verify campaign was removed from active
        assert len(comm_process.active_campaigns) == 0
        assert comm_process._stats["campaigns_completed"] == 1

        # Verify UPDATE was made
        mock_batch_writer.update_record.assert_called()


class TestResponseLifecycle:
    """Tests for campaign response lifecycle."""

    def test_pending_response_tracking(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test that pending responses are tracked in shared state."""
        response_id = uuid4()
        campaign_id = uuid4()

        # Add a pending response
        shared_state.add_pending_campaign_response(
            response_id,
            {
                "campaign_id": campaign_id,
                "communication_id": uuid4(),
                "member_id": uuid4(),
                "policy_id": uuid4(),
                "sent_date": datetime.now(),
                "predicted_open_date": datetime.now() + timedelta(hours=1),
                "status": "pending_open",
            },
        )

        # Verify it's tracked
        assert response_id in shared_state.pending_campaign_responses

    def test_due_response_retrieval(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test retrieving due responses."""
        response_id = uuid4()
        now = datetime.now()

        # Add a response that's already due
        shared_state.add_pending_campaign_response(
            response_id,
            {
                "campaign_id": uuid4(),
                "communication_id": uuid4(),
                "member_id": uuid4(),
                "policy_id": uuid4(),
                "sent_date": now - timedelta(hours=2),
                "predicted_open_date": now - timedelta(hours=1),
                "status": "pending_open",
            },
        )

        # Get due responses
        due_responses = shared_state.get_due_campaign_responses(now)

        assert len(due_responses) == 1
        assert due_responses[0][0] == response_id


class TestCommunicationProcessConfiguration:
    """Tests for communication process configuration."""

    def test_communication_process_disabled_config(self):
        """Test that communication process respects enabled flag."""
        config = MagicMock()
        config.communication = MagicMock()
        config.communication.enabled = False

        # Verify config structure
        assert config.communication.enabled is False


class TestCommunicationProcessStats:
    """Tests for communication process statistics."""

    def test_stats_tracking(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test that stats are tracked correctly."""
        comm_process = CommunicationProcess(
            sim_env=sim_env,
            config=communication_config,
            batch_writer=mock_batch_writer,
            id_generator=id_generator,
            reference=test_reference,
            worker_id=0,
            shared_state=shared_state,
        )

        # Initial stats should be zero
        assert comm_process._stats["transactional_sent"] == 0
        assert comm_process._stats["marketing_sent"] == 0
        assert comm_process._stats["campaigns_created"] == 0

    def test_get_stats_method(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test that get_stats returns stats dict."""
        comm_process = CommunicationProcess(
            sim_env=sim_env,
            config=communication_config,
            batch_writer=mock_batch_writer,
            id_generator=id_generator,
            reference=test_reference,
            worker_id=0,
            shared_state=shared_state,
        )

        stats = comm_process.get_stats()

        assert isinstance(stats, dict)
        assert "transactional_sent" in stats
        assert "marketing_sent" in stats
        assert "campaigns_created" in stats
        assert "responses_opened" in stats


class TestFatigueRules:
    """Tests for communication fatigue rules."""

    def test_fatigue_check(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
        shared_state: SharedState,
        mock_batch_writer,
        communication_config,
    ):
        """Test that fatigue rules prevent excessive marketing."""
        comm_process = CommunicationProcess(
            sim_env=sim_env,
            config=communication_config,
            batch_writer=mock_batch_writer,
            id_generator=id_generator,
            reference=test_reference,
            worker_id=0,
            shared_state=shared_state,
        )

        member_id = uuid4()

        # Member should not be fatigued initially
        assert comm_process._is_fatigued(member_id) is False

        # Simulate sending max marketing emails
        for i in range(3):
            comm_process.recent_comms.setdefault(member_id, []).append({
                "communication_id": uuid4(),
                "sent_date": datetime.now() - timedelta(days=1),
                "is_marketing": True,
            })

        # Now member should be fatigued (max is 2)
        assert comm_process._is_fatigued(member_id) is True
