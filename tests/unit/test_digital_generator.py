"""
Unit tests for Digital Behavior Domain Generator.

Tests for DigitalBehaviorGenerator and digital domain models.
"""

from datetime import date, datetime
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.domain.digital import (
    WebSessionCreate,
    DigitalEventCreate,
)
from brickwell_health.domain.enums import (
    DeviceType,
    DigitalEventType,
    PageCategory,
    SessionType,
    TriggerEventType,
)
from brickwell_health.generators.digital_generator import DigitalBehaviorGenerator
from brickwell_health.generators.id_generator import IDGenerator


# =============================================================================
# DOMAIN MODEL TESTS
# =============================================================================


class TestWebSessionCreate:
    """Tests for WebSessionCreate domain model."""

    def test_session_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        session = WebSessionCreate(
            session_id=uuid4(),
            member_id=uuid4(),
            policy_id=uuid4(),
            session_start=datetime.now(),
            session_end=datetime.now(),
            duration_seconds=300,
            page_count=5,
            event_count=12,
            device_type=DeviceType.MOBILE,
            session_type=SessionType.APP,
            browser="Chrome",
            operating_system="iOS",
        )

        data = session.model_dump_db()

        assert data["device_type"] == "Mobile"
        assert data["session_type"] == "App"
        assert data["duration_seconds"] == 300
        assert data["page_count"] == 5

    def test_session_with_intent_signals(self):
        """Test session with intent signals."""
        session = WebSessionCreate(
            session_id=uuid4(),
            member_id=uuid4(),
            session_start=datetime.now(),
            viewed_cancel_page=True,
            viewed_claims_page=True,
            viewed_billing_page=False,
        )

        assert session.viewed_cancel_page is True
        assert session.viewed_claims_page is True
        assert session.viewed_billing_page is False

    def test_session_with_trigger(self):
        """Test session with trigger context."""
        claim_id = uuid4()
        session = WebSessionCreate(
            session_id=uuid4(),
            member_id=uuid4(),
            session_start=datetime.now(),
            trigger_event_type=TriggerEventType.CLAIM_REJECTED,
            trigger_event_id=claim_id,
        )

        data = session.model_dump_db()

        assert data["trigger_event_type"] == "ClaimRejected"
        assert data["trigger_event_id"] == claim_id


class TestDigitalEventCreate:
    """Tests for DigitalEventCreate domain model."""

    def test_event_model_dump_db(self):
        """Test model_dump_db converts enums to values."""
        event = DigitalEventCreate(
            event_id=uuid4(),
            session_id=uuid4(),
            member_id=uuid4(),
            event_timestamp=datetime.now(),
            event_type=DigitalEventType.PAGE_VIEW,
            page_path="/claims/submit",
            page_category=PageCategory.CLAIMS,
            page_title="Submit a Claim | Brickwell Health",
        )

        data = event.model_dump_db()

        assert data["event_type"] == "PageView"
        assert data["page_category"] == "Claims"
        assert data["page_path"] == "/claims/submit"

    def test_click_event(self):
        """Test click event with element details."""
        event = DigitalEventCreate(
            event_id=uuid4(),
            session_id=uuid4(),
            member_id=uuid4(),
            event_timestamp=datetime.now(),
            event_type=DigitalEventType.CLICK,
            page_path="/billing",
            page_category=PageCategory.BILLING,
            element_id="btn-pay-now",
            element_text="Pay Now",
        )

        assert event.event_type == DigitalEventType.CLICK
        assert event.element_id == "btn-pay-now"
        assert event.element_text == "Pay Now"

    def test_search_event(self):
        """Test search event with query details."""
        event = DigitalEventCreate(
            event_id=uuid4(),
            session_id=uuid4(),
            member_id=uuid4(),
            event_timestamp=datetime.now(),
            event_type=DigitalEventType.SEARCH,
            page_path="/faq",
            page_category=PageCategory.FAQ,
            search_query="dental cover",
            search_results_count=5,
        )

        assert event.event_type == DigitalEventType.SEARCH
        assert event.search_query == "dental cover"
        assert event.search_results_count == 5

    def test_form_submit_event(self):
        """Test form submission event."""
        event = DigitalEventCreate(
            event_id=uuid4(),
            session_id=uuid4(),
            member_id=uuid4(),
            event_timestamp=datetime.now(),
            event_type=DigitalEventType.FORM_SUBMIT,
            page_path="/claims/submit",
            page_category=PageCategory.CLAIMS,
            form_name="claim_submission",
            form_completed=True,
        )

        assert event.event_type == DigitalEventType.FORM_SUBMIT
        assert event.form_name == "claim_submission"
        assert event.form_completed is True


# =============================================================================
# GENERATOR TESTS
# =============================================================================


class TestDigitalBehaviorGenerator:
    """Tests for DigitalBehaviorGenerator."""

    @pytest.fixture
    def generator(self, test_rng, test_reference, id_generator, sim_env):
        """Create a generator instance for testing."""
        return DigitalBehaviorGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

    def test_generate_session_basic(self, generator):
        """Test basic session generation."""
        member_id = uuid4()
        policy_id = uuid4()

        session, events = generator.generate_session(
            member_id=member_id,
            policy_id=policy_id,
        )

        assert session.session_id is not None
        assert session.member_id == member_id
        assert session.policy_id == policy_id
        assert session.duration_seconds is not None
        assert session.duration_seconds > 0
        assert session.page_count >= 1
        assert len(events) >= 1
        assert session.is_authenticated is True

    def test_generate_trigger_session(self, generator):
        """Test trigger-based session generation."""
        claim_id = uuid4()
        member_id = uuid4()
        policy_id = uuid4()

        session, events = generator.generate_session(
            member_id=member_id,
            policy_id=policy_id,
            trigger_event_type=TriggerEventType.CLAIM_REJECTED,
            trigger_event_id=claim_id,
        )

        assert session.trigger_event_type == TriggerEventType.CLAIM_REJECTED
        assert session.trigger_event_id == claim_id
        # Should start on claims page for claim_rejected trigger
        assert session.viewed_claims_page is True

    def test_trigger_billing_session(self, generator):
        """Test billing trigger starts on billing page."""
        invoice_id = uuid4()

        session, events = generator.generate_session(
            member_id=uuid4(),
            policy_id=uuid4(),
            trigger_event_type=TriggerEventType.PAYMENT_FAILED,
            trigger_event_id=invoice_id,
        )

        assert session.trigger_event_type == TriggerEventType.PAYMENT_FAILED
        # Should start on billing page
        assert session.viewed_billing_page is True

    def test_session_has_page_view_events(self, generator):
        """Test that session contains page view events."""
        session, events = generator.generate_session(
            member_id=uuid4(),
            policy_id=uuid4(),
        )

        # Should have at least one page view
        page_views = [e for e in events if e.event_type == DigitalEventType.PAGE_VIEW]
        assert len(page_views) >= 1

        # Each page view should have a path and category
        for pv in page_views:
            assert pv.page_path is not None
            assert pv.page_category is not None

    def test_event_types_generated(self, test_rng, test_reference, sim_env):
        """Test that various event types are generated over multiple sessions."""
        id_generator = IDGenerator(test_rng, prefix_year=2024)
        generator = DigitalBehaviorGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

        event_types = set()
        for _ in range(50):
            session, events = generator.generate_session(
                member_id=uuid4(),
                policy_id=uuid4(),
            )
            for event in events:
                event_types.add(event.event_type)

        # Should have page views at minimum
        assert DigitalEventType.PAGE_VIEW in event_types

    def test_device_type_sampling(self, generator):
        """Test device type distribution."""
        device_types = []
        for _ in range(100):
            session, _ = generator.generate_session(
                member_id=uuid4(),
                policy_id=uuid4(),
            )
            device_types.append(session.device_type)

        # Should have multiple device types
        unique_devices = set(device_types)
        assert len(unique_devices) >= 2

        # Mobile should be most common (55% in config)
        mobile_count = device_types.count(DeviceType.MOBILE)
        assert mobile_count > 30  # Should be roughly 55%

    def test_session_duration_range(self, generator):
        """Test session duration is within expected range."""
        for _ in range(50):
            session, _ = generator.generate_session(
                member_id=uuid4(),
                policy_id=uuid4(),
            )
            # Duration should be between 10s and 1 hour
            assert 10 <= session.duration_seconds <= 3600

    def test_page_count_positive(self, generator):
        """Test page count is always at least 1."""
        for _ in range(50):
            session, _ = generator.generate_session(
                member_id=uuid4(),
                policy_id=uuid4(),
            )
            assert session.page_count >= 1

    def test_intent_signal_cancel_page(self, test_rng, test_reference, sim_env):
        """Test that cancel page views set intent signal."""
        id_generator = IDGenerator(test_rng, prefix_year=2024)
        generator = DigitalBehaviorGenerator(
            rng=test_rng,
            reference=test_reference,
            id_generator=id_generator,
            sim_env=sim_env,
        )

        # Generate many sessions to get one with cancel page
        cancel_views = 0
        for _ in range(200):
            session, events = generator.generate_session(
                member_id=uuid4(),
                policy_id=uuid4(),
            )
            if session.viewed_cancel_page:
                cancel_views += 1
                # If cancel flag is set, should have visited cancel category
                cancel_events = [
                    e for e in events
                    if e.page_category == PageCategory.CANCEL
                ]
                assert len(cancel_events) >= 1

        # Should have some cancel page views (3% of page visits in config)
        # With 200 sessions averaging 4.5 pages each, expect ~27 cancel views
        assert cancel_views > 0

    def test_session_type_matches_device(self, generator):
        """Test session type aligns with device type."""
        for _ in range(50):
            session, _ = generator.generate_session(
                member_id=uuid4(),
                policy_id=uuid4(),
            )
            if session.device_type == DeviceType.DESKTOP:
                assert session.session_type == SessionType.WEB
            else:
                assert session.session_type == SessionType.APP

    def test_events_belong_to_session(self, generator):
        """Test all events have correct session_id."""
        session, events = generator.generate_session(
            member_id=uuid4(),
            policy_id=uuid4(),
        )

        for event in events:
            assert event.session_id == session.session_id

    def test_events_have_member_id(self, generator):
        """Test all events have member_id."""
        member_id = uuid4()
        session, events = generator.generate_session(
            member_id=member_id,
            policy_id=uuid4(),
        )

        for event in events:
            assert event.member_id == member_id

    def test_event_sequence_ordering(self, generator):
        """Test events have sequential ordering."""
        session, events = generator.generate_session(
            member_id=uuid4(),
            policy_id=uuid4(),
        )

        if len(events) > 1:
            sequences = [e.event_sequence for e in events if e.event_sequence]
            # Sequences should be unique
            assert len(sequences) == len(set(sequences))

    def test_entry_exit_pages_set(self, generator):
        """Test entry and exit pages are set."""
        session, events = generator.generate_session(
            member_id=uuid4(),
            policy_id=uuid4(),
        )

        assert session.entry_page is not None
        assert session.exit_page is not None
        # Entry page should match first event
        if events:
            assert session.entry_page == events[0].page_path
