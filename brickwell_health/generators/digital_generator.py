"""
Digital Behavior Domain Generator for Brickwell Health Simulator.

Generator for Web Sessions and Digital Events.
"""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

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
from brickwell_health.generators.base import BaseGenerator

if TYPE_CHECKING:
    from brickwell_health.generators.id_generator import IDGenerator
    from brickwell_health.core.environment import SimulationEnvironment
    from brickwell_health.reference.loader import ReferenceDataLoader


class DigitalBehaviorGenerator(BaseGenerator[WebSessionCreate]):
    """
    Generator for web sessions and digital events.

    Generates browsing sessions with realistic page navigation patterns,
    engagement metrics, and intent signals for NBA analytics.
    """

    def __init__(
        self,
        rng,
        reference: "ReferenceDataLoader",
        id_generator: "IDGenerator",
        sim_env: "SimulationEnvironment",
        config: dict | None = None,
    ):
        """
        Initialize the digital behavior generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator for UUIDs
            sim_env: Simulation environment
            config: Optional digital config
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

        # Page paths by category
        self.page_paths = {
            PageCategory.HOME: ["/", "/home", "/dashboard"],
            PageCategory.DASHBOARD: ["/dashboard", "/my-account"],
            PageCategory.CLAIMS: [
                "/claims",
                "/claims/submit",
                "/claims/status",
                "/claims/history",
                "/claims/dispute",
            ],
            PageCategory.BILLING: [
                "/billing",
                "/billing/invoices",
                "/billing/payment",
                "/billing/history",
                "/billing/update-payment",
            ],
            PageCategory.PRODUCTS: [
                "/products",
                "/products/hospital",
                "/products/extras",
                "/products/compare",
            ],
            PageCategory.SUPPORT: ["/support", "/contact", "/faq", "/help"],
            PageCategory.FAQ: ["/faq", "/help/claims", "/help/billing"],
            PageCategory.ACCOUNT: [
                "/account",
                "/account/profile",
                "/account/preferences",
                "/account/password",
            ],
            PageCategory.CANCEL: [
                "/cancel",
                "/cancel/reasons",
                "/cancel/confirm",
                "/leaving-us",
            ],
            PageCategory.UPGRADE: [
                "/upgrade",
                "/upgrade/options",
                "/upgrade/compare",
                "/change-cover",
            ],
            PageCategory.COMPARE: [
                "/compare",
                "/compare/products",
                "/compare/funds",
            ],
        }

        # Browser distribution
        self.browsers = {
            "Chrome": 0.65,
            "Safari": 0.20,
            "Firefox": 0.08,
            "Edge": 0.05,
            "Other": 0.02,
        }

        # Operating systems by device
        self.os_by_device = {
            DeviceType.MOBILE: {"iOS": 0.55, "Android": 0.45},
            DeviceType.TABLET: {"iOS": 0.70, "Android": 0.30},
            DeviceType.DESKTOP: {"Windows": 0.60, "MacOS": 0.35, "Linux": 0.05},
        }

    def generate(self, **kwargs: Any) -> WebSessionCreate:
        """
        Generate a web session (default implementation).

        Use generate_session() for full session with events.
        """
        session, _ = self.generate_session(**kwargs)
        return session

    def generate_session(
        self,
        member_id: UUID,
        policy_id: UUID,
        trigger_event_type: Optional[TriggerEventType] = None,
        trigger_event_id: Optional[UUID] = None,
        engagement_level: str = "medium",
    ) -> tuple[WebSessionCreate, list[DigitalEventCreate]]:
        """
        Generate a web session with events.

        Args:
            member_id: Member ID (required for authenticated sessions)
            policy_id: Policy ID
            trigger_event_type: What triggered this session
            trigger_event_id: ID of trigger entity
            engagement_level: Member engagement level (high/medium/low)

        Returns:
            Tuple of (WebSessionCreate, list of DigitalEventCreate)
        """
        session_id = self.id_generator.generate_uuid()

        # Sample session characteristics
        device_type = self._sample_device_type()
        browser = self._sample_browser()
        operating_system = self._sample_os(device_type)
        session_type = (
            SessionType.WEB if device_type == DeviceType.DESKTOP else SessionType.APP
        )

        # Sample duration and page count
        duration_seconds = self._sample_duration()
        page_count = self._sample_page_count()

        # Session timing
        session_start = self.get_current_datetime()
        session_end = session_start + timedelta(seconds=duration_seconds)

        # Generate events and determine intent signals
        events, intent_signals = self._generate_events(
            session_id=session_id,
            member_id=member_id,
            session_start=session_start,
            duration_seconds=duration_seconds,
            page_count=page_count,
            trigger_event_type=trigger_event_type,
        )

        # Determine entry and exit pages
        entry_page = events[0].page_path if events else "/home"
        exit_page = events[-1].page_path if events else entry_page

        session = WebSessionCreate(
            session_id=session_id,
            member_id=member_id,
            policy_id=policy_id,
            session_start=session_start,
            session_end=session_end,
            duration_seconds=duration_seconds,
            page_count=page_count,
            event_count=len(events),
            device_type=device_type,
            browser=browser,
            operating_system=operating_system,
            entry_page=entry_page,
            exit_page=exit_page,
            referrer=self._sample_referrer(),
            is_authenticated=True,
            session_type=session_type,
            viewed_cancel_page=intent_signals.get("cancel", False),
            viewed_upgrade_page=intent_signals.get("upgrade", False),
            viewed_claims_page=intent_signals.get("claims", False),
            viewed_billing_page=intent_signals.get("billing", False),
            viewed_compare_page=intent_signals.get("compare", False),
            trigger_event_type=trigger_event_type,
            trigger_event_id=trigger_event_id,
        )

        return session, events

    def _sample_device_type(self) -> DeviceType:
        """Sample device type from config distribution."""
        distribution = self.config.get(
            "device_distribution", {"Mobile": 0.55, "Desktop": 0.38, "Tablet": 0.07}
        )
        devices = list(distribution.keys())
        probs = list(distribution.values())
        device_name = self.rng.choice(devices, p=probs)
        return DeviceType(device_name)

    def _sample_browser(self) -> str:
        """Sample browser."""
        browsers = list(self.browsers.keys())
        probs = list(self.browsers.values())
        return self.rng.choice(browsers, p=probs)

    def _sample_os(self, device_type: DeviceType) -> str:
        """Sample operating system based on device type."""
        os_dist = self.os_by_device.get(
            device_type, {"Windows": 0.60, "MacOS": 0.35, "Linux": 0.05}
        )
        os_names = list(os_dist.keys())
        probs = list(os_dist.values())
        return self.rng.choice(os_names, p=probs)

    def _sample_duration(self) -> int:
        """Sample session duration using lognormal distribution."""
        mu = self.config.get("duration_mu", 5.99)
        sigma = self.config.get("duration_sigma", 0.50)

        duration = self.rng.lognormal(mu, sigma)
        return int(min(max(duration, 10), 3600))  # 10s to 1 hour

    def _sample_page_count(self) -> int:
        """Sample number of pages viewed using negative binomial."""
        mean = self.config.get("pages_per_session_mean", 4.53)
        dispersion = self.config.get("pages_per_session_dispersion", 2.5)

        # Negative binomial parameterization
        p = dispersion / (dispersion + mean)
        count = self.rng.negative_binomial(dispersion, p)
        return max(1, int(count))

    def _sample_referrer(self) -> Optional[str]:
        """Sample referrer source."""
        referrers = {
            None: 0.40,  # Direct
            "https://www.google.com.au": 0.30,
            "https://www.bing.com": 0.05,
            "https://www.facebook.com": 0.10,
            "https://www.comparethemarket.com.au": 0.08,
            "https://www.iselect.com.au": 0.07,
        }

        refs = list(referrers.keys())
        probs = list(referrers.values())
        return self.rng.choice(refs, p=probs)

    def _generate_events(
        self,
        session_id: UUID,
        member_id: UUID,
        session_start: datetime,
        duration_seconds: int,
        page_count: int,
        trigger_event_type: Optional[TriggerEventType],
    ) -> tuple[list[DigitalEventCreate], dict[str, bool]]:
        """Generate events for a session."""
        events = []
        intent_signals = {
            "cancel": False,
            "upgrade": False,
            "claims": False,
            "billing": False,
            "compare": False,
        }

        # Determine starting category based on trigger
        if trigger_event_type:
            start_category = self._get_category_for_trigger(trigger_event_type)
        else:
            start_category = PageCategory.HOME

        current_time = session_start
        avg_time_per_page = duration_seconds / max(page_count, 1)

        categories_visited: list[PageCategory] = []

        for seq in range(1, page_count + 1):
            # Determine category for this page
            if seq == 1:
                category = start_category
            else:
                category = self._sample_next_category(categories_visited)

            categories_visited.append(category)

            # Track intent signals
            if category == PageCategory.CANCEL:
                intent_signals["cancel"] = True
            elif category == PageCategory.UPGRADE:
                intent_signals["upgrade"] = True
            elif category == PageCategory.CLAIMS:
                intent_signals["claims"] = True
            elif category == PageCategory.BILLING:
                intent_signals["billing"] = True
            elif category == PageCategory.COMPARE:
                intent_signals["compare"] = True

            # Sample page path
            page_paths = self.page_paths.get(category, ["/"])
            page_path = self.rng.choice(page_paths)

            # Create page view event
            time_on_page = int(self.rng.exponential(avg_time_per_page))
            time_on_page = max(5, min(time_on_page, 300))  # 5s to 5min

            page_view = DigitalEventCreate(
                event_id=self.id_generator.generate_uuid(),
                session_id=session_id,
                member_id=member_id,
                event_timestamp=current_time,
                event_type=DigitalEventType.PAGE_VIEW,
                page_path=page_path,
                page_category=category,
                page_title=self._get_page_title(page_path),
                event_sequence=len(events) + 1,
                time_on_page_seconds=time_on_page,
            )
            events.append(page_view)

            # Generate additional events on this page
            additional_events = self._generate_page_events(
                session_id=session_id,
                member_id=member_id,
                page_path=page_path,
                page_category=category,
                base_time=current_time,
                sequence_start=len(events) + 1,
            )
            events.extend(additional_events)

            current_time += timedelta(seconds=time_on_page)

        return events, intent_signals

    def _get_category_for_trigger(
        self, trigger_event_type: TriggerEventType
    ) -> PageCategory:
        """Get starting category based on trigger event."""
        mapping = {
            TriggerEventType.CLAIM_SUBMITTED: PageCategory.CLAIMS,
            TriggerEventType.CLAIM_REJECTED: PageCategory.CLAIMS,
            TriggerEventType.CLAIM_PAID: PageCategory.CLAIMS,
            TriggerEventType.INVOICE_ISSUED: PageCategory.BILLING,
            TriggerEventType.PAYMENT_FAILED: PageCategory.BILLING,
            TriggerEventType.ARREARS_CREATED: PageCategory.BILLING,
            TriggerEventType.RENEWAL_REMINDER: PageCategory.PRODUCTS,
        }
        return mapping.get(trigger_event_type, PageCategory.DASHBOARD)

    def _sample_next_category(
        self, visited: list[PageCategory]
    ) -> PageCategory:
        """Sample next category based on navigation patterns."""
        distribution = self.config.get(
            "page_category_distribution",
            {
                "Home": 0.20,
                "Claims": 0.25,
                "Billing": 0.20,
                "Products": 0.12,
                "Support": 0.10,
                "Account": 0.08,
                "Cancel": 0.03,
                "Upgrade": 0.02,
            },
        )

        # Build weights, reducing probability for recently visited
        weights = {}
        for cat_name, prob in distribution.items():
            try:
                cat = PageCategory(cat_name)
            except ValueError:
                cat = PageCategory.HOME

            if cat in visited[-2:]:  # Reduce if visited in last 2 pages
                weights[cat] = prob * 0.3
            else:
                weights[cat] = prob

        # Normalize
        total = sum(weights.values())
        categories = list(weights.keys())
        probs = [w / total for w in weights.values()]

        idx = self.rng.choice(len(categories), p=probs)
        return categories[idx]

    def _get_page_title(self, page_path: str) -> str:
        """Get page title from path."""
        titles = {
            "/": "Home | Brickwell Health",
            "/home": "Home | Brickwell Health",
            "/dashboard": "My Dashboard | Brickwell Health",
            "/claims": "Claims | Brickwell Health",
            "/claims/submit": "Submit a Claim | Brickwell Health",
            "/claims/status": "Claim Status | Brickwell Health",
            "/claims/history": "Claims History | Brickwell Health",
            "/claims/dispute": "Dispute a Claim | Brickwell Health",
            "/billing": "Billing | Brickwell Health",
            "/billing/invoices": "Invoices | Brickwell Health",
            "/billing/payment": "Make a Payment | Brickwell Health",
            "/billing/history": "Payment History | Brickwell Health",
            "/billing/update-payment": "Update Payment Method | Brickwell Health",
            "/products": "Our Products | Brickwell Health",
            "/products/compare": "Compare Products | Brickwell Health",
            "/cancel": "Cancel Membership | Brickwell Health",
            "/cancel/reasons": "Cancellation Reasons | Brickwell Health",
            "/cancel/confirm": "Confirm Cancellation | Brickwell Health",
            "/upgrade": "Upgrade Your Cover | Brickwell Health",
            "/upgrade/options": "Upgrade Options | Brickwell Health",
            "/compare": "Compare Health Funds | Brickwell Health",
            "/support": "Support | Brickwell Health",
            "/contact": "Contact Us | Brickwell Health",
            "/faq": "FAQ | Brickwell Health",
            "/account": "My Account | Brickwell Health",
            "/account/profile": "Profile | Brickwell Health",
        }
        return titles.get(page_path, f"{page_path} | Brickwell Health")

    def _generate_page_events(
        self,
        session_id: UUID,
        member_id: UUID,
        page_path: str,
        page_category: PageCategory,
        base_time: datetime,
        sequence_start: int,
    ) -> list[DigitalEventCreate]:
        """Generate additional events on a page (clicks, searches, forms)."""
        events = []
        sequence = sequence_start

        # Probability of events by page category
        search_prob = (
            0.1
            if page_category
            in [PageCategory.SUPPORT, PageCategory.FAQ, PageCategory.PRODUCTS]
            else 0.05
        )
        form_start_prob = (
            0.15
            if page_category in [PageCategory.CLAIMS, PageCategory.ACCOUNT]
            else 0.05
        )

        # Clicks (Poisson number of clicks)
        num_clicks = self.rng.poisson(2)
        for _ in range(num_clicks):
            if self.rng.random() < 0.5:  # 50% chance per potential click
                click_event = DigitalEventCreate(
                    event_id=self.id_generator.generate_uuid(),
                    session_id=session_id,
                    member_id=member_id,
                    event_timestamp=base_time + timedelta(seconds=int(self.rng.integers(1, 30))),
                    event_type=DigitalEventType.CLICK,
                    page_path=page_path,
                    page_category=page_category,
                    element_id=f"btn-{self.rng.integers(1, 100)}",
                    element_text=self._sample_button_text(page_category),
                    event_sequence=sequence,
                )
                events.append(click_event)
                sequence += 1

        # Search
        if self.rng.random() < search_prob:
            search_event = DigitalEventCreate(
                event_id=self.id_generator.generate_uuid(),
                session_id=session_id,
                member_id=member_id,
                event_timestamp=base_time + timedelta(seconds=int(self.rng.integers(5, 60))),
                event_type=DigitalEventType.SEARCH,
                page_path=page_path,
                page_category=page_category,
                search_query=self._sample_search_query(page_category),
                search_results_count=int(self.rng.integers(0, 20)),
                event_sequence=sequence,
            )
            events.append(search_event)
            sequence += 1

        # Form
        if self.rng.random() < form_start_prob:
            form_completed = self.rng.random() < 0.6  # 60% completion rate

            form_event = DigitalEventCreate(
                event_id=self.id_generator.generate_uuid(),
                session_id=session_id,
                member_id=member_id,
                event_timestamp=base_time + timedelta(seconds=int(self.rng.integers(10, 120))),
                event_type=(
                    DigitalEventType.FORM_SUBMIT
                    if form_completed
                    else DigitalEventType.FORM_START
                ),
                page_path=page_path,
                page_category=page_category,
                form_name=self._sample_form_name(page_category),
                form_completed=form_completed,
                event_sequence=sequence,
            )
            events.append(form_event)
            sequence += 1

        return events

    def _sample_button_text(self, category: PageCategory) -> str:
        """Sample button text based on page category."""
        buttons = {
            PageCategory.CLAIMS: ["Submit Claim", "View Status", "Download Receipt"],
            PageCategory.BILLING: ["Pay Now", "View Invoice", "Update Payment Method"],
            PageCategory.PRODUCTS: ["Get Quote", "Compare", "View Benefits"],
            PageCategory.SUPPORT: ["Contact Us", "Start Chat", "Call Back"],
            PageCategory.CANCEL: [
                "Continue",
                "Speak to Us First",
                "I've Changed My Mind",
            ],
            PageCategory.UPGRADE: ["Upgrade Now", "Compare Options", "Get Quote"],
        }
        options = buttons.get(category, ["Click", "Submit", "Continue"])
        return self.rng.choice(options)

    def _sample_search_query(self, category: PageCategory) -> str:
        """Sample search query based on page category."""
        queries = {
            PageCategory.SUPPORT: [
                "claim rejected",
                "how to claim",
                "waiting period",
                "contact",
            ],
            PageCategory.FAQ: [
                "dental cover",
                "hospital excess",
                "overseas cover",
                "pregnancy",
            ],
            PageCategory.PRODUCTS: [
                "gold hospital",
                "extras cover",
                "family policy",
                "no gap",
            ],
        }
        options = queries.get(category, ["help", "question", "how to"])
        return self.rng.choice(options)

    def _sample_form_name(self, category: PageCategory) -> str:
        """Sample form name based on page category."""
        forms = {
            PageCategory.CLAIMS: "claim_submission",
            PageCategory.ACCOUNT: "profile_update",
            PageCategory.BILLING: "payment_form",
            PageCategory.CANCEL: "cancellation_form",
            PageCategory.SUPPORT: "contact_form",
        }
        return forms.get(category, "form")
