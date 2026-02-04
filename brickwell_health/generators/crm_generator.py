"""
CRM Domain Generators for Brickwell Health Simulator.

Generators for Interaction, Case, and Complaint entities.
"""

from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Any, Optional, TYPE_CHECKING
from uuid import UUID

from brickwell_health.domain.crm import (
    InteractionCreate,
    CaseCreate,
    ComplaintCreate,
)
from brickwell_health.domain.enums import (
    CasePriority,
    CaseStatus,
    ComplaintSeverity,
    ComplaintSource,
    ComplaintStatus,
    InteractionChannel,
    InteractionDirection,
    TriggerEventType,
)
from brickwell_health.generators.base import BaseGenerator

if TYPE_CHECKING:
    from brickwell_health.generators.id_generator import IDGenerator
    from brickwell_health.core.environment import SimulationEnvironment
    from brickwell_health.reference.loader import ReferenceDataLoader


class InteractionGenerator(BaseGenerator[InteractionCreate]):
    """
    Generator for interaction records.

    Generates member interactions across all channels (phone, email, chat, etc.)
    with appropriate duration, wait time, and FCR (First Contact Resolution) rates.
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
        Initialize the interaction generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator for UUIDs and reference numbers
            sim_env: Simulation environment
            config: Optional interaction config
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

        # Load interaction types from reference data
        try:
            self.interaction_types = {
                t["type_code"]: t for t in reference.get_interaction_types()
            }
        except (FileNotFoundError, KeyError):
            self.interaction_types = {}

        try:
            self.interaction_outcomes = {
                o["outcome_code"]: o for o in reference.get_interaction_outcomes()
            }
        except (FileNotFoundError, KeyError):
            self.interaction_outcomes = {}

    def generate(
        self,
        policy_id: UUID,
        member_id: UUID,
        interaction_type_code: str,
        trigger_event_type: Optional[TriggerEventType] = None,
        trigger_event_id: Optional[UUID] = None,
        claim_id: Optional[UUID] = None,
        invoice_id: Optional[UUID] = None,
        channel: Optional[InteractionChannel] = None,
        **kwargs: Any,
    ) -> InteractionCreate:
        """
        Generate an interaction record.

        Args:
            policy_id: Policy ID
            member_id: Member ID
            interaction_type_code: Type of interaction (from reference data)
            trigger_event_type: What triggered this interaction
            trigger_event_id: ID of triggering entity
            claim_id: Related claim ID (if any)
            invoice_id: Related invoice ID (if any)
            channel: Force specific channel (otherwise sampled)

        Returns:
            InteractionCreate instance
        """
        interaction_id = self.id_generator.generate_uuid()
        interaction_reference = self.id_generator.generate_interaction_reference()

        # Get interaction type details
        interaction_type = self.interaction_types.get(interaction_type_code, {})
        interaction_type_id = interaction_type.get("type_id", 1)
        duration_type = interaction_type.get("typical_duration_type", "standard")

        # Sample channel if not provided
        if channel is None:
            channel = self._sample_channel(trigger_event_type)

        # Sample duration based on type
        duration_seconds = self._sample_duration(duration_type)

        # Calculate timing
        start_datetime = self.get_current_datetime()
        end_datetime = (
            start_datetime + timedelta(seconds=duration_seconds)
            if duration_seconds
            else None
        )

        # Sample wait time
        wait_time_seconds = self._sample_wait_time(channel)

        # Determine FCR based on interaction type
        fcr = self._sample_fcr(duration_type)

        # Sample outcome
        outcome_id = self._sample_outcome(
            fcr, interaction_type.get("requires_case", False)
        )

        # Generate handler
        handled_by = f"AGENT-{self.rng.integers(100, 999)}"
        queue_name = self._get_queue_name(
            interaction_type.get("type_category", "General")
        )

        # Generate subject
        subject = self._generate_subject(interaction_type_code, trigger_event_type)

        return InteractionCreate(
            interaction_id=interaction_id,
            interaction_reference=interaction_reference,
            policy_id=policy_id,
            member_id=member_id,
            interaction_type_id=interaction_type_id,
            channel=channel,
            direction=InteractionDirection.INBOUND,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            duration_seconds=duration_seconds,
            subject=subject,
            summary=None,  # Populated by LLM in deferred processing
            outcome_id=outcome_id,
            handled_by=handled_by,
            queue_name=queue_name,
            wait_time_seconds=wait_time_seconds,
            first_contact_resolution=fcr,
            satisfaction_score=None,  # May be collected via CSAT
            trigger_event_type=trigger_event_type,
            trigger_event_id=trigger_event_id,
            case_id=None,  # Set later if case created
            claim_id=claim_id,
            invoice_id=invoice_id,
        )

    def _sample_channel(
        self, trigger_event_type: Optional[TriggerEventType]
    ) -> InteractionChannel:
        """Sample channel based on trigger event type."""
        # Trigger-specific channel distributions
        if trigger_event_type == TriggerEventType.CLAIM_REJECTED:
            weights = {"Phone": 0.70, "Email": 0.20, "Chat": 0.10}
        elif trigger_event_type == TriggerEventType.PAYMENT_FAILED:
            weights = {"Phone": 0.55, "Email": 0.30, "Chat": 0.15}
        elif trigger_event_type == TriggerEventType.POLICY_SUSPENDED:
            weights = {"Phone": 0.75, "Email": 0.15, "Chat": 0.10}
        else:
            # Default distribution from config
            weights = self.config.get(
                "channel_distribution",
                {"Phone": 0.40, "Email": 0.30, "Chat": 0.20, "Branch": 0.10},
            )

        channels = list(weights.keys())
        probs = [weights[c] for c in channels]

        # Normalize probabilities
        total = sum(probs)
        probs = [p / total for p in probs]

        channel_name = self.rng.choice(channels, p=probs)
        return InteractionChannel(channel_name)

    def _sample_duration(self, duration_type: str) -> int:
        """Sample call duration using lognormal distribution."""
        params = self.config.get("duration_params", {})

        # Get params for duration type, with defaults
        type_params = params.get(duration_type, {})
        mu = type_params.get("mu", 5.48) if isinstance(type_params, dict) else 5.48
        sigma = type_params.get("sigma", 0.45) if isinstance(type_params, dict) else 0.45

        duration = self.rng.lognormal(mu, sigma)
        return int(min(max(duration, 30), 3600))  # 30s to 1 hour

    def _sample_wait_time(self, channel: InteractionChannel) -> Optional[int]:
        """Sample wait time before call answered."""
        if channel not in [InteractionChannel.PHONE, InteractionChannel.CHAT]:
            return None

        mu = self.config.get("wait_time_mu", 3.5)
        sigma = self.config.get("wait_time_sigma", 0.8)

        wait_time = self.rng.lognormal(mu, sigma)
        return int(min(max(wait_time, 5), 1800))  # 5s to 30 minutes

    def _sample_fcr(self, duration_type: str) -> bool:
        """Sample whether interaction achieved First Contact Resolution."""
        fcr_rates = self.config.get(
            "fcr_rates",
            {"simple": 0.85, "standard": 0.72, "complex": 0.35, "dispute": 0.40},
        )
        rate = fcr_rates.get(duration_type, 0.72)
        return self.rng.random() < rate

    def _sample_outcome(self, fcr: bool, requires_case: bool) -> int:
        """Sample interaction outcome."""
        if fcr:
            return 1  # Resolved
        elif requires_case:
            return 3  # Case Created
        else:
            return int(self.rng.choice([2, 4, 5, 6]))  # Info/Escalated/Callback/Transferred

    def _get_queue_name(self, category: str) -> str:
        """Get queue name based on interaction category."""
        queues = {
            "Claims": "Claims",
            "Billing": "Billing",
            "Policy": "Membership",
            "Coverage": "Products",
            "General": "General",
            "Complaint": "Escalations",
            "Retention": "Retention",
        }
        return queues.get(category, "General")

    def _generate_subject(
        self,
        interaction_type_code: str,
        trigger_event_type: Optional[TriggerEventType],
    ) -> str:
        """Generate a subject line for the interaction."""
        subjects = {
            "CLAIM_STATUS": "Claim status inquiry",
            "CLAIM_DISPUTE": "Dispute regarding claim decision",
            "BILLING_INQUIRY": "Billing inquiry",
            "BILLING_DISPUTE": "Dispute regarding invoice",
            "PAYMENT_ARRANGEMENT": "Request for payment arrangement",
            "POLICY_INFO": "Policy information request",
            "COVER_INQUIRY": "Coverage inquiry",
            "GENERAL_INQUIRY": "General inquiry",
            "COMPLAINT": "Formal complaint",
            "BENEFIT_INQUIRY": "Benefits inquiry",
            "MEMBERSHIP_CARD": "Membership card request",
        }
        return subjects.get(interaction_type_code, "Member inquiry")


class CaseGenerator(BaseGenerator[CaseCreate]):
    """
    Generator for service case records.

    Generates cases with appropriate priority, SLA, and assignment.
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
        Initialize the case generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
            sim_env: Simulation environment
            config: Optional case config
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

        # Load case types from reference data
        try:
            self.case_types = {
                t["type_code"]: t for t in reference.get_case_types()
            }
        except (FileNotFoundError, KeyError):
            self.case_types = {}

    def generate(
        self,
        policy_id: UUID,
        member_id: UUID,
        case_type_code: str,
        source_interaction_id: Optional[UUID] = None,
        related_claim_id: Optional[UUID] = None,
        related_invoice_id: Optional[UUID] = None,
        priority_override: Optional[CasePriority] = None,
        **kwargs: Any,
    ) -> CaseCreate:
        """
        Generate a service case record.

        Args:
            policy_id: Policy ID
            member_id: Member ID
            case_type_code: Type of case (from reference data)
            source_interaction_id: Interaction that created this case
            related_claim_id: Related claim ID (if any)
            related_invoice_id: Related invoice ID (if any)
            priority_override: Force specific priority

        Returns:
            CaseCreate instance
        """
        case_id = self.id_generator.generate_uuid()
        case_number = self.id_generator.generate_case_number()

        # Get case type details
        case_type = self.case_types.get(case_type_code, {})
        case_type_id = case_type.get("type_id", 20)  # Default to General
        sla_hours = case_type.get("sla_hours", 72)

        # Determine priority
        if priority_override:
            priority = priority_override
        else:
            default_priority = case_type.get("default_priority", "Medium")
            priority = CasePriority(default_priority)

        # Calculate due date based on SLA
        created_at = self.get_current_datetime()
        due_date = (created_at + timedelta(hours=sla_hours)).date()

        # Generate subject
        subject = self._generate_subject(
            case_type_code, case_type.get("type_name", "General Case")
        )

        # Assign to team
        assigned_team = case_type.get("type_category", "General")

        return CaseCreate(
            case_id=case_id,
            case_number=case_number,
            case_type_id=case_type_id,
            policy_id=policy_id,
            member_id=member_id,
            subject=subject,
            description=None,  # Can be set later
            priority=priority,
            status=CaseStatus.OPEN,
            assigned_to=None,  # Assigned during processing
            assigned_team=assigned_team,
            source_interaction_id=source_interaction_id,
            related_claim_id=related_claim_id,
            related_invoice_id=related_invoice_id,
            due_date=due_date,
            resolution_date=None,
            resolution_summary=None,
            sla_breached=False,
            note_count=0,
            task_count=0,
            created_at=created_at,
        )

    def _generate_subject(self, case_type_code: str, type_name: str) -> str:
        """Generate a subject line for the case."""
        return f"{type_name} - {case_type_code}"


class ComplaintGenerator(BaseGenerator[ComplaintCreate]):
    """
    Generator for complaint records.

    Generates complaints with appropriate severity, category, and SLA.
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
        Initialize the complaint generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
            sim_env: Simulation environment
            config: Optional complaint config
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.config = config or {}

        # Load complaint categories from reference data
        try:
            self.complaint_categories = {
                c["category_code"]: c for c in reference.get_complaint_categories()
            }
        except (FileNotFoundError, KeyError):
            self.complaint_categories = {}

    def generate(
        self,
        policy_id: UUID,
        member_id: UUID,
        category_code: str,
        case_id: Optional[UUID] = None,
        source: Optional[ComplaintSource] = None,
        related_claim_id: Optional[UUID] = None,
        related_invoice_id: Optional[UUID] = None,
        charge_amount: Optional[Decimal] = None,
        **kwargs: Any,
    ) -> ComplaintCreate:
        """
        Generate a complaint record.

        Args:
            policy_id: Policy ID
            member_id: Member ID
            category_code: Complaint category (from reference data)
            case_id: Related case ID (if any)
            source: Source of complaint
            related_claim_id: Related claim ID (if any)
            related_invoice_id: Related invoice ID (if any)
            charge_amount: Charge amount for severity determination

        Returns:
            ComplaintCreate instance
        """
        complaint_id = self.id_generator.generate_uuid()
        complaint_number = self.id_generator.generate_complaint_number()

        # Get category details
        category = self.complaint_categories.get(category_code, {})
        category_id = category.get("category_id", 15)  # Default to Other
        sla_days = category.get("sla_days", 21)

        # Determine severity based on charge amount and category
        severity = self._determine_severity(category_code, charge_amount)

        # Sample source if not provided
        if source is None:
            source = self._sample_source()

        # Calculate dates
        received_date = self.get_current_date()
        due_date = received_date + timedelta(days=sla_days)

        # Generate subject
        subject = self._generate_subject(
            category.get("category_name", "General Complaint")
        )

        return ComplaintCreate(
            complaint_id=complaint_id,
            complaint_number=complaint_number,
            case_id=case_id,
            policy_id=policy_id,
            member_id=member_id,
            complaint_category_id=category_id,
            subject=subject,
            description=None,  # Populated by LLM in deferred processing
            severity=severity,
            status=ComplaintStatus.RECEIVED,
            source=source,
            received_date=received_date,
            acknowledged_date=None,
            due_date=due_date,
            assigned_to=None,
            resolution_date=None,
            resolution_summary=None,
            resolution_outcome=None,
            compensation_amount=None,
            phio_escalated=False,
            phio_reference=None,
            phio_escalation_date=None,
            phio_decision_outcome=None,
            internal_review_requested=False,
            internal_review_outcome=None,
            escalation_count=0,
            related_claim_id=related_claim_id,
            related_invoice_id=related_invoice_id,
        )

    def _determine_severity(
        self,
        category_code: str,
        charge_amount: Optional[Decimal],
    ) -> ComplaintSeverity:
        """Determine complaint severity."""
        # High-severity categories
        if category_code in ["PRIVACY_BREACH", "STAFF_CONDUCT"]:
            return ComplaintSeverity.HIGH

        # Amount-based severity
        if charge_amount:
            if charge_amount > Decimal("2000"):
                return ComplaintSeverity.HIGH
            elif charge_amount > Decimal("500"):
                return ComplaintSeverity.MEDIUM

        # Default sampling
        weights = [0.20, 0.50, 0.25, 0.05]  # Low, Medium, High, Critical
        severity_names = ["Low", "Medium", "High", "Critical"]
        severity_name = self.rng.choice(severity_names, p=weights)
        return ComplaintSeverity(severity_name)

    def _sample_source(self) -> ComplaintSource:
        """Sample complaint source."""
        weights = {
            "Phone": 0.50,
            "Email": 0.30,
            "Letter": 0.10,
            "PHIO": 0.05,
            "InApp": 0.05,
        }
        sources = list(weights.keys())
        probs = list(weights.values())
        source_name = self.rng.choice(sources, p=probs)
        return ComplaintSource(source_name)

    def _generate_subject(self, category_name: str) -> str:
        """Generate a subject line for the complaint."""
        return f"Formal Complaint - {category_name}"
