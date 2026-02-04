"""
Shared state container for cross-process communication.

Holds data that needs to be shared between Acquisition, Claims, Billing,
and Lifecycle processes within a single worker.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID


@dataclass
class SharedState:
    """
    Container for state shared between simulation processes.

    All processes run in the same thread/process, so no locking needed.
    Each worker has its own SharedState instance.
    """

    # Active policies: policy_id -> policy data dict
    # Used by: Billing, Lifecycle, Claims
    active_policies: dict[UUID, dict[str, Any]] = field(default_factory=dict)

    # Policy members: policy_member_id -> member data dict
    # Used by: Claims
    policy_members: dict[UUID, dict[str, Any]] = field(default_factory=dict)

    # Waiting periods: policy_member_id -> list of waiting period dicts
    # Used by: Claims
    waiting_periods: dict[UUID, list[dict[str, Any]]] = field(default_factory=dict)

    # Pending invoices: invoice_id -> invoice data dict
    # Used by: Billing
    pending_invoices: dict[UUID, dict[str, Any]] = field(default_factory=dict)

    # Pending claims: claim_id -> claim transition data dict
    # Used by: Claims (for lifecycle state transitions)
    # Structure: {
    #     claim_id: {
    #         "status": "SUBMITTED" | "ASSESSED" | "APPROVED",
    #         "assessment_date": date,  # When to transition to ASSESSED
    #         "approval_date": date,    # When to transition to APPROVED/REJECTED
    #         "payment_date": date,     # When to transition to PAID
    #         "approved": bool,         # True=approve at approval_date, False=reject
    #         "denial_reason": DenialReason | None,  # For stochastic rejections
    #         "claim_line_ids": list[UUID],  # ALL claim lines for this claim
    #         "benefit_category_id": int,
    #         "benefit_amount": Decimal,
    #         "member_data": dict,
    #     }
    # }
    pending_claims: dict[UUID, dict[str, Any]] = field(default_factory=dict)

    # Member change events: list of pending events for policy/billing to process
    # Used by: MemberLifecycleProcess (producer), PolicyLifecycleProcess, BillingProcess (consumers)
    # Structure: {
    #     "member_id": UUID,
    #     "policy_id": UUID,
    #     "change_type": str,  # "DEATH", "ADDRESS_CHANGE", etc.
    #     "change_data": dict,  # Event-specific data
    # }
    member_change_events: list[dict[str, Any]] = field(default_factory=list)

    # =========================================================================
    # NBA/NPS Domain Fields
    # =========================================================================

    # CRM Event Queue for trigger-based events (FIFO)
    # Used by: Claims, Billing (producers), CRMProcess (consumer)
    # Structure: {
    #     "event_type": str,  # TriggerEventType value
    #     "timestamp": datetime,
    #     "policy_id": UUID,
    #     "member_id": UUID,
    #     # Event-specific fields...
    # }
    crm_event_queue: deque[dict[str, Any]] = field(default_factory=deque)

    # Track recent interactions for survey suppression and context
    # Used by: CRMProcess, SurveyProcess
    recent_interactions: dict[UUID, list[dict[str, Any]]] = field(default_factory=dict)

    # Track pending surveys to prevent duplicates
    # Key: (member_id, survey_type) -> survey_data
    pending_surveys: dict[tuple[UUID, str], dict[str, Any]] = field(default_factory=dict)

    # Communication preferences cache (opt-in/out)
    # Key: member_id -> {preference_type_channel: bool}
    communication_preferences: dict[UUID, dict[str, bool]] = field(default_factory=dict)

    # Digital engagement levels (assigned at member creation)
    # Key: member_id -> "high" | "medium" | "low"
    member_engagement_levels: dict[UUID, str] = field(default_factory=dict)

    # Communication Event Queue (populated by CRMProcess after interaction creation)
    # Used by: CRMProcess (producer), CommunicationProcess (consumer)
    # Structure: {
    #     "event_type": "interaction_completed",
    #     "interaction_id": UUID,
    #     "trigger_event_type": str,  # Original trigger (claim_paid, etc.)
    #     "policy_id": UUID,
    #     "member_id": UUID,
    #     "claim_id": UUID | None,
    #     "invoice_id": UUID | None,
    #     "timestamp": datetime,
    # }
    communication_event_queue: deque[dict[str, Any]] = field(default_factory=deque)

    # Pending campaign responses for delayed lifecycle tracking
    # Used by: CommunicationProcess (INSERT then UPDATE pattern)
    # Key: response_id -> response_data
    # Structure: {
    #     "campaign_id": UUID,
    #     "communication_id": UUID,
    #     "member_id": UUID,
    #     "policy_id": UUID,
    #     "sent_date": datetime,
    #     "predicted_open_date": datetime | None,
    #     "predicted_click_date": datetime | None,
    #     "will_convert": bool,
    #     "status": "pending_open" | "pending_click" | "pending_convert",
    # }
    pending_campaign_responses: dict[UUID, dict[str, Any]] = field(default_factory=dict)

    def add_policy(
        self,
        policy_id: UUID,
        policy_data: dict[str, Any],
    ) -> None:
        """
        Register a new policy in shared state.

        Called by AcquisitionProcess when a policy is approved.

        Args:
            policy_id: The policy UUID
            policy_data: Dictionary containing policy details:
                - policy: Policy object
                - members: List of Member objects
                - coverages: List of Coverage objects
                - tier: Hospital tier (Gold/Silver/Bronze/Basic)
                - product_id: Product ID
                - excess: Excess amount
                - status: Policy status (Active)
                - mandate: DirectDebitMandate object
                - lhc_loading: LHC loading percentage
                - age_discount: Age-based discount percentage
                - rebate_pct: PHI rebate percentage
        """
        self.active_policies[policy_id] = policy_data

    def add_policy_member(
        self,
        policy_member_id: UUID,
        member_data: dict[str, Any],
    ) -> None:
        """
        Register a policy member for claims processing.

        Called by AcquisitionProcess for each member on a policy.

        Args:
            policy_member_id: The policy_member UUID
            member_data: Dictionary containing:
                - policy: Policy object
                - member: Member object
                - policy_member_id: UUID
                - age: Member's age
                - gender: Member's gender
                - hospital_coverage: Hospital coverage object (if any)
                - extras_coverage: Extras coverage object (if any)
                - ambulance_coverage: Ambulance coverage object (if any)
        """
        self.policy_members[policy_member_id] = member_data

    def add_waiting_periods(
        self,
        policy_member_id: UUID,
        waiting_periods: list[dict[str, Any]],
    ) -> None:
        """
        Register waiting periods for a policy member.

        Args:
            policy_member_id: The policy_member UUID
            waiting_periods: List of waiting period dictionaries with:
                - coverage_type: Type of coverage (Hospital/Extras/Ambulance)
                - start_date: When waiting period started
                - end_date: When waiting period ends
                - waiting_period_type: Type (Initial/PreExisting/etc)
        """
        self.waiting_periods[policy_member_id] = waiting_periods

    def remove_policy(self, policy_id: UUID) -> None:
        """
        Remove a policy (on cancellation or lapse).

        Args:
            policy_id: The policy UUID to remove
        """
        self.active_policies.pop(policy_id, None)

    def remove_policy_members(self, policy_id: UUID) -> None:
        """
        Remove all members of a policy from claims tracking.

        Called when a policy is lapsed or cancelled.

        Args:
            policy_id: The policy UUID whose members should be removed
        """
        # Find and remove all policy_members for this policy
        members_to_remove = [
            pm_id for pm_id, data in self.policy_members.items()
            if data.get("policy") and data["policy"].policy_id == policy_id
        ]
        for pm_id in members_to_remove:
            self.policy_members.pop(pm_id, None)
            self.waiting_periods.pop(pm_id, None)

    def update_policy_status(self, policy_id: UUID, status: str) -> None:
        """
        Update policy status (Active/Suspended/Cancelled).

        Args:
            policy_id: The policy UUID
            status: New status
        """
        if policy_id in self.active_policies:
            self.active_policies[policy_id]["status"] = status

    def get_stats(self) -> dict[str, int]:
        """Return counts of tracked entities."""
        return {
            "active_policies": len(self.active_policies),
            "policy_members": len(self.policy_members),
            "members_with_waiting_periods": len(self.waiting_periods),
            "pending_invoices": len(self.pending_invoices),
            "pending_claims": len(self.pending_claims),
            "pending_member_change_events": len(self.member_change_events),
            "crm_event_queue": len(self.crm_event_queue),
            "communication_event_queue": len(self.communication_event_queue),
            "members_with_interactions": len(self.recent_interactions),
            "pending_surveys": len(self.pending_surveys),
            "members_with_preferences": len(self.communication_preferences),
            "members_with_engagement_level": len(self.member_engagement_levels),
            "pending_campaign_responses": len(self.pending_campaign_responses),
        }

    def add_member_change_event(
        self,
        member_id: UUID,
        policy_id: UUID,
        change_type: str,
        change_data: dict[str, Any],
    ) -> None:
        """
        Queue a member change event for other processes to react to.

        Called by MemberLifecycleProcess when a member change occurs.
        PolicyLifecycleProcess and BillingProcess consume these events.

        Args:
            member_id: The member UUID
            policy_id: The associated policy UUID
            change_type: Type of change (e.g., "DEATH", "ADDRESS_CHANGE")
            change_data: Event-specific data dictionary
        """
        self.member_change_events.append({
            "member_id": member_id,
            "policy_id": policy_id,
            "change_type": change_type,
            "change_data": change_data,
        })

    def get_member_change_events(self, change_type: str | None = None) -> list[dict[str, Any]]:
        """
        Get and clear pending member change events.

        Consumers call this to retrieve events. Events are removed from the queue
        after retrieval to prevent duplicate processing.

        Args:
            change_type: Optional filter for specific event types.
                         If None, returns and clears all events.

        Returns:
            List of event dictionaries
        """
        if change_type:
            # Filter and remove only matching events
            events = [e for e in self.member_change_events if e["change_type"] == change_type]
            self.member_change_events = [e for e in self.member_change_events if e["change_type"] != change_type]
        else:
            # Return and clear all events
            events = self.member_change_events
            self.member_change_events = []
        return events

    def update_member_data(self, member_id: UUID, updates: dict[str, Any]) -> None:
        """
        Update member data in the policy_members cache.

        Called by MemberLifecycleProcess to keep cached member data in sync
        with database updates.

        Note: Pydantic models may not allow setting all attributes. This method
        silently skips fields that can't be set (e.g., deceased_flag on MemberCreate).

        Args:
            member_id: The member UUID to update
            updates: Dictionary of field names to new values
        """
        for pm_id, data in self.policy_members.items():
            member = data.get("member")
            if member and member.member_id == member_id:
                for key, value in updates.items():
                    try:
                        if hasattr(member, key):
                            setattr(member, key, value)
                    except (ValueError, AttributeError):
                        # Pydantic model may not allow setting this field
                        # Store in the data dict instead for reference
                        data[f"_updated_{key}"] = value

    def remove_policy_member(self, policy_member_id: UUID) -> None:
        """
        Remove a single policy member from tracking.

        Called when a member is removed from a policy (e.g., death, dependent aging out).

        Args:
            policy_member_id: The policy_member UUID to remove
        """
        self.policy_members.pop(policy_member_id, None)
        self.waiting_periods.pop(policy_member_id, None)

    # =========================================================================
    # CRM Event Queue Methods
    # =========================================================================

    def add_crm_event(self, event: dict[str, Any]) -> None:
        """
        Add a CRM trigger event to the queue.

        Called by ClaimsProcess and BillingProcess when events occur
        that should trigger CRM actions (interactions, cases, surveys).

        Args:
            event: Event dictionary containing:
                - event_type: str (TriggerEventType value)
                - timestamp: datetime
                - policy_id: UUID
                - member_id: UUID
                - Additional event-specific fields
        """
        self.crm_event_queue.append(event)

    def get_crm_events(self, max_events: int | None = None) -> list[dict[str, Any]]:
        """
        Get and remove CRM events from the queue (FIFO).

        Called by CRMProcess to consume events.

        Args:
            max_events: Maximum number of events to return (None = all)

        Returns:
            List of events in FIFO order
        """
        events = []
        count = 0
        while self.crm_event_queue and (max_events is None or count < max_events):
            events.append(self.crm_event_queue.popleft())
            count += 1
        return events

    def peek_crm_events(self) -> list[dict[str, Any]]:
        """Peek at CRM events without removing them."""
        return list(self.crm_event_queue)

    # =========================================================================
    # Interaction Tracking Methods
    # =========================================================================

    def add_interaction(self, member_id: UUID, interaction_data: dict[str, Any]) -> None:
        """
        Track a recent interaction for a member.

        Used for survey suppression and context building.

        Args:
            member_id: The member UUID
            interaction_data: Interaction details including timestamp
        """
        if member_id not in self.recent_interactions:
            self.recent_interactions[member_id] = []
        self.recent_interactions[member_id].append(interaction_data)

        # Keep only last 10 interactions per member
        if len(self.recent_interactions[member_id]) > 10:
            self.recent_interactions[member_id] = self.recent_interactions[member_id][-10:]

    def get_recent_interactions(
        self, member_id: UUID, days: int = 30
    ) -> list[dict[str, Any]]:
        """
        Get recent interactions for a member within specified days.

        Args:
            member_id: The member UUID
            days: Number of days to look back

        Returns:
            List of interaction dictionaries
        """
        interactions = self.recent_interactions.get(member_id, [])
        cutoff = datetime.now() - timedelta(days=days)
        return [
            i for i in interactions
            if i.get("timestamp", datetime.min) > cutoff
        ]

    # =========================================================================
    # Survey Tracking Methods
    # =========================================================================

    def has_pending_survey(self, member_id: UUID, survey_type: str) -> bool:
        """
        Check if member has a pending survey of given type.

        Used to prevent duplicate surveys.

        Args:
            member_id: The member UUID
            survey_type: Type of survey (NPS, CSAT)

        Returns:
            True if pending survey exists
        """
        key = (member_id, survey_type)
        return key in self.pending_surveys

    def add_pending_survey(
        self, member_id: UUID, survey_type: str, survey_data: dict[str, Any]
    ) -> None:
        """
        Mark a survey as pending for a member.

        Args:
            member_id: The member UUID
            survey_type: Type of survey (NPS, CSAT)
            survey_data: Survey details
        """
        key = (member_id, survey_type)
        self.pending_surveys[key] = survey_data

    def remove_pending_survey(self, member_id: UUID, survey_type: str) -> None:
        """
        Remove a pending survey marker.

        Called when survey is completed or expired.

        Args:
            member_id: The member UUID
            survey_type: Type of survey
        """
        key = (member_id, survey_type)
        self.pending_surveys.pop(key, None)

    # =========================================================================
    # Communication Preferences Methods
    # =========================================================================

    def set_communication_preferences(
        self, member_id: UUID, preferences: dict[str, bool]
    ) -> None:
        """
        Set communication preferences for a member.

        Args:
            member_id: The member UUID
            preferences: Dict of preference keys to bool values
                e.g., {"transactional_email": True, "marketing_sms": False}
        """
        self.communication_preferences[member_id] = preferences

    def get_communication_preferences(self, member_id: UUID) -> dict[str, bool]:
        """
        Get communication preferences for a member.

        Returns default (all opted-in) if not set.

        Args:
            member_id: The member UUID

        Returns:
            Dict of preference keys to bool values
        """
        return self.communication_preferences.get(member_id, {
            "transactional_email": True,
            "transactional_sms": True,
            "marketing_email": True,
            "marketing_sms": True,
        })

    def is_opted_in(self, member_id: UUID, preference_type: str, channel: str) -> bool:
        """
        Check if member is opted in for a specific type and channel.

        Args:
            member_id: The member UUID
            preference_type: "transactional" or "marketing"
            channel: "email" or "sms"

        Returns:
            True if opted in (defaults to True if not set)
        """
        prefs = self.get_communication_preferences(member_id)
        key = f"{preference_type.lower()}_{channel.lower()}"
        return prefs.get(key, True)

    # =========================================================================
    # Digital Engagement Methods
    # =========================================================================

    def set_engagement_level(self, member_id: UUID, level: str) -> None:
        """
        Set digital engagement level for a member.

        Assigned at member creation based on distribution.

        Args:
            member_id: The member UUID
            level: "high", "medium", or "low"
        """
        self.member_engagement_levels[member_id] = level

    def get_engagement_level(self, member_id: UUID) -> str:
        """
        Get digital engagement level for a member.

        Args:
            member_id: The member UUID

        Returns:
            Engagement level (defaults to "medium")
        """
        return self.member_engagement_levels.get(member_id, "medium")

    # =========================================================================
    # Communication Event Queue Methods
    # =========================================================================

    def add_communication_event(self, event: dict[str, Any]) -> None:
        """
        Add a communication trigger event to the queue.

        Called by CRMProcess after creating an interaction to trigger
        transactional communications.

        Args:
            event: Event dictionary containing:
                - event_type: str ("interaction_completed")
                - interaction_id: UUID
                - trigger_event_type: str (original trigger like "claim_paid")
                - policy_id: UUID
                - member_id: UUID
                - claim_id: UUID | None
                - invoice_id: UUID | None
                - timestamp: datetime
        """
        self.communication_event_queue.append(event)

    def get_communication_events(
        self, max_events: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Get and remove communication events from the queue (FIFO).

        Called by CommunicationProcess to consume events.

        Args:
            max_events: Maximum number of events to return (None = all)

        Returns:
            List of events in FIFO order
        """
        events = []
        count = 0
        while self.communication_event_queue and (
            max_events is None or count < max_events
        ):
            events.append(self.communication_event_queue.popleft())
            count += 1
        return events

    def peek_communication_events(self) -> list[dict[str, Any]]:
        """Peek at communication events without removing them."""
        return list(self.communication_event_queue)

    # =========================================================================
    # Pending Campaign Response Methods
    # =========================================================================

    def add_pending_campaign_response(
        self, response_id: UUID, response_data: dict[str, Any]
    ) -> None:
        """
        Track a pending campaign response for delayed lifecycle processing.

        Called by CommunicationProcess after sending a campaign communication
        to schedule delayed open/click/conversion updates.

        Args:
            response_id: The campaign_response UUID
            response_data: Response details including predicted dates
        """
        self.pending_campaign_responses[response_id] = response_data

    def get_due_campaign_responses(
        self, current_datetime: datetime
    ) -> list[tuple[UUID, dict[str, Any]]]:
        """
        Get campaign responses that are due for lifecycle updates.

        Returns responses where the predicted date has passed.

        Args:
            current_datetime: Current simulation datetime

        Returns:
            List of (response_id, response_data) tuples for due responses
        """
        due_responses = []
        for response_id, data in list(self.pending_campaign_responses.items()):
            status = data.get("status", "")

            if status == "pending_open":
                predicted_date = data.get("predicted_open_date")
                if predicted_date and predicted_date <= current_datetime:
                    due_responses.append((response_id, data))

            elif status == "pending_click":
                predicted_date = data.get("predicted_click_date")
                if predicted_date and predicted_date <= current_datetime:
                    due_responses.append((response_id, data))

            elif status == "pending_convert":
                predicted_date = data.get("predicted_convert_date")
                if predicted_date and predicted_date <= current_datetime:
                    due_responses.append((response_id, data))

        return due_responses

    def update_pending_campaign_response(
        self, response_id: UUID, updates: dict[str, Any]
    ) -> None:
        """
        Update a pending campaign response status.

        Called to advance the response through lifecycle stages.

        Args:
            response_id: The campaign_response UUID
            updates: Fields to update
        """
        if response_id in self.pending_campaign_responses:
            self.pending_campaign_responses[response_id].update(updates)

    def remove_pending_campaign_response(self, response_id: UUID) -> None:
        """
        Remove a pending campaign response after lifecycle is complete.

        Args:
            response_id: The campaign_response UUID
        """
        self.pending_campaign_responses.pop(response_id, None)
