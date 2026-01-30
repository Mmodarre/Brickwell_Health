"""
Shared state container for cross-process communication.

Holds data that needs to be shared between Acquisition, Claims, Billing,
and Lifecycle processes within a single worker.
"""

from dataclasses import dataclass, field
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
