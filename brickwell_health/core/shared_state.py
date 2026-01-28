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
        }
