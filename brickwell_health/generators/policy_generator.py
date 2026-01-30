"""
Policy generator for Brickwell Health Simulator.

Generates policies from approved applications.
"""

from datetime import date
from decimal import Decimal
from typing import Any, TYPE_CHECKING
from uuid import UUID

from brickwell_health.domain.application import ApplicationCreate, ApplicationMemberCreate
from brickwell_health.domain.enums import (
    PolicyStatus,
    PolicyType,
    MemberRole,
    RelationshipType,
    DistributionChannel,
)
from brickwell_health.domain.member import MemberCreate
from brickwell_health.domain.policy import PolicyCreate, PolicyMemberCreate
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.statistics.income_model import IncomeModel

if TYPE_CHECKING:
    from brickwell_health.core.environment import SimulationEnvironment


class PolicyGenerator(BaseGenerator[PolicyCreate]):
    """
    Generates policies from approved applications.
    """

    def __init__(
        self,
        rng,
        reference,
        id_generator: IDGenerator,
        sim_env: "SimulationEnvironment",
    ):
        """
        Initialize the policy generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
            sim_env: Simulation environment for time access
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.income_model = IncomeModel(rng)

    def generate(
        self,
        application: ApplicationCreate,
        members: list[MemberCreate],
        premium_amount: Decimal | None = None,
        policy_id: UUID | None = None,
        **kwargs: Any,
    ) -> tuple[PolicyCreate, list[PolicyMemberCreate]]:
        """
        Generate a policy from an approved application.

        Args:
            application: Approved application
            members: Member records (primary first)
            premium_amount: Optional premium (will be calculated if not provided)
            policy_id: Optional pre-generated UUID

        Returns:
            Tuple of (PolicyCreate, list[PolicyMemberCreate])
        """
        if policy_id is None:
            policy_id = self.id_generator.generate_uuid()

        # Calculate premium if not provided
        if premium_amount is None:
            premium_amount = self._calculate_premium(
                application.product_id,
                application.requested_policy_type,
                application.state,
                application.requested_excess,
            )

        # Determine rebate tier
        primary = members[0]
        is_family = application.requested_policy_type != PolicyType.SINGLE
        primary_age = self._calculate_age(primary.date_of_birth, application.requested_start_date)

        income = self.income_model.sample_income(primary_age, application.state, is_family)
        rebate_tier = self.income_model.get_rebate_tier(income, is_family)

        policy = PolicyCreate(
            policy_id=policy_id,
            policy_number=self.id_generator.generate_policy_number(),
            application_id=application.application_id,
            product_id=application.product_id,
            policy_status=PolicyStatus.ACTIVE,
            policy_type=application.requested_policy_type,
            effective_date=application.requested_start_date,
            end_date=None,
            cancellation_reason=None,
            payment_frequency="Monthly",
            premium_amount=premium_amount,
            excess_amount=application.requested_excess,
            government_rebate_tier=rebate_tier,
            rebate_claimed_as="ReducedPremium" if self.bernoulli(0.85) else "TaxReturn",
            distribution_channel=application.channel,
            state_of_residence=application.state,
            original_join_date=application.requested_start_date,
            previous_fund_code=application.previous_fund_code,
            transfer_certificate_date=application.requested_start_date if application.transfer_certificate_received else None,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

        # Generate policy members
        policy_members = []
        for i, member in enumerate(members):
            if i == 0:
                role = MemberRole.PRIMARY
                relationship = RelationshipType.SELF
            elif i == 1 and application.requested_policy_type in [PolicyType.COUPLE, PolicyType.FAMILY]:
                role = MemberRole.PARTNER
                relationship = RelationshipType.SPOUSE
            else:
                role = MemberRole.DEPENDENT
                relationship = RelationshipType.CHILD

            pm = PolicyMemberCreate(
                policy_member_id=self.id_generator.generate_uuid(),
                policy_id=policy_id,
                member_id=member.member_id,
                member_role=role,
                relationship_to_primary=relationship,
                effective_date=application.requested_start_date,
                end_date=None,
                is_active=True,
                created_at=self.get_current_datetime(),
                created_by="SIMULATION",
            )
            policy_members.append(pm)

        return policy, policy_members

    def _calculate_premium(
        self,
        product_id: int,
        policy_type: PolicyType,
        state: str,
        excess: Decimal | None,
    ) -> Decimal:
        """
        Calculate monthly premium.

        Args:
            product_id: Product ID
            policy_type: Policy type
            state: State code
            excess: Excess amount

        Returns:
            Monthly premium amount
        """
        # Try to get from reference data
        rates = self.reference.get_premium_rates(
            product_id=product_id,
            state=state,
        )

        if rates:
            # Find matching rate
            for rate in rates:
                if rate.get("policy_type") == policy_type.value:
                    return Decimal(str(rate.get("base_premium_monthly", 200)))

            # Fallback to first matching product
            return Decimal(str(rates[0].get("base_premium_monthly", 200)))

        # Default premium based on policy type
        base_premiums = {
            PolicyType.SINGLE: Decimal("180"),
            PolicyType.COUPLE: Decimal("350"),
            PolicyType.FAMILY: Decimal("450"),
            PolicyType.SINGLE_PARENT: Decimal("350"),
        }

        base = base_premiums.get(policy_type, Decimal("200"))

        # Adjust for excess
        if excess:
            excess_discount = min(Decimal("0.15"), excess / Decimal("10000"))
            base = base * (1 - excess_discount)

        return base.quantize(Decimal("0.01"))

    def _calculate_age(self, dob: date, as_of: date) -> int:
        """Calculate age in years."""
        age = as_of.year - dob.year
        if (as_of.month, as_of.day) < (dob.month, dob.day):
            age -= 1
        return max(0, age)

    def cancel_policy(
        self,
        policy: PolicyCreate,
        cancellation_date: date,
        reason: str,
    ) -> PolicyCreate:
        """
        Cancel a policy.

        Args:
            policy: Policy to cancel
            cancellation_date: Effective date of cancellation
            reason: Cancellation reason

        Returns:
            Updated PolicyCreate
        """
        policy.policy_status = PolicyStatus.CANCELLED
        policy.end_date = cancellation_date
        policy.cancellation_reason = reason
        return policy

    def suspend_policy(
        self,
        policy: PolicyCreate,
    ) -> PolicyCreate:
        """
        Suspend a policy.

        Args:
            policy: Policy to suspend

        Returns:
            Updated PolicyCreate
        """
        policy.policy_status = PolicyStatus.SUSPENDED
        return policy
