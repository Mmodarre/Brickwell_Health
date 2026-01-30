"""
Regulatory records generator for Brickwell Health Simulator.

Generates LHC loading, age-based discount, and PHI rebate entitlement records.
"""

from datetime import date
from decimal import Decimal
from typing import Any, TYPE_CHECKING
from uuid import UUID

from brickwell_health.domain.member import (
    MemberCreate,
    LHCLoadingCreate,
    AgeBasedDiscountCreate,
    PHIRebateEntitlementCreate,
)
from brickwell_health.domain.policy import PolicyCreate
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.config.regulatory import (
    LHCLoadingCalculator,
    AgeBasedDiscountCalculator,
    PHIRebateCalculator,
)
from brickwell_health.statistics.income_model import IncomeModel
from brickwell_health.utils.time_conversion import get_age, get_financial_year

if TYPE_CHECKING:
    from brickwell_health.core.environment import SimulationEnvironment


class RegulatoryGenerator(BaseGenerator):
    """
    Generates regulatory records for members.

    Creates:
    - LHC loading records
    - Age-based discount records
    - PHI rebate entitlement records
    """

    def __init__(
        self,
        rng,
        reference,
        id_generator: IDGenerator,
        sim_env: "SimulationEnvironment",
    ):
        """
        Initialize the regulatory generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
            sim_env: Simulation environment for time access
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.lhc_calc = LHCLoadingCalculator()
        self.age_discount_calc = AgeBasedDiscountCalculator()
        self.rebate_calc = PHIRebateCalculator(sim_env=sim_env)
        self.income_model = IncomeModel(rng)

    def generate(self, **kwargs) -> dict[str, Any]:
        """Not used - use specific generate methods instead."""
        raise NotImplementedError("Use generate_lhc_loading, generate_age_discount, etc.")

    def generate_lhc_loading(
        self,
        member: MemberCreate,
        policy: PolicyCreate,
        join_date: date,
        lhc_loading_id: UUID | None = None,
    ) -> LHCLoadingCreate | None:
        """
        Generate LHC loading record for a member.

        Args:
            member: Member
            policy: Policy
            join_date: Date member joined PHI
            lhc_loading_id: Optional pre-generated UUID

        Returns:
            LHCLoadingCreate or None if not applicable
        """
        if lhc_loading_id is None:
            lhc_loading_id = self.id_generator.generate_uuid()

        # Calculate LHC loading
        result = self.lhc_calc.calculate_loading(
            date_of_birth=member.date_of_birth,
            as_of_date=join_date,
            join_date=join_date,
            continuous_cover_start=join_date,
        )

        # If not eligible or exempt, no record needed
        if not result["eligible"] or result["is_exempt"]:
            return None

        # If no loading, still create record for tracking
        return LHCLoadingCreate(
            lhc_loading_id=lhc_loading_id,
            member_id=member.member_id,
            policy_id=policy.policy_id,
            certified_age_of_entry=result["certified_age_of_entry"] or get_age(
                member.date_of_birth, join_date
            ),
            base_day=result["base_day"],
            loading_percentage=result["loading_percentage"],
            loading_start_date=join_date,
            loading_removal_date=self._calculate_loading_removal_date(
                join_date, result["years_to_removal"]
            ),
            continuous_cover_start=join_date,
            years_without_cover=result["years_without_cover"],
            is_loading_active=result["loading_percentage"] > 0,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

    def generate_age_discount(
        self,
        member: MemberCreate,
        policy: PolicyCreate,
        join_date: date,
        age_discount_id: UUID | None = None,
    ) -> AgeBasedDiscountCreate | None:
        """
        Generate age-based discount record.

        Args:
            member: Member
            policy: Policy
            join_date: Date member joined
            age_discount_id: Optional pre-generated UUID

        Returns:
            AgeBasedDiscountCreate or None if not eligible
        """
        if age_discount_id is None:
            age_discount_id = self.id_generator.generate_uuid()

        age_at_join = get_age(member.date_of_birth, join_date)

        # Calculate discount
        result = self.age_discount_calc.calculate_discount(
            age_at_join=age_at_join,
            current_age=age_at_join,  # At join time
        )

        if not result["eligible"]:
            return None

        # Calculate phase-out dates
        phase_out_start, phase_out_end = self.age_discount_calc.get_phase_out_dates(
            member.date_of_birth
        )

        return AgeBasedDiscountCreate(
            age_discount_id=age_discount_id,
            member_id=member.member_id,
            policy_id=policy.policy_id,
            age_at_eligibility=age_at_join,
            discount_percentage=result["original_discount"],
            eligibility_date=join_date,
            phase_out_start_date=phase_out_start,
            phase_out_end_date=phase_out_end,
            current_discount_pct=result["current_discount"],
            is_active=result["current_discount"] > 0,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

    def generate_rebate_entitlement(
        self,
        policy: PolicyCreate,
        members: list[MemberCreate],
        effective_date: date,
        rebate_entitlement_id: UUID | None = None,
    ) -> PHIRebateEntitlementCreate:
        """
        Generate PHI rebate entitlement record.

        Args:
            policy: Policy
            members: All members on policy
            effective_date: Effective date
            rebate_entitlement_id: Optional pre-generated UUID

        Returns:
            PHIRebateEntitlementCreate
        """
        if rebate_entitlement_id is None:
            rebate_entitlement_id = self.id_generator.generate_uuid()

        # Determine oldest member
        oldest_age = max(
            get_age(m.date_of_birth, effective_date) for m in members
        )

        # Determine if family
        is_family = len(members) > 1

        # Get primary member for income sampling
        primary = members[0]
        primary_age = get_age(primary.date_of_birth, effective_date)
        income = self.income_model.sample_income(
            primary_age, policy.state_of_residence, is_family
        )

        # Calculate rebate
        result = self.rebate_calc.calculate_rebate(
            income=income,
            is_family=is_family,
            oldest_member_age=oldest_age,
        )

        financial_year = get_financial_year(effective_date)

        return PHIRebateEntitlementCreate(
            rebate_entitlement_id=rebate_entitlement_id,
            policy_id=policy.policy_id,
            financial_year=financial_year,
            income_tier=result["tier"],
            oldest_member_age_bracket=result["age_bracket"],
            rebate_percentage=result["rebate_percentage"] / 100,  # Convert to decimal
            income_declaration_date=effective_date,
            declared_income_range=self.income_model.sample_declared_income_range(income),
            single_or_family="Family" if is_family else "Single",
            mls_liable=result["mls_liable"],
            effective_date=effective_date,
            end_date=None,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

    def generate_all_regulatory_records(
        self,
        policy: PolicyCreate,
        members: list[MemberCreate],
        join_date: date,
    ) -> dict[str, list]:
        """
        Generate all regulatory records for a new policy.

        Args:
            policy: Policy
            members: All policy members
            join_date: Policy start date

        Returns:
            Dictionary with lists of:
                - lhc_loadings
                - age_discounts
                - rebate_entitlements
        """
        lhc_loadings = []
        age_discounts = []

        for member in members:
            # LHC loading (for adults only)
            age = get_age(member.date_of_birth, join_date)
            if age >= 18:
                lhc = self.generate_lhc_loading(member, policy, join_date)
                if lhc:
                    lhc_loadings.append(lhc)

                # Age-based discount
                age_discount = self.generate_age_discount(member, policy, join_date)
                if age_discount:
                    age_discounts.append(age_discount)

        # PHI rebate (one per policy)
        rebate = self.generate_rebate_entitlement(policy, members, join_date)

        return {
            "lhc_loadings": lhc_loadings,
            "age_discounts": age_discounts,
            "rebate_entitlements": [rebate],
        }

    def _calculate_loading_removal_date(
        self,
        start_date: date,
        years_to_removal: int | None,
    ) -> date | None:
        """Calculate when LHC loading will be removed."""
        if years_to_removal is None or years_to_removal <= 0:
            return None
        
        import calendar
        target_year = start_date.year + years_to_removal
        target_month = start_date.month
        # Cap day at max days in target month (handles Feb 29 -> Feb 28 for non-leap years)
        max_day = calendar.monthrange(target_year, target_month)[1]
        target_day = min(start_date.day, max_day)
        return date(target_year, target_month, target_day)
