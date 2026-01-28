"""
Waiting period generator for Brickwell Health Simulator.

Generates waiting period records for policy members.
"""

from datetime import date, datetime
from typing import Any
from uuid import UUID

from brickwell_health.domain.coverage import CoverageCreate, WaitingPeriodCreate
from brickwell_health.domain.enums import (
    WaitingPeriodType,
    WaitingPeriodStatus,
    CoverageType,
)
from brickwell_health.domain.policy import PolicyMemberCreate
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.utils.time_conversion import add_months


class WaitingPeriodGenerator(BaseGenerator[WaitingPeriodCreate]):
    """
    Generates waiting period records.

    Waiting periods control when benefits become available:
    - General: 2 months
    - Pre-existing: 12 months
    - Obstetric: 12 months
    - Psychiatric: 2 months
    """

    # Standard waiting periods in months
    STANDARD_WAITING_PERIODS = {
        WaitingPeriodType.GENERAL: 2,
        WaitingPeriodType.PRE_EXISTING: 12,
        WaitingPeriodType.OBSTETRIC: 12,
        WaitingPeriodType.PSYCHIATRIC: 2,
    }

    def __init__(self, rng, reference, id_generator: IDGenerator):
        """
        Initialize the waiting period generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
        """
        super().__init__(rng, reference)
        self.id_generator = id_generator

    def generate(
        self,
        policy_member: PolicyMemberCreate,
        coverage: CoverageCreate,
        waiting_period_type: WaitingPeriodType,
        start_date: date,
        duration_months: int | None = None,
        is_transfer: bool = False,
        waiting_period_id: UUID | None = None,
        **kwargs: Any,
    ) -> WaitingPeriodCreate:
        """
        Generate a waiting period record.

        Args:
            policy_member: Policy member the waiting period applies to
            coverage: Coverage the waiting period is for
            waiting_period_type: Type of waiting period
            start_date: Start date
            duration_months: Duration (uses standard if not provided)
            is_transfer: If True, may reduce/waive waiting period
            waiting_period_id: Optional pre-generated UUID

        Returns:
            WaitingPeriodCreate instance
        """
        if waiting_period_id is None:
            waiting_period_id = self.id_generator.generate_uuid()

        # Determine duration
        if duration_months is None:
            duration_months = self.STANDARD_WAITING_PERIODS.get(
                waiting_period_type, 2
            )

        # Transfers may have reduced waiting periods
        if is_transfer:
            # Assume continuity of cover reduces waiting periods
            duration_months = 0
            status = WaitingPeriodStatus.WAIVED
            waiver_reason = "Transfer - continuity of cover"
        else:
            status = WaitingPeriodStatus.IN_PROGRESS
            waiver_reason = None

        end_date = add_months(start_date, duration_months)

        return WaitingPeriodCreate(
            waiting_period_id=waiting_period_id,
            policy_member_id=policy_member.policy_member_id,
            coverage_id=coverage.coverage_id,
            waiting_period_type=waiting_period_type,
            benefit_category_id=None,  # Applies to all
            clinical_category_id=None,
            start_date=start_date,
            end_date=end_date,
            duration_months=duration_months,
            status=status,
            waiver_reason=waiver_reason,
            exemption_granted=is_transfer,
            exemption_type="Transfer" if is_transfer else None,
            exemption_reason=waiver_reason,
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

    def generate_waiting_periods_for_member(
        self,
        policy_member: PolicyMemberCreate,
        coverages: list[CoverageCreate],
        start_date: date,
        is_transfer: bool = False,
    ) -> list[WaitingPeriodCreate]:
        """
        Generate all applicable waiting periods for a member.

        Args:
            policy_member: Policy member
            coverages: Member's coverages
            start_date: Start date
            is_transfer: Whether member is transferring

        Returns:
            List of WaitingPeriodCreate instances
        """
        waiting_periods = []

        for coverage in coverages:
            # Hospital coverage has all waiting period types
            if coverage.coverage_type == CoverageType.HOSPITAL:
                for wp_type in WaitingPeriodType:
                    wp = self.generate(
                        policy_member=policy_member,
                        coverage=coverage,
                        waiting_period_type=wp_type,
                        start_date=start_date,
                        is_transfer=is_transfer,
                    )
                    waiting_periods.append(wp)

            # Extras has only general waiting period
            elif coverage.coverage_type == CoverageType.EXTRAS:
                wp = self.generate(
                    policy_member=policy_member,
                    coverage=coverage,
                    waiting_period_type=WaitingPeriodType.GENERAL,
                    start_date=start_date,
                    is_transfer=is_transfer,
                )
                waiting_periods.append(wp)

            # Ambulance has no waiting period
            # (or very short one depending on fund)

        return waiting_periods

    def is_waiting_period_complete(
        self,
        waiting_period: WaitingPeriodCreate,
        as_of_date: date,
    ) -> bool:
        """
        Check if a waiting period is complete.

        Args:
            waiting_period: Waiting period to check
            as_of_date: Date to check against

        Returns:
            True if waiting period is complete
        """
        if waiting_period.status == WaitingPeriodStatus.WAIVED:
            return True
        if waiting_period.status == WaitingPeriodStatus.COMPLETED:
            return True
        return as_of_date >= waiting_period.end_date

    def complete_waiting_period(
        self,
        waiting_period: WaitingPeriodCreate,
    ) -> WaitingPeriodCreate:
        """
        Mark a waiting period as completed.

        Args:
            waiting_period: Waiting period to complete

        Returns:
            Updated WaitingPeriodCreate
        """
        waiting_period.status = WaitingPeriodStatus.COMPLETED
        return waiting_period
