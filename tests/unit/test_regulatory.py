"""
Unit tests for regulatory calculations.
"""

from datetime import date
from decimal import Decimal

import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.config.regulatory import (
    LHCLoadingCalculator,
    AgeBasedDiscountCalculator,
    PHIRebateCalculator,
)


class TestLHCLoadingCalculator:
    """Tests for LHC loading calculations."""

    def test_no_loading_if_joined_before_31(self):
        """No LHC loading if joined PHI before age 31."""
        calc = LHCLoadingCalculator()

        result = calc.calculate_loading(
            date_of_birth=date(1990, 1, 1),
            as_of_date=date(2024, 1, 1),
            join_date=date(2020, 1, 1),  # Joined at age 30
        )

        assert result["loading_percentage"] == Decimal("0")
        assert result["eligible"] is True

    def test_loading_applied_if_joined_after_31(self):
        """LHC loading should be 2% per year without cover after 31."""
        calc = LHCLoadingCalculator()

        # Person born 1985, joins PHI at 40 (in 2025)
        # Base day would be July 1, 2016 (after 31st birthday)
        # 9 years without cover = 18% loading
        result = calc.calculate_loading(
            date_of_birth=date(1985, 1, 1),
            as_of_date=date(2025, 1, 1),
            join_date=date(2025, 1, 1),
        )

        # Loading should be significant
        assert result["loading_percentage"] > Decimal("0")
        assert result["years_without_cover"] > 0

    def test_exempt_if_born_before_1934(self):
        """People born before July 1, 1934 are exempt from LHC."""
        calc = LHCLoadingCalculator()

        result = calc.calculate_loading(
            date_of_birth=date(1933, 6, 30),
            as_of_date=date(2024, 1, 1),
            join_date=None,
        )

        assert result["is_exempt"] is True
        assert result["loading_percentage"] == Decimal("0")

    def test_max_loading_is_70_percent(self):
        """LHC loading should cap at 70%."""
        calc = LHCLoadingCalculator()

        # Person who waited 50 years after base day
        result = calc.calculate_loading(
            date_of_birth=date(1950, 1, 1),
            as_of_date=date(2024, 1, 1),
            join_date=None,  # Never had cover
        )

        assert result["loading_percentage"] == Decimal("70")

    def test_loading_removed_after_10_years(self):
        """LHC loading should be removed after 10 continuous years."""
        calc = LHCLoadingCalculator()

        # Joined with loading 11 years ago
        result = calc.calculate_loading(
            date_of_birth=date(1970, 1, 1),
            as_of_date=date(2024, 1, 1),
            join_date=date(2013, 1, 1),
            continuous_cover_start=date(2013, 1, 1),
        )

        assert result["loading_percentage"] == Decimal("0")


class TestAgeBasedDiscountCalculator:
    """Tests for age-based discount calculations."""

    def test_discount_for_18_year_old(self):
        """18-year-old should get maximum 10% discount."""
        calc = AgeBasedDiscountCalculator()

        result = calc.calculate_discount(age_at_join=18, current_age=18)

        # (30 - 18) * 2% = 24%, capped at 10%
        assert result["original_discount"] == Decimal("10")
        assert result["current_discount"] == Decimal("10")

    def test_no_discount_for_30_year_old(self):
        """30-year-old should not be eligible for discount."""
        calc = AgeBasedDiscountCalculator()

        result = calc.calculate_discount(age_at_join=30, current_age=30)

        assert result["eligible"] is False
        assert result["current_discount"] == Decimal("0")

    def test_discount_phases_out_after_41(self):
        """Discount should phase out from age 41 to 51."""
        calc = AgeBasedDiscountCalculator()

        # Joined at 25 (10% discount), now 45
        result = calc.calculate_discount(age_at_join=25, current_age=45)

        # 4 years into phase-out = 8% reduction
        expected = Decimal("10") - Decimal("8")
        assert result["current_discount"] == expected
        assert result["phase_out_status"] == "in_progress"

    def test_discount_fully_phased_out_at_51(self):
        """Discount should be zero at age 51."""
        calc = AgeBasedDiscountCalculator()

        result = calc.calculate_discount(age_at_join=25, current_age=51)

        assert result["current_discount"] == Decimal("0")
        assert result["phase_out_status"] == "complete"


class TestPHIRebateCalculator:
    """Tests for PHI rebate calculations."""

    def test_base_tier_rebate_under_65(self, sim_env: SimulationEnvironment):
        """Base tier under-65 should get ~24.6% rebate."""
        calc = PHIRebateCalculator(sim_env=sim_env)

        result = calc.calculate_rebate(
            income=50000,
            is_family=False,
            oldest_member_age=40,
        )

        assert result["tier"] == "Base"
        assert result["rebate_percentage"] == Decimal("24.608")

    def test_higher_rebate_for_over_70(self, sim_env: SimulationEnvironment):
        """Over-70 should get higher rebate in same tier."""
        calc = PHIRebateCalculator(sim_env=sim_env)

        result_under_65 = calc.calculate_rebate(
            income=50000,
            is_family=False,
            oldest_member_age=40,
        )

        result_over_70 = calc.calculate_rebate(
            income=50000,
            is_family=False,
            oldest_member_age=75,
        )

        assert result_over_70["rebate_percentage"] > result_under_65["rebate_percentage"]

    def test_tier_3_no_rebate(self, sim_env: SimulationEnvironment):
        """High income (Tier 3) should get no rebate."""
        calc = PHIRebateCalculator(sim_env=sim_env)

        result = calc.calculate_rebate(
            income=200000,  # Above Tier 3 threshold
            is_family=False,
            oldest_member_age=40,
        )

        assert result["tier"] == "Tier 3"
        assert result["rebate_percentage"] == Decimal("0")
        assert result["mls_liable"] is True

    def test_family_uses_family_thresholds(self, sim_env: SimulationEnvironment):
        """Family policies should use family income thresholds."""
        calc = PHIRebateCalculator(sim_env=sim_env)

        # Income that would be Tier 1 for single but Base for family
        result_single = calc.calculate_rebate(
            income=100000,
            is_family=False,
            oldest_member_age=40,
        )

        result_family = calc.calculate_rebate(
            income=100000,
            is_family=True,
            oldest_member_age=40,
        )

        # Family threshold is higher, so same income = better tier
        assert result_family["tier"] == "Base"
        assert result_single["tier"] in ["Tier 1", "Tier 2"]  # Higher tier = lower rebate
