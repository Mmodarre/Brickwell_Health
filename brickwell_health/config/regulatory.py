"""
Regulatory calculations for Australian Private Health Insurance.

Implements:
- Lifetime Health Cover (LHC) loading
- Age-based discount (youth discount)
- Private Health Insurance Rebate
- Medicare Levy Surcharge awareness
"""

from datetime import date
from decimal import Decimal
from typing import Any

from brickwell_health.utils.time_conversion import get_age, get_financial_year


class LHCLoadingCalculator:
    """
    Lifetime Health Cover (LHC) Loading calculator.

    LHC was introduced on 1 July 2000 to encourage people to take out
    hospital cover earlier in life and maintain it.

    Rules:
    - Base age: 31 (your "base day" is July 1 after you turn 31)
    - Loading: 2% for each year without hospital cover after base day
    - Maximum loading: 70%
    - Removal: After 10 continuous years of hospital cover
    - Exemptions: People born before 1 July 1934

    Usage:
        calc = LHCLoadingCalculator()
        loading = calc.calculate_loading(date_of_birth, current_date, join_date=None)
    """

    BASE_AGE = 31
    LOADING_PER_YEAR = Decimal("2")  # 2% per year
    MAX_LOADING = Decimal("70")  # Maximum 70%
    YEARS_TO_REMOVE = 10  # 10 continuous years removes loading
    EXEMPTION_BIRTHDATE = date(1934, 7, 1)  # Born before this date = exempt
    LHC_START_DATE = date(2000, 7, 1)  # LHC scheme started

    def calculate_loading(
        self,
        date_of_birth: date,
        as_of_date: date,
        join_date: date | None = None,
        continuous_cover_start: date | None = None,
    ) -> dict[str, Any]:
        """
        Calculate LHC loading for a person.

        Args:
            date_of_birth: Person's date of birth
            as_of_date: Date to calculate loading as of
            join_date: Date they joined/will join PHI (None = never had cover)
            continuous_cover_start: Start of continuous coverage (if different from join_date)

        Returns:
            Dictionary with:
                - loading_percentage: Current LHC loading (0-70)
                - is_exempt: Whether person is exempt from LHC
                - base_day: Their LHC base day
                - years_without_cover: Years counted for loading
                - years_to_removal: Years of continuous cover needed to remove loading
                - eligible: Whether LHC applies (age 31+)
        """
        result = {
            "loading_percentage": Decimal("0"),
            "is_exempt": False,
            "base_day": None,
            "years_without_cover": 0,
            "years_to_removal": None,
            "eligible": False,
            "certified_age_of_entry": None,
        }

        # Check exemption for people born before 1 July 1934
        if date_of_birth < self.EXEMPTION_BIRTHDATE:
            result["is_exempt"] = True
            return result

        # Calculate base day (July 1 after 31st birthday)
        base_day = self._calculate_base_day(date_of_birth)
        result["base_day"] = base_day

        # Not eligible if not yet reached base day
        if as_of_date < base_day:
            return result

        result["eligible"] = True

        # If never had cover
        if join_date is None:
            years_without = self._years_between(base_day, as_of_date)
            result["years_without_cover"] = years_without
            result["loading_percentage"] = min(
                self.MAX_LOADING,
                Decimal(str(years_without)) * self.LOADING_PER_YEAR,
            )
            result["certified_age_of_entry"] = None
            return result

        # Calculate years without cover
        cover_start = continuous_cover_start or join_date

        if join_date < base_day:
            # Joined before base day - no loading
            result["years_without_cover"] = 0
            result["loading_percentage"] = Decimal("0")
            result["certified_age_of_entry"] = self._get_age_at_date(date_of_birth, join_date)
        else:
            # Joined after base day
            years_without = self._years_between(base_day, join_date)
            result["years_without_cover"] = years_without
            result["loading_percentage"] = min(
                self.MAX_LOADING,
                Decimal(str(years_without)) * self.LOADING_PER_YEAR,
            )
            result["certified_age_of_entry"] = self._get_age_at_date(date_of_birth, join_date)

        # Check if loading should be removed (10 years continuous)
        if cover_start and result["loading_percentage"] > 0:
            years_covered = self._years_between(cover_start, as_of_date)
            if years_covered >= self.YEARS_TO_REMOVE:
                result["loading_percentage"] = Decimal("0")
                result["years_to_removal"] = 0
            else:
                result["years_to_removal"] = self.YEARS_TO_REMOVE - years_covered

        return result

    def _calculate_base_day(self, date_of_birth: date) -> date:
        """Calculate the base day (July 1 after 31st birthday)."""
        age_31_year = date_of_birth.year + self.BASE_AGE

        # Birthday is before July 1
        if date_of_birth.month < 7 or (date_of_birth.month == 7 and date_of_birth.day == 1):
            return date(age_31_year, 7, 1)
        else:
            # Birthday is after July 1, so base day is next year's July 1
            return date(age_31_year + 1, 7, 1)

    def _years_between(self, start: date, end: date) -> int:
        """Calculate complete years between two dates."""
        years = end.year - start.year
        if (end.month, end.day) < (start.month, start.day):
            years -= 1
        return max(0, years)

    def _get_age_at_date(self, dob: date, at_date: date) -> int:
        """Calculate age at a specific date."""
        return get_age(dob, at_date)


class AgeBasedDiscountCalculator:
    """
    Age-based discount (youth discount) calculator.

    Introduced 1 April 2019 to encourage young people to take out PHI.

    Rules:
    - Available to ages 18-29
    - 2% discount per year under 30
    - Maximum discount: 10% (at ages 18-25)
    - Locked in when you join and kept until age 41
    - Phase-out: Discount reduces by 2% each year from age 41 to 51

    Usage:
        calc = AgeBasedDiscountCalculator()
        discount = calc.calculate_discount(join_age, current_age)
    """

    MIN_AGE = 18
    MAX_AGE = 29
    DISCOUNT_PER_YEAR = Decimal("2")  # 2% per year under 30
    MAX_DISCOUNT = Decimal("10")  # Maximum 10%
    PHASE_OUT_START_AGE = 41
    PHASE_OUT_END_AGE = 51

    def calculate_discount(
        self,
        age_at_join: int,
        current_age: int,
    ) -> dict[str, Any]:
        """
        Calculate age-based discount.

        Args:
            age_at_join: Age when member joined PHI
            current_age: Current age

        Returns:
            Dictionary with:
                - original_discount: Discount percentage when joined
                - current_discount: Current discount percentage
                - eligible: Whether member is eligible
                - phase_out_status: Current phase-out status
        """
        result = {
            "original_discount": Decimal("0"),
            "current_discount": Decimal("0"),
            "eligible": False,
            "phase_out_status": None,
        }

        # Not eligible if joined outside age range
        if age_at_join < self.MIN_AGE or age_at_join > self.MAX_AGE:
            return result

        result["eligible"] = True

        # Calculate original discount (2% per year under 30, max 10%)
        years_under_30 = 30 - age_at_join
        original_discount = min(
            self.MAX_DISCOUNT,
            Decimal(str(years_under_30)) * self.DISCOUNT_PER_YEAR,
        )
        result["original_discount"] = original_discount

        # Apply phase-out if applicable
        if current_age < self.PHASE_OUT_START_AGE:
            # Full discount applies
            result["current_discount"] = original_discount
            result["phase_out_status"] = "not_started"
        elif current_age >= self.PHASE_OUT_END_AGE:
            # Discount fully phased out
            result["current_discount"] = Decimal("0")
            result["phase_out_status"] = "complete"
        else:
            # In phase-out period
            years_in_phase_out = current_age - self.PHASE_OUT_START_AGE
            reduction = Decimal(str(years_in_phase_out)) * self.DISCOUNT_PER_YEAR
            result["current_discount"] = max(Decimal("0"), original_discount - reduction)
            result["phase_out_status"] = "in_progress"

        return result

    def get_eligibility_date(self, date_of_birth: date) -> date:
        """
        Get the date a person becomes eligible for age-based discount.

        Args:
            date_of_birth: Person's date of birth

        Returns:
            Date they turn 18
        """
        return date(date_of_birth.year + 18, date_of_birth.month, date_of_birth.day)

    def get_phase_out_dates(
        self,
        date_of_birth: date,
    ) -> tuple[date, date]:
        """
        Get phase-out start and end dates.

        Args:
            date_of_birth: Person's date of birth

        Returns:
            Tuple of (phase_out_start, phase_out_end)
        """
        phase_out_start = date(
            date_of_birth.year + self.PHASE_OUT_START_AGE,
            date_of_birth.month,
            date_of_birth.day,
        )
        phase_out_end = date(
            date_of_birth.year + self.PHASE_OUT_END_AGE,
            date_of_birth.month,
            date_of_birth.day,
        )
        return phase_out_start, phase_out_end


class PHIRebateCalculator:
    """
    Private Health Insurance Rebate calculator.

    The government provides a rebate on PHI premiums, based on:
    - Age (higher rebate for older people)
    - Income (rebate reduces at higher income levels)
    - Family status (different thresholds for families)

    Rebate tiers (2024-2025):
    - Base tier: Full rebate
    - Tier 1: Reduced rebate
    - Tier 2: Further reduced
    - Tier 3: No rebate (MLS may apply)

    Usage:
        # With reference data (preferred):
        calc = PHIRebateCalculator(reference=reference_loader)
        rebate = calc.calculate_rebate(income, is_family, oldest_member_age)

        # Without reference data (uses hardcoded fallback values):
        calc = PHIRebateCalculator()
        rebate = calc.calculate_rebate(income, is_family, oldest_member_age)
    """

    # Fallback 2024-2025 income thresholds (used when reference data unavailable)
    FALLBACK_THRESHOLDS = {
        "single": [
            (0, 97000, "Base"),
            (97000, 113000, "Tier 1"),
            (113000, 151000, "Tier 2"),
            (151000, float("inf"), "Tier 3"),
        ],
        "family": [
            (0, 194000, "Base"),
            (194000, 226000, "Tier 1"),
            (226000, 302000, "Tier 2"),
            (302000, float("inf"), "Tier 3"),
        ],
    }

    # Fallback rebate percentages by tier and age bracket (2024-2025)
    FALLBACK_REBATE_PERCENTAGES = {
        "Base": {"under65": Decimal("24.608"), "65-69": Decimal("28.710"), "70+": Decimal("32.812")},
        "Tier 1": {"under65": Decimal("16.405"), "65-69": Decimal("20.507"), "70+": Decimal("24.608")},
        "Tier 2": {"under65": Decimal("8.202"), "65-69": Decimal("12.304"), "70+": Decimal("16.405")},
        "Tier 3": {"under65": Decimal("0"), "65-69": Decimal("0"), "70+": Decimal("0")},
    }

    # Medicare Levy Surcharge percentages by tier
    MLS_PERCENTAGES = {
        "Base": Decimal("0"),
        "Tier 1": Decimal("1.0"),
        "Tier 2": Decimal("1.25"),
        "Tier 3": Decimal("1.5"),
    }

    def __init__(self, reference=None):
        """
        Initialize the PHI Rebate Calculator.

        Args:
            reference: Optional ReferenceDataLoader for loading rebate tiers
                      from phi_rebate_tier.json. If not provided, uses
                      hardcoded fallback values.
        """
        self.reference = reference

    def calculate_rebate(
        self,
        income: int,
        is_family: bool,
        oldest_member_age: int,
        financial_year: str | None = None,
    ) -> dict[str, Any]:
        """
        Calculate PHI rebate percentage.

        Args:
            income: Annual taxable income
            is_family: True for family/couple policies
            oldest_member_age: Age of oldest person on policy
            financial_year: Financial year (default: current)

        Returns:
            Dictionary with:
                - rebate_percentage: Rebate as percentage (e.g., 24.608)
                - tier: Income tier name
                - age_bracket: Age bracket used
                - mls_percentage: Medicare Levy Surcharge if applicable
                - mls_liable: Whether MLS would apply without PHI
        """
        if financial_year is None:
            financial_year = get_financial_year(date.today())

        # Try to use reference data first
        if self.reference is not None:
            result = self._calculate_from_reference(
                income, is_family, oldest_member_age, financial_year
            )
            if result is not None:
                return result

        # Fall back to hardcoded values
        return self._calculate_from_fallback(
            income, is_family, oldest_member_age, financial_year
        )

    def _calculate_from_reference(
        self,
        income: int,
        is_family: bool,
        oldest_member_age: int,
        financial_year: str,
    ) -> dict[str, Any] | None:
        """Calculate rebate from reference data."""
        try:
            tiers = self.reference.get_phi_rebate_tiers(financial_year)
            if not tiers:
                return None

            # Find matching tier based on income
            threshold_field = "family_threshold_min" if is_family else "single_threshold_min"

            matching_tier = None
            for tier in sorted(tiers, key=lambda t: t.get(threshold_field, 0), reverse=True):
                if income >= tier.get(threshold_field, 0):
                    matching_tier = tier
                    break

            if not matching_tier:
                matching_tier = tiers[0]  # Default to base tier

            # Determine age bracket
            age_bracket = self._get_age_bracket(oldest_member_age)

            # Get rebate percentage based on age
            if oldest_member_age >= 70:
                rebate_pct = Decimal(str(matching_tier.get("rebate_pct_70_plus", 0)))
            elif oldest_member_age >= 65:
                rebate_pct = Decimal(str(matching_tier.get("rebate_pct_65_to_69", 0)))
            else:
                rebate_pct = Decimal(str(matching_tier.get("rebate_pct_under_65", 0)))

            # Convert from decimal (0.24608) to percentage (24.608)
            rebate_pct = rebate_pct * Decimal("100")

            # Get MLS percentage
            mls_pct = matching_tier.get("mls_percentage")
            if mls_pct is not None:
                mls_pct = Decimal(str(mls_pct)) * Decimal("100")
            else:
                mls_pct = Decimal("0")

            tier_name = matching_tier.get("tier_name", "Base")

            return {
                "rebate_percentage": rebate_pct,
                "tier": tier_name,
                "age_bracket": age_bracket,
                "mls_percentage": mls_pct,
                "mls_liable": tier_name != "Base",
                "financial_year": financial_year,
            }

        except Exception:
            # If anything fails, return None to use fallback
            return None

    def _calculate_from_fallback(
        self,
        income: int,
        is_family: bool,
        oldest_member_age: int,
        financial_year: str,
    ) -> dict[str, Any]:
        """Calculate rebate from hardcoded fallback values."""
        # Determine income tier
        tier_type = "family" if is_family else "single"
        tier = self._get_tier_fallback(income, tier_type)

        # Determine age bracket
        age_bracket = self._get_age_bracket(oldest_member_age)

        # Get rebate percentage
        rebate_pct = self.FALLBACK_REBATE_PERCENTAGES[tier][age_bracket]

        # Get MLS percentage
        mls_pct = self.MLS_PERCENTAGES[tier]

        return {
            "rebate_percentage": rebate_pct,
            "tier": tier,
            "age_bracket": age_bracket,
            "mls_percentage": mls_pct,
            "mls_liable": tier != "Base",
            "financial_year": financial_year,
        }

    def _get_tier_fallback(self, income: int, tier_type: str) -> str:
        """Determine income tier using fallback thresholds."""
        thresholds = self.FALLBACK_THRESHOLDS[tier_type]
        for min_inc, max_inc, tier_name in thresholds:
            if min_inc <= income < max_inc:
                return tier_name
        return "Tier 3"

    def _get_age_bracket(self, age: int) -> str:
        """Determine age bracket for rebate."""
        if age >= 70:
            return "70+"
        elif age >= 65:
            return "65-69"
        else:
            return "under65"

    def calculate_premium_with_rebate(
        self,
        gross_premium: Decimal,
        rebate_percentage: Decimal,
        claim_as_reduction: bool = True,
    ) -> dict[str, Decimal]:
        """
        Calculate premium after rebate.

        Args:
            gross_premium: Premium before rebate
            rebate_percentage: Rebate percentage (e.g., 24.608)
            claim_as_reduction: True to reduce premium, False for tax claim

        Returns:
            Dictionary with gross_premium, rebate_amount, net_premium
        """
        rebate_amount = (gross_premium * rebate_percentage / Decimal("100")).quantize(
            Decimal("0.01")
        )

        if claim_as_reduction:
            net_premium = gross_premium - rebate_amount
        else:
            # Full premium paid, rebate claimed at tax time
            net_premium = gross_premium

        return {
            "gross_premium": gross_premium,
            "rebate_amount": rebate_amount,
            "net_premium": net_premium,
        }


class RegulatoryCalculator:
    """
    Combined regulatory calculations.

    Provides a unified interface for all regulatory calculations.
    """

    def __init__(self, reference=None):
        """
        Initialize with individual calculators.

        Args:
            reference: Optional ReferenceDataLoader for loading PHI rebate
                      tiers from reference data.
        """
        self.lhc = LHCLoadingCalculator()
        self.age_discount = AgeBasedDiscountCalculator()
        self.rebate = PHIRebateCalculator(reference=reference)

    def calculate_all_adjustments(
        self,
        date_of_birth: date,
        join_date: date,
        current_date: date,
        income: int,
        is_family: bool,
        oldest_member_age: int | None = None,
    ) -> dict[str, Any]:
        """
        Calculate all regulatory adjustments for a policy.

        Args:
            date_of_birth: Primary member's DOB
            join_date: Date joined PHI
            current_date: Current date
            income: Household income
            is_family: True for family/couple
            oldest_member_age: Age of oldest member (default: from DOB)

        Returns:
            Dictionary with all adjustment details
        """
        current_age = get_age(date_of_birth, current_date)
        join_age = get_age(date_of_birth, join_date)

        if oldest_member_age is None:
            oldest_member_age = current_age

        return {
            "lhc_loading": self.lhc.calculate_loading(
                date_of_birth, current_date, join_date
            ),
            "age_discount": self.age_discount.calculate_discount(
                join_age, current_age
            ),
            "rebate": self.rebate.calculate_rebate(
                income, is_family, oldest_member_age
            ),
        }
