"""
Income model for Brickwell Health Simulator.

Models household income for rebate tier determination.
"""

from numpy.random import Generator as RNG


class IncomeModel:
    """
    Models household income distribution.

    Based on ABS income data, adjusted for PHI members
    (who tend to have higher incomes).
    """

    # Base income by age bracket (median)
    AGE_INCOME = {
        (18, 24): 35000,
        (25, 34): 60000,
        (35, 44): 80000,
        (45, 54): 90000,
        (55, 64): 75000,
        (65, 74): 55000,
        (75, 99): 40000,
    }

    # State income multipliers (relative to national)
    STATE_MULTIPLIERS = {
        "NSW": 1.10,
        "VIC": 1.05,
        "ACT": 1.30,
        "WA": 1.15,
        "QLD": 0.95,
        "SA": 0.90,
        "TAS": 0.85,
        "NT": 1.10,
    }

    # PHI members have higher income on average
    PHI_INCOME_MULTIPLIER = 1.2

    # Rebate income tiers (2024-2025 FY)
    REBATE_TIERS_SINGLE = [
        (0, 97000, "Tier 0"),
        (97000, 113000, "Tier 1"),
        (113000, 151000, "Tier 2"),
        (151000, float("inf"), "Tier 3"),
    ]

    REBATE_TIERS_FAMILY = [
        (0, 194000, "Tier 0"),
        (194000, 226000, "Tier 1"),
        (226000, 302000, "Tier 2"),
        (302000, float("inf"), "Tier 3"),
    ]

    def __init__(self, rng: RNG):
        """
        Initialize the income model.

        Args:
            rng: NumPy random number generator
        """
        self.rng = rng

    def sample_income(
        self,
        age: int,
        state: str,
        is_family: bool = False,
    ) -> int:
        """
        Sample annual taxable income.

        Args:
            age: Age of primary income earner
            state: State code
            is_family: True for family/couple income

        Returns:
            Annual income in dollars
        """
        # Get base income for age
        base_income = self._get_base_income(age)

        # Apply state multiplier
        state_mult = self.STATE_MULTIPLIERS.get(state, 1.0)
        income = base_income * state_mult

        # Apply PHI member adjustment
        income *= self.PHI_INCOME_MULTIPLIER

        # Family income is typically 1.5-1.8x single
        if is_family:
            income *= self.rng.uniform(1.5, 1.8)

        # Add log-normal variation
        variation = self.rng.lognormal(0, 0.4)
        income *= variation / 1.08  # Adjust for lognormal mean

        # Clamp to reasonable range
        income = max(20000, min(500000, income))

        return int(income)

    def _get_base_income(self, age: int) -> int:
        """Get base income for an age."""
        for (min_age, max_age), income in self.AGE_INCOME.items():
            if min_age <= age <= max_age:
                return income
        return 50000  # Default

    def get_rebate_tier(
        self,
        income: int,
        is_family: bool,
    ) -> str:
        """
        Determine rebate tier based on income.

        Args:
            income: Annual taxable income
            is_family: True for family/couple

        Returns:
            Rebate tier name
        """
        tiers = self.REBATE_TIERS_FAMILY if is_family else self.REBATE_TIERS_SINGLE

        for min_inc, max_inc, tier in tiers:
            if min_inc <= income < max_inc:
                return tier

        return "Tier 3"  # Default to highest

    def get_rebate_percentage(
        self,
        income: int,
        is_family: bool,
        oldest_age: int,
    ) -> float:
        """
        Get rebate percentage for income and age.

        Args:
            income: Annual taxable income
            is_family: True for family/couple
            oldest_age: Age of oldest person on policy

        Returns:
            Rebate percentage (0-1)
        """
        tier = self.get_rebate_tier(income, is_family)

        # 2024-2025 rebate percentages
        rebate_table = {
            "Tier 0": {"under65": 0.2465, "65-69": 0.2882, "70+": 0.3298},
            "Tier 1": {"under65": 0.1644, "65-69": 0.2060, "70+": 0.2477},
            "Tier 2": {"under65": 0.0822, "65-69": 0.1238, "70+": 0.1644},
            "Tier 3": {"under65": 0.0000, "65-69": 0.0000, "70+": 0.0000},
        }

        tier_rates = rebate_table.get(tier, rebate_table["Tier 3"])

        if oldest_age >= 70:
            return tier_rates["70+"]
        elif oldest_age >= 65:
            return tier_rates["65-69"]
        else:
            return tier_rates["under65"]

    def sample_declared_income_range(self, actual_income: int) -> str:
        """
        Sample declared income range (for rebate forms).

        Members may declare a range rather than exact income.

        Args:
            actual_income: Actual income

        Returns:
            Income range string
        """
        ranges = [
            (0, 50000, "$0-$50,000"),
            (50000, 70000, "$50,001-$70,000"),
            (70000, 90000, "$70,001-$90,000"),
            (90000, 105000, "$90,001-$105,000"),
            (105000, 120000, "$105,001-$120,000"),
            (120000, 140000, "$120,001-$140,000"),
            (140000, 180000, "$140,001-$180,000"),
            (180000, 250000, "$180,001-$250,000"),
            (250000, float("inf"), "Over $250,000"),
        ]

        for min_inc, max_inc, label in ranges:
            if min_inc <= actual_income < max_inc:
                return label

        return "Over $250,000"
