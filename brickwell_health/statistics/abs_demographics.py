"""
Australian Bureau of Statistics demographic distributions.

Based on ABS Census 2021 data for realistic population generation.
"""

from numpy.random import Generator as RNG


class ABSDemographics:
    """
    ABS Census 2021 based demographic distributions.

    Provides sampling methods for generating realistic Australian demographics.

    Usage:
        demographics = ABSDemographics(rng)
        state = demographics.sample_state()
        age = demographics.sample_age(role="Primary")
        gender = demographics.sample_gender()
    """

    # State population distribution (2021 Census)
    STATE_DISTRIBUTION = {
        "NSW": 0.319,
        "VIC": 0.259,
        "QLD": 0.202,
        "WA": 0.104,
        "SA": 0.070,
        "TAS": 0.022,
        "ACT": 0.017,
        "NT": 0.010,
    }

    # Gender distribution
    GENDER_DISTRIBUTION = {
        "Male": 0.491,
        "Female": 0.509,
    }

    # Adult age distribution (18-85+) for primary/partner members
    ADULT_AGE_BRACKETS = [
        (18, 24, 0.095),
        (25, 34, 0.175),
        (35, 44, 0.165),
        (45, 54, 0.150),
        (55, 64, 0.145),
        (65, 74, 0.135),
        (75, 84, 0.095),
        (85, 99, 0.040),
    ]

    # Child age distribution (0-25 for dependents)
    CHILD_AGE_BRACKETS = [
        (0, 4, 0.25),
        (5, 9, 0.25),
        (10, 14, 0.22),
        (15, 17, 0.15),
        (18, 21, 0.08),
        (22, 25, 0.05),
    ]

    # Partner age difference from primary
    PARTNER_AGE_DIFF = {
        "Male": -2.5,  # Male primaries typically older than partner
        "Female": 2.5,  # Female primaries typically younger than partner
    }
    PARTNER_AGE_STD = 4.0

    # Same-sex couple proportion
    SAME_SEX_COUPLE_RATE = 0.03

    # Number of children distribution by policy type
    CHILDREN_DISTRIBUTION = {
        "Family": [(1, 0.35), (2, 0.40), (3, 0.18), (4, 0.05), (5, 0.02)],
        "SingleParent": [(1, 0.50), (2, 0.35), (3, 0.12), (4, 0.03)],
    }

    # Postcode ranges by state
    POSTCODE_RANGES = {
        "NSW": (2000, 2999),
        "VIC": (3000, 3999),
        "QLD": (4000, 4999),
        "SA": (5000, 5999),
        "WA": (6000, 6999),
        "TAS": (7000, 7999),
        "NT": (800, 899),
        "ACT": (2600, 2639),
    }

    def __init__(self, rng: RNG):
        """
        Initialize with random number generator.

        Args:
            rng: NumPy random number generator
        """
        self.rng = rng

    def sample_state(self) -> str:
        """
        Sample a state based on population distribution.

        Returns:
            State code (e.g., "NSW", "VIC")
        """
        import numpy as np
        states = list(self.STATE_DISTRIBUTION.keys())
        probs = np.array(list(self.STATE_DISTRIBUTION.values()))
        probs = probs / probs.sum()  # Normalize to sum to 1
        return self.rng.choice(states, p=probs)

    def sample_gender(self) -> str:
        """
        Sample gender based on distribution.

        Returns:
            Gender ("Male" or "Female")
        """
        import numpy as np
        genders = list(self.GENDER_DISTRIBUTION.keys())
        probs = np.array(list(self.GENDER_DISTRIBUTION.values()))
        probs = probs / probs.sum()  # Normalize to sum to 1
        return self.rng.choice(genders, p=probs)

    def sample_age(self, role: str = "Primary") -> int:
        """
        Sample age based on role.

        Args:
            role: "Primary", "Partner", or "Dependent"

        Returns:
            Age in years
        """
        if role == "Dependent":
            brackets = self.CHILD_AGE_BRACKETS
        else:
            brackets = self.ADULT_AGE_BRACKETS

        # Select bracket based on weights
        bracket_weights = [b[2] for b in brackets]
        total = sum(bracket_weights)
        bracket_probs = [w / total for w in bracket_weights]

        bracket_idx = self.rng.choice(len(brackets), p=bracket_probs)
        min_age, max_age, _ = brackets[bracket_idx]

        # Sample within bracket
        return int(self.rng.integers(min_age, max_age + 1))

    def sample_partner_age(self, primary_age: int, primary_gender: str) -> int:
        """
        Sample partner age correlated to primary member.

        Args:
            primary_age: Age of primary member
            primary_gender: Gender of primary member

        Returns:
            Partner age
        """
        # Check for same-sex couple
        if self.rng.random() < self.SAME_SEX_COUPLE_RATE:
            # Same-sex: similar age distribution
            age_diff = self.rng.normal(0, self.PARTNER_AGE_STD)
        else:
            # Opposite-sex: apply typical age difference
            mean_diff = self.PARTNER_AGE_DIFF.get(primary_gender, 0)
            age_diff = self.rng.normal(mean_diff, self.PARTNER_AGE_STD)

        partner_age = int(primary_age + age_diff)

        # Clamp to valid adult range
        return max(18, min(99, partner_age))

    def sample_num_children(self, policy_type: str) -> int:
        """
        Sample number of children for family policy types.

        Args:
            policy_type: "Family" or "SingleParent"

        Returns:
            Number of children (0 if not applicable)
        """
        if policy_type not in self.CHILDREN_DISTRIBUTION:
            return 0

        distribution = self.CHILDREN_DISTRIBUTION[policy_type]
        counts = [d[0] for d in distribution]
        probs = [d[1] for d in distribution]
        total = sum(probs)
        probs = [p / total for p in probs]

        return self.rng.choice(counts, p=probs)

    def sample_child_ages(self, num_children: int, parent_age: int) -> list[int]:
        """
        Sample realistic child ages given parent age.

        Args:
            num_children: Number of children to generate
            parent_age: Age of parent

        Returns:
            List of child ages (oldest first)
        """
        if num_children == 0:
            return []

        # Minimum parent age at birth: 18
        # Maximum child age: parent_age - 18, capped at 25 for dependents
        max_child_age = min(25, parent_age - 18)
        if max_child_age < 0:
            return []

        ages = []
        for _ in range(num_children):
            # Sample from child distribution but cap at max
            age = self.sample_age(role="Dependent")
            age = min(age, max_child_age)
            ages.append(age)

        return sorted(ages, reverse=True)  # Oldest first

    def sample_postcode(self, state: str) -> str:
        """
        Sample a postcode for the given state.

        Args:
            state: State code

        Returns:
            Postcode string (4 digits, zero-padded)
        """
        min_pc, max_pc = self.POSTCODE_RANGES.get(state, (2000, 2999))
        postcode = int(self.rng.integers(min_pc, max_pc + 1))
        return str(postcode).zfill(4)

    def sample_title(self, gender: str, age: int) -> str:
        """
        Sample appropriate title based on gender and age.

        Args:
            gender: "Male" or "Female"
            age: Age in years

        Returns:
            Title string
        """
        if age < 18:
            return "Master" if gender == "Male" else "Miss"

        if gender == "Male":
            return "Mr"
        elif gender == "Female":
            # Simplified: approximate married proportion by age
            if age > 25 and self.rng.random() < 0.6:
                return "Mrs"
            return "Ms"
        return ""

    def get_phi_penetration_by_age(self, age: int) -> float:
        """
        Get PHI (Private Health Insurance) penetration rate by age.

        Based on APRA PHI statistics.

        Args:
            age: Age in years

        Returns:
            Penetration rate (0-1)
        """
        # PHI penetration by age bracket (APRA data approximation)
        if age < 25:
            return 0.35
        elif age < 35:
            return 0.40
        elif age < 45:
            return 0.50
        elif age < 55:
            return 0.55
        elif age < 65:
            return 0.55
        elif age < 75:
            return 0.60
        else:
            return 0.65
