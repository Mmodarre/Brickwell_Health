"""
Common distribution functions for simulation.

Provides standalone distribution sampling functions.
"""

from numpy.random import Generator as RNG


def sample_from_distribution(
    rng: RNG,
    distribution: dict[str, float],
) -> str:
    """
    Sample from a categorical distribution.

    Args:
        rng: NumPy random number generator
        distribution: Dict mapping options to probabilities/weights

    Returns:
        Sampled option key
    """
    options = list(distribution.keys())
    weights = list(distribution.values())
    total = sum(weights)
    probs = [w / total for w in weights]
    return rng.choice(options, p=probs)


def sample_age_for_role(rng: RNG, role: str) -> int:
    """
    Sample age appropriate for a member role.

    Args:
        rng: NumPy random number generator
        role: "Primary", "Partner", or "Dependent"

    Returns:
        Age in years
    """
    if role == "Dependent":
        # Children 0-25
        brackets = [(0, 10, 0.4), (11, 17, 0.35), (18, 25, 0.25)]
    else:
        # Adults 25-75 (primary members tend to be working age)
        brackets = [
            (25, 34, 0.20),
            (35, 44, 0.25),
            (45, 54, 0.25),
            (55, 64, 0.20),
            (65, 75, 0.10),
        ]

    # Select bracket
    weights = [b[2] for b in brackets]
    total = sum(weights)
    probs = [w / total for w in weights]
    bracket_idx = rng.choice(len(brackets), p=probs)
    min_age, max_age, _ = brackets[bracket_idx]

    return int(rng.integers(min_age, max_age + 1))


def sample_partner_age(rng: RNG, primary_age: int, primary_gender: str) -> int:
    """
    Sample partner age correlated to primary.

    Args:
        rng: NumPy random number generator
        primary_age: Age of primary member
        primary_gender: Gender of primary member

    Returns:
        Partner age
    """
    # Typical age difference
    if primary_gender == "Male":
        mean_diff = -2.5  # Partner younger
    else:
        mean_diff = 2.5  # Partner older

    age_diff = rng.normal(mean_diff, 4.0)
    partner_age = int(primary_age + age_diff)

    return max(18, min(99, partner_age))


def sample_num_children(rng: RNG, policy_type: str) -> int:
    """
    Sample number of children for a policy.

    Args:
        rng: NumPy random number generator
        policy_type: "Family" or "SingleParent"

    Returns:
        Number of children
    """
    if policy_type == "Family":
        distribution = [(1, 0.35), (2, 0.40), (3, 0.18), (4, 0.07)]
    elif policy_type == "SingleParent":
        distribution = [(1, 0.50), (2, 0.35), (3, 0.15)]
    else:
        return 0

    counts = [d[0] for d in distribution]
    probs = [d[1] for d in distribution]
    total = sum(probs)
    probs = [p / total for p in probs]

    return rng.choice(counts, p=probs)


def sample_child_ages(
    rng: RNG,
    num_children: int,
    parent_age: int,
) -> list[int]:
    """
    Sample child ages given parent age.

    Args:
        rng: NumPy random number generator
        num_children: Number of children
        parent_age: Age of parent

    Returns:
        List of child ages (oldest first)
    """
    if num_children == 0:
        return []

    max_child_age = min(25, parent_age - 18)
    if max_child_age < 0:
        return []

    ages = []
    for _ in range(num_children):
        age = sample_age_for_role(rng, "Dependent")
        age = min(age, max_child_age)
        ages.append(age)

    return sorted(ages, reverse=True)


def sample_waiting_period_months(
    rng: RNG,
    period_type: str,
    has_transfer: bool = False,
) -> int:
    """
    Sample waiting period duration.

    Args:
        rng: NumPy random number generator
        period_type: Type of waiting period
        has_transfer: Whether member has transfer certificate

    Returns:
        Duration in months
    """
    if has_transfer:
        # Reduced waiting periods with transfer
        return 0

    # Standard waiting periods
    durations = {
        "General": 2,
        "Pre-existing": 12,
        "Obstetric": 12,
        "Psychiatric": 2,
    }

    return durations.get(period_type, 2)


def sample_claim_amount(
    rng: RNG,
    claim_type: str,
    mean_amount: float,
) -> float:
    """
    Sample a claim amount from log-normal distribution.

    Args:
        rng: NumPy random number generator
        claim_type: Type of claim
        mean_amount: Mean claim amount

    Returns:
        Claim amount
    """
    # Use log-normal distribution for realistic claim amounts
    # Parameters chosen to match mean while allowing variation
    sigma = 0.5  # Controls spread
    mu = float(rng.lognormal(0, sigma))

    # Scale to achieve desired mean
    amount = mean_amount * mu / 1.28  # exp(sigma^2/2) ≈ 1.28 for sigma=0.5

    return max(10.0, amount)  # Minimum $10
