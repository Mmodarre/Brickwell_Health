"""
Product selection model for Brickwell Health Simulator.

Models how members choose insurance products.
"""

from typing import Any

from numpy.random import Generator as RNG

from brickwell_health.reference.loader import ReferenceDataLoader


class ProductSelectionModel:
    """
    Models product selection behavior.

    Takes into account:
    - Age of applicants
    - Policy type (Single/Couple/Family)
    - State (price differences)
    - Configured tier distribution
    """

    def __init__(
        self,
        rng: RNG,
        reference: ReferenceDataLoader,
        tier_distribution: dict[str, float],
    ):
        """
        Initialize the product selection model.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            tier_distribution: Target tier distribution
        """
        self.rng = rng
        self.reference = reference
        self.tier_distribution = tier_distribution

    def select_product(
        self,
        policy_type: str,
        state: str,
        primary_age: int,
        has_hospital: bool = True,
        has_extras: bool = True,
    ) -> dict[str, Any] | None:
        """
        Select a product for a new policy.

        Args:
            policy_type: Single/Couple/Family/SingleParent
            state: State code
            primary_age: Age of primary applicant
            has_hospital: Whether to include hospital cover
            has_extras: Whether to include extras cover

        Returns:
            Selected product record, or None if no suitable product found
        """
        products = self.reference.get_products(active_only=True)

        if not products:
            return None

        # Filter by coverage type
        suitable = []
        for p in products:
            if has_hospital and has_extras:
                # Combined product or separate hospital/extras
                if p.get("is_hospital") or p.get("is_extras"):
                    suitable.append(p)
            elif has_hospital:
                if p.get("is_hospital") and not p.get("is_extras"):
                    suitable.append(p)
            elif has_extras:
                if p.get("is_extras") and not p.get("is_hospital"):
                    suitable.append(p)

        if not suitable:
            suitable = products  # Fallback to all products

        # Filter by policy type availability
        suitable = [
            p for p in suitable
            if policy_type in (p.get("available_policy_types") or "")
        ]

        if not suitable:
            suitable = products

        # Select tier based on distribution
        tier = self._select_tier()

        # Prefer products in selected tier
        tier_map = {"Gold": 1, "Silver": 2, "Bronze": 3, "Basic": 4}
        tier_id = tier_map.get(tier)

        tier_products = [p for p in suitable if p.get("product_tier_id") == tier_id]

        if tier_products:
            return self.rng.choice(tier_products)
        else:
            return self.rng.choice(suitable)

    def _select_tier(self) -> str:
        """
        Select a hospital tier based on distribution.

        Returns:
            Tier name (Gold/Silver/Bronze/Basic)
        """
        tiers = list(self.tier_distribution.keys())
        weights = list(self.tier_distribution.values())
        total = sum(weights)
        probs = [w / total for w in weights]
        return self.rng.choice(tiers, p=probs)

    def select_excess(
        self,
        product: dict[str, Any],
        primary_age: int,
    ) -> float | None:
        """
        Select an excess amount for a hospital product.

        Younger members more likely to choose higher excess.

        Args:
            product: Product record
            primary_age: Age of primary applicant

        Returns:
            Excess amount, or None if not applicable
        """
        if not product.get("is_hospital"):
            return None

        excess_options = self.reference.get_excess_options()
        if not excess_options:
            return product.get("default_excess", 500.0)

        # Get valid excess amounts
        amounts = sorted([e.get("excess_amount", 0) for e in excess_options])
        if not amounts:
            return product.get("default_excess", 500.0)

        # Age-based preference for excess
        # Younger → higher excess (lower premium)
        # Older → lower excess (more claims expected)
        if primary_age < 35:
            # Prefer higher excess
            weights = [0.1, 0.2, 0.3, 0.4][:len(amounts)]
        elif primary_age < 50:
            # Balanced
            weights = [0.25] * len(amounts)
        else:
            # Prefer lower excess
            weights = [0.4, 0.3, 0.2, 0.1][:len(amounts)]

        # Pad weights if needed
        while len(weights) < len(amounts):
            weights.append(0.1)

        total = sum(weights)
        probs = [w / total for w in weights]

        return float(self.rng.choice(amounts, p=probs))

    def should_include_ambulance(self, state: str) -> bool:
        """
        Determine if ambulance cover should be included.

        Some states have free ambulance schemes.

        Args:
            state: State code

        Returns:
            True if ambulance cover should be added
        """
        # States with government ambulance schemes
        free_ambulance_states = {"QLD", "TAS"}

        if state in free_ambulance_states:
            # Less likely to add ambulance if state provides free
            return self.rng.random() < 0.3
        else:
            # More likely to add ambulance cover
            return self.rng.random() < 0.8
