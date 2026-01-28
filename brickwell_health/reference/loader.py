"""
Reference data loader for Brickwell Health Simulator.

Loads and caches reference data from JSON files with support for
temporal lookups (effective-dated records).
"""

import json
from datetime import date
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()


def get_effective_record(
    records: list[dict[str, Any]],
    as_of_date: date,
    key_fields: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """
    Get the record effective as of a specific date.

    For reference data with effective_date/end_date, this returns the
    record that is effective on the given date.

    Args:
        records: List of records to search
        as_of_date: Date to find effective record for
        key_fields: Optional dict of field names/values to match

    Returns:
        The effective record, or None if not found
    """
    as_of_str = as_of_date.isoformat()
    
    matching = []
    for r in records:
        # Check key fields match
        if key_fields:
            if not all(r.get(k) == v for k, v in key_fields.items()):
                continue
        
        # Check effective date
        eff_date = r.get("effective_date")
        if eff_date is None:
            # No effective date means always effective
            matching.append(r)
            continue
            
        # Convert to string for comparison if needed
        if isinstance(eff_date, date):
            eff_date = eff_date.isoformat()
            
        if eff_date > as_of_str:
            continue
            
        # Check end date
        end_date = r.get("end_date")
        if end_date:
            if isinstance(end_date, date):
                end_date = end_date.isoformat()
            if end_date <= as_of_str:
                continue
        
        matching.append(r)
    
    if not matching:
        return None
    
    # Return the most recent effective record
    def get_eff_date(rec: dict) -> str:
        eff = rec.get("effective_date", "")
        if isinstance(eff, date):
            return eff.isoformat()
        return eff or ""
    
    return max(matching, key=get_eff_date)


class ReferenceDataLoader:
    """
    Loads and caches reference data from JSON files.

    Thread-safe for read operations. Each worker should have its own instance.

    Usage:
        loader = ReferenceDataLoader(Path("data/reference"))
        products = loader.load("product.json")
        active_products = loader.get_products()
    """

    def __init__(self, reference_path: Path | str):
        """
        Initialize the reference data loader.

        Args:
            reference_path: Path to the reference data directory
        """
        self.reference_path = Path(reference_path)
        self._cache: dict[str, list[dict[str, Any]]] = {}

    def load(self, filename: str) -> list[dict[str, Any]]:
        """
        Load a reference data file with caching.

        Args:
            filename: Name of the JSON file to load

        Returns:
            List of records from the file

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        if filename not in self._cache:
            file_path = self.reference_path / filename
            
            if not file_path.exists():
                raise FileNotFoundError(f"Reference data file not found: {file_path}")
            
            logger.info("loading_reference_data", file=filename)

            with open(file_path) as f:
                data = json.load(f)

            # Handle both list and dict formats
            if isinstance(data, dict):
                # Some files might have a wrapper object
                data = data.get("records", [data])
            
            self._cache[filename] = data

            logger.info(
                "loaded_reference_data",
                file=filename,
                records=len(self._cache[filename]),
            )

        return self._cache[filename]

    def clear_cache(self) -> None:
        """Clear the reference data cache."""
        self._cache.clear()

    # =========================================================================
    # Product Methods
    # =========================================================================

    def get_products(self, active_only: bool = True) -> list[dict[str, Any]]:
        """
        Get products, optionally filtered to active only.

        Args:
            active_only: If True, return only active products

        Returns:
            List of product records
        """
        products = self.load("product.json")
        if active_only:
            return [p for p in products if p.get("status") == "Active"]
        return products

    def get_product_by_id(self, product_id: int) -> dict[str, Any] | None:
        """Get a product by ID."""
        products = self.load("product.json")
        return next((p for p in products if p.get("product_id") == product_id), None)

    def get_products_by_tier(self, tier: str) -> list[dict[str, Any]]:
        """
        Get active products for a specific hospital tier.

        Args:
            tier: Tier name (Gold/Silver/Bronze/Basic)

        Returns:
            List of matching products
        """
        products = self.get_products()
        tier_map = {"Gold": 1, "Silver": 2, "Bronze": 3, "Basic": 4}
        tier_id = tier_map.get(tier)
        if tier_id is None:
            return []
        return [p for p in products if p.get("product_tier_id") == tier_id]

    def get_product_types(self) -> list[dict[str, Any]]:
        """Get product types (Hospital/Extras/Combined/Ambulance)."""
        return self.load("product_type.json")

    def get_product_tiers(self) -> list[dict[str, Any]]:
        """Get product tiers (Gold/Silver/Bronze/Basic)."""
        return self.load("product_tier.json")

    # =========================================================================
    # Benefit Methods
    # =========================================================================

    def get_benefit_categories(self) -> list[dict[str, Any]]:
        """Get benefit categories."""
        return self.load("benefit_category.json")

    def get_clinical_categories(self) -> list[dict[str, Any]]:
        """Get hospital clinical categories."""
        return self.load("clinical_category.json")

    def get_product_benefits(self, product_id: int) -> list[dict[str, Any]]:
        """Get benefits for a specific product."""
        benefits = self.load("product_benefit.json")
        return [b for b in benefits if b.get("product_id") == product_id]

    # =========================================================================
    # Waiting Period Methods
    # =========================================================================

    def get_waiting_period_rules(self, product_id: int) -> list[dict[str, Any]]:
        """
        Get waiting period rules for a product.

        Args:
            product_id: Product ID to get rules for

        Returns:
            List of waiting period rules
        """
        rules = self.load("waiting_period_rule.json")
        return [
            r for r in rules
            if r.get("product_id") == product_id and r.get("is_active", True)
        ]

    # =========================================================================
    # Location Methods
    # =========================================================================

    def get_states(self) -> list[dict[str, Any]]:
        """Get Australian states/territories."""
        return self.load("state_territory.json")

    def get_state_by_code(self, state_code: str) -> dict[str, Any] | None:
        """Get a state by its code."""
        states = self.get_states()
        return next(
            (s for s in states if s.get("state_code") == state_code.upper()),
            None
        )

    # =========================================================================
    # Provider Methods
    # =========================================================================

    def get_providers(self, active_only: bool = True) -> list[dict[str, Any]]:
        """Get healthcare providers."""
        providers = self.load("provider.json")
        if active_only:
            return [p for p in providers if p.get("status") == "Active"]
        return providers

    def get_hospitals(self, active_only: bool = True) -> list[dict[str, Any]]:
        """Get hospitals."""
        hospitals = self.load("hospital.json")
        if active_only:
            return [h for h in hospitals if h.get("is_active", True)]
        return hospitals

    # =========================================================================
    # Claims Methods
    # =========================================================================

    def get_mbs_items(self, category_id: int | None = None) -> list[dict[str, Any]]:
        """
        Get MBS (Medical Benefits Schedule) items.

        Args:
            category_id: Optional category to filter by

        Returns:
            List of MBS items
        """
        items = self.load("mbs_item.json")
        if category_id is not None:
            return [i for i in items if i.get("mbs_category_id") == category_id]
        return items

    def get_extras_items(self, service_type_id: int | None = None) -> list[dict[str, Any]]:
        """
        Get extras item codes (dental, optical, etc.).

        Args:
            service_type_id: Optional service type to filter by

        Returns:
            List of extras items
        """
        items = self.load("extras_item_code.json")
        if service_type_id is not None:
            return [i for i in items if i.get("service_type_id") == service_type_id]
        return items

    def get_drg_codes(self) -> list[dict[str, Any]]:
        """Get DRG (Diagnosis Related Group) codes."""
        return self.load("drg_code.json")

    def get_claim_rejection_reasons(self) -> list[dict[str, Any]]:
        """Get claim rejection reasons."""
        return self.load("claim_rejection_reason.json")

    # =========================================================================
    # Premium/Rebate Methods
    # =========================================================================

    def get_premium_rates(
        self,
        product_id: int | None = None,
        state: str | None = None,
        as_of_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get premium rates with optional filtering.

        Args:
            product_id: Filter by product
            state: Filter by state
            as_of_date: Get rates effective on this date

        Returns:
            List of premium rate records
        """
        try:
            rates = self.load("premium_rate.json")
        except FileNotFoundError:
            logger.warning("premium_rate.json not found, returning empty list")
            return []

        if product_id is not None:
            rates = [r for r in rates if r.get("product_id") == product_id]

        if state is not None:
            # Match by state_territory_id or state code
            state_info = self.get_state_by_code(state)
            if state_info:
                state_id = state_info.get("state_territory_id")
                rates = [r for r in rates if r.get("state_territory_id") == state_id]

        if as_of_date is not None:
            rates = [
                r for r in rates
                if get_effective_record([r], as_of_date) is not None
            ]

        return rates

    def get_phi_rebate_tiers(self, financial_year: str | None = None) -> list[dict[str, Any]]:
        """
        Get PHI rebate tiers.

        Args:
            financial_year: Optional financial year to filter by (e.g., "2024-2025")

        Returns:
            List of rebate tier records
        """
        try:
            tiers = self.load("phi_rebate_tier.json")
        except FileNotFoundError:
            logger.warning("phi_rebate_tier.json not found, returning empty list")
            return []

        if financial_year:
            tiers = [
                t for t in tiers
                if t.get("financial_year") == financial_year and t.get("is_active", True)
            ]

        return tiers

    def get_rebate_percentage(
        self,
        income: int,
        is_family: bool,
        oldest_member_age: int,
        financial_year: str = "2024-2025",
    ) -> float:
        """
        Look up rebate percentage from reference data.

        Args:
            income: Annual taxable income
            is_family: True for family/couple policies
            oldest_member_age: Age of oldest person on policy
            financial_year: Financial year for lookup

        Returns:
            Rebate percentage as decimal (e.g., 0.2465 for 24.65%)
        """
        tiers = self.get_phi_rebate_tiers(financial_year)
        if not tiers:
            return 0.0

        # Find matching tier based on income
        threshold_field = "family_threshold_min" if is_family else "single_threshold_min"
        
        matching_tier = None
        for tier in sorted(tiers, key=lambda t: t.get(threshold_field, 0), reverse=True):
            if income >= tier.get(threshold_field, 0):
                matching_tier = tier
                break

        if not matching_tier:
            matching_tier = tiers[0]  # Default to base tier

        # Select rebate by age bracket
        if oldest_member_age >= 70:
            return float(matching_tier.get("rebate_pct_70_plus", 0))
        elif oldest_member_age >= 65:
            return float(matching_tier.get("rebate_pct_65_to_69", 0))
        else:
            return float(matching_tier.get("rebate_pct_under_65", 0))

    # =========================================================================
    # Excess Methods
    # =========================================================================

    def get_excess_options(self) -> list[dict[str, Any]]:
        """Get available excess options."""
        return self.load("excess_option.json")

    # =========================================================================
    # Prosthesis Methods
    # =========================================================================

    def get_prosthesis_categories(self) -> list[dict[str, Any]]:
        """Get prosthesis categories."""
        try:
            return self.load("prosthesis_category.json")
        except FileNotFoundError:
            return []

    def get_prosthesis_items(self, category_id: int | None = None) -> list[dict[str, Any]]:
        """Get prosthesis items."""
        try:
            items = self.load("prosthesis_list_item.json")
        except FileNotFoundError:
            return []

        if category_id is not None:
            items = [i for i in items if i.get("prosthesis_category_id") == category_id]

        return items

    # =========================================================================
    # Benefit Limit Methods
    # =========================================================================

    def get_benefit_limits(self) -> list[dict[str, Any]]:
        """Get all benefit limits."""
        try:
            return self.load("benefit_limit.json")
        except FileNotFoundError:
            return []

    def get_benefit_limit_periods(self) -> list[dict[str, Any]]:
        """Get benefit limit period definitions."""
        try:
            return self.load("benefit_limit_period.json")
        except FileNotFoundError:
            return []

    def build_benefit_limit_lookup(self) -> dict[tuple[int, int], dict[str, Any]]:
        """
        Build a lookup dictionary for benefit limits.

        Returns:
            Dict mapping (product_id, benefit_category_id) -> limit info
        """
        limits = self.get_benefit_limits()
        product_benefits = self.load("product_benefit.json")

        # Build product_benefit lookup: product_benefit_id -> (product_id, benefit_category_id)
        pb_lookup = {
            pb["product_benefit_id"]: (pb["product_id"], pb["benefit_category_id"])
            for pb in product_benefits
        }

        # Build limit lookup: (product_id, benefit_category_id) -> limit info
        result = {}
        for lim in limits:
            pb_id = lim.get("product_benefit_id")
            if pb_id not in pb_lookup:
                continue

            product_id, benefit_category_id = pb_lookup[pb_id]
            key = (product_id, benefit_category_id)

            # Only store if has actual limit values
            if lim.get("limit_amount") is not None or lim.get("limit_count") is not None:
                result[key] = {
                    "limit_amount": lim.get("limit_amount"),
                    "limit_count": lim.get("limit_count"),
                    "per_person_limit": lim.get("per_person_limit"),
                    "per_service_limit": lim.get("per_service_limit"),
                    "limit_type": lim.get("limit_type", "Dollar"),
                    "limit_period_id": lim.get("limit_period_id", 1),  # Default to Calendar Year
                }

        return result
