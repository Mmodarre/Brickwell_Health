"""
Reference data loader for Brickwell Health Simulator.

Loads and caches reference data from database tables with fallback to JSON files
for reference data not yet migrated to database.
"""

import json
from datetime import date
from pathlib import Path
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.engine import Engine

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
    Loads and caches reference data from database tables.

    For reference data migrated to database tables, queries the database.
    For reference data not yet migrated, falls back to JSON files.

    Thread-safe for read operations. Each worker should have its own instance.

    Usage:
        loader = ReferenceDataLoader(engine, Path("data/reference"))
        products = loader.get_products()
        providers = loader.get_providers()
    """

    def __init__(self, engine: Engine, json_fallback_path: Path | str | None = None):
        """
        Initialize the reference data loader.

        Args:
            engine: SQLAlchemy engine for database connection
            json_fallback_path: Optional path to JSON files for non-migrated data
        """
        self.engine = engine
        self.json_fallback_path = Path(json_fallback_path) if json_fallback_path else None
        self._cache: dict[str, list[dict[str, Any]]] = {}

    def _query_table(self, table_name: str, cache_key: str | None = None) -> list[dict[str, Any]]:
        """
        Query a database table and return results as list of dicts.

        Args:
            table_name: Name of the table to query
            cache_key: Optional cache key (defaults to table_name)

        Returns:
            List of records as dictionaries
        """
        if cache_key is None:
            cache_key = table_name

        if cache_key not in self._cache:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table_name}"))
                # Convert rows to dicts
                columns = result.keys()
                self._cache[cache_key] = [
                    dict(zip(columns, row)) for row in result.fetchall()
                ]

            logger.debug(
                "loaded_reference_from_db",
                table=table_name,
                records=len(self._cache[cache_key])
            )

        return self._cache[cache_key]

    def _load_json_fallback(self, filename: str) -> list[dict[str, Any]]:
        """
        Load reference data from JSON file (fallback for non-migrated data).

        Args:
            filename: Name of the JSON file to load

        Returns:
            List of records from the file

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        if filename not in self._cache:
            if not self.json_fallback_path:
                raise FileNotFoundError(
                    f"No JSON fallback path configured and {filename} not in database"
                )

            file_path = self.json_fallback_path / filename

            if not file_path.exists():
                raise FileNotFoundError(f"Reference data file not found: {file_path}")

            logger.info("loading_reference_data_from_json", file=filename)

            with open(file_path) as f:
                data = json.load(f)

            # Handle both list and dict formats
            if isinstance(data, dict):
                # Some files might have a wrapper object
                data = data.get("records", [data])

            self._cache[filename] = data

            logger.info(
                "loaded_reference_data_from_json",
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
        products = self._query_table("product")
        if active_only:
            return [p for p in products if p.get("status") == "Active"]
        return products

    def get_product_by_id(self, product_id: int) -> dict[str, Any] | None:
        """Get a product by ID."""
        products = self.get_products(active_only=False)
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
        return self._load_json_fallback("product_type.json")

    def get_product_tiers(self) -> list[dict[str, Any]]:
        """Get product tiers (Gold/Silver/Bronze/Basic)."""
        return self._load_json_fallback("product_tier.json")

    # =========================================================================
    # Benefit Methods
    # =========================================================================

    def get_benefit_categories(self) -> list[dict[str, Any]]:
        """Get benefit categories."""
        return self._query_table("benefit_category")

    def get_clinical_categories(self) -> list[dict[str, Any]]:
        """Get hospital clinical categories."""
        return self._query_table("clinical_category")

    def get_clinical_category_wp_mapping(self) -> dict[int, str]:
        """
        Build mapping from clinical_category_id to waiting_period_type.

        This maps hospital clinical categories to the specialized waiting period
        that applies to them:
        - Obstetric categories (pregnancy-related): blocked by Obstetric WP
        - Psychiatric category: blocked by Psychiatric WP
        - All others: only blocked by General WP (and probabilistically by Pre-existing)

        Returns:
            Dict mapping clinical_category_id -> "Obstetric" | "Psychiatric" | "General"
        """
        categories = self.get_clinical_categories()

        # Categories that map to specialized waiting periods (based on category_code)
        obstetric_codes = {"PREGNANCY", "ASSISTED_REPRO", "MISCARRIAGE_TERM"}
        psychiatric_codes = {"PSYCHIATRIC"}

        mapping = {}
        for cat in categories:
            cat_id = cat.get("clinical_category_id")
            code = cat.get("category_code", "")

            if code in obstetric_codes:
                mapping[cat_id] = "Obstetric"
            elif code in psychiatric_codes:
                mapping[cat_id] = "Psychiatric"
            else:
                mapping[cat_id] = "General"

        return mapping

    def get_product_benefits(self, product_id: int) -> list[dict[str, Any]]:
        """Get benefits for a specific product."""
        benefits = self._load_json_fallback("product_benefit.json")
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
        rules = self._load_json_fallback("waiting_period_rule.json")
        return [
            r for r in rules
            if r.get("product_id") == product_id and r.get("is_active", True)
        ]

    # =========================================================================
    # Location Methods
    # =========================================================================

    def get_states(self) -> list[dict[str, Any]]:
        """Get Australian states/territories."""
        return self._query_table("state_territory")

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
        providers = self._query_table("provider")
        if active_only:
            return [p for p in providers if p.get("status") == "Active"]
        return providers

    def get_providers_by_type_and_state(
        self,
        provider_type: str | None = None,
        state: str | None = None,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Get providers filtered by type and/or state.

        Args:
            provider_type: Provider type (e.g., "GP", "Specialist", "Dentist")
            state: State code (e.g., "NSW", "VIC")
            active_only: If True, return only active providers

        Returns:
            List of matching providers
        """
        providers = self.get_providers(active_only=active_only)

        if provider_type:
            providers = [
                p for p in providers
                if p.get("provider_type", "").lower() == provider_type.lower()
            ]

        if state:
            providers = [
                p for p in providers
                if p.get("state", "").upper() == state.upper()
            ]

        return providers

    def get_hospitals(self, active_only: bool = True) -> list[dict[str, Any]]:
        """Get hospitals."""
        hospitals = self._query_table("hospital")
        if active_only:
            return [h for h in hospitals if h.get("is_active", True)]
        return hospitals

    def get_hospitals_by_state(
        self,
        state: str | None = None,
        has_icu: bool | None = None,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Get hospitals filtered by state and/or ICU availability.

        Args:
            state: State code (e.g., "NSW", "VIC")
            has_icu: If True, only hospitals with ICU
            active_only: If True, return only active hospitals

        Returns:
            List of matching hospitals
        """
        hospitals = self.get_hospitals(active_only=active_only)

        if state:
            hospitals = [
                h for h in hospitals
                if h.get("state", "").upper() == state.upper()
            ]

        if has_icu is not None:
            hospitals = [
                h for h in hospitals
                if h.get("has_icu", False) == has_icu
            ]

        return hospitals

    # =========================================================================
    # Claims Methods
    # =========================================================================

    def get_mbs_categories(self) -> list[dict[str, Any]]:
        """Get MBS categories."""
        try:
            return self._load_json_fallback("mbs_category.json")
        except FileNotFoundError:
            return []

    def get_mbs_items(self, category_id: int | None = None) -> list[dict[str, Any]]:
        """
        Get MBS (Medical Benefits Schedule) items.

        Args:
            category_id: Optional category to filter by

        Returns:
            List of MBS items
        """
        items = self._query_table("mbs_item")
        if category_id is not None:
            return [i for i in items if i.get("category_id") == category_id]
        return items

    def get_extras_items(self, service_type_id: int | None = None) -> list[dict[str, Any]]:
        """
        Get extras item codes (dental, optical, etc.).

        Args:
            service_type_id: Optional service type to filter by

        Returns:
            List of extras items
        """
        items = self._query_table("extras_item_code")
        if service_type_id is not None:
            return [i for i in items if i.get("service_type_id") == service_type_id]
        return items

    def get_extras_items_by_service_type(
        self,
        service_type: str,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Get extras item codes filtered by service type name.

        Args:
            service_type: Service type name (e.g., "Dental", "Optical", "Physiotherapy")
            active_only: If True, return only active items

        Returns:
            List of matching extras items
        """
        # Map service type names to service_type_id
        service_type_map = {
            "Dental": 1,
            "Optical": 2,
            "Physiotherapy": 3,
            "Chiropractic": 4,
            "Podiatry": 5,
            "Psychology": 6,
            "Massage": 7,
            "Acupuncture": 8,
            "Natural Therapies": 9,
            "Osteopathy": 10,
            "Speech Pathology": 11,
            "Dietetics": 12,
            "Occupational Therapy": 13,
        }

        service_type_id = service_type_map.get(service_type)
        if service_type_id is None:
            # Try case-insensitive match
            for name, type_id in service_type_map.items():
                if name.lower() == service_type.lower():
                    service_type_id = type_id
                    break

        if service_type_id is None:
            return []

        items = self.get_extras_items(service_type_id=service_type_id)
        if active_only:
            items = [i for i in items if i.get("is_active", True)]
        return items

    def get_drg_codes(self) -> list[dict[str, Any]]:
        """Get DRG (Diagnosis Related Group) codes."""
        return self._load_json_fallback("drg_code.json")

    def get_claim_rejection_reasons(self) -> list[dict[str, Any]]:
        """Get claim rejection reasons."""
        return self._query_table("claim_rejection_reason")

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
            rates = self._load_json_fallback("premium_rate.json")
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
            tiers = self._load_json_fallback("phi_rebate_tier.json")
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
        return self._load_json_fallback("excess_option.json")

    # =========================================================================
    # Prosthesis Methods
    # =========================================================================

    def get_prosthesis_categories(self) -> list[dict[str, Any]]:
        """Get prosthesis categories."""
        try:
            return self._load_json_fallback("prosthesis_category.json")
        except FileNotFoundError:
            return []

    def get_prosthesis_items(self, category_id: int | None = None) -> list[dict[str, Any]]:
        """Get prosthesis items."""
        items = self._query_table("prosthesis_list_item")

        if category_id is not None:
            items = [i for i in items if i.get("prosthesis_category_id") == category_id]

        return items

    # =========================================================================
    # Benefit Limit Methods
    # =========================================================================

    def get_benefit_limits(self) -> list[dict[str, Any]]:
        """Get all benefit limits."""
        try:
            return self._load_json_fallback("benefit_limit.json")
        except FileNotFoundError:
            return []

    def get_benefit_limit_periods(self) -> list[dict[str, Any]]:
        """Get benefit limit period definitions."""
        try:
            return self._load_json_fallback("benefit_limit_period.json")
        except FileNotFoundError:
            return []

    def build_benefit_limit_lookup(self) -> dict[tuple[int, int], dict[str, Any]]:
        """
        Build a lookup dictionary for benefit limits.

        Returns:
            Dict mapping (product_id, benefit_category_id) -> limit info
        """
        limits = self.get_benefit_limits()
        product_benefits = self._load_json_fallback("product_benefit.json")

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

    # =========================================================================
    # CRM/Interaction Methods
    # =========================================================================

    def get_interaction_types(self) -> list[dict[str, Any]]:
        """Get all interaction types."""
        return self._query_table("interaction_type")

    def get_interaction_type_by_code(self, code: str) -> dict[str, Any] | None:
        """Get an interaction type by code."""
        types = self.get_interaction_types()
        return next((t for t in types if t.get("type_code") == code), None)

    def get_interaction_outcomes(self) -> list[dict[str, Any]]:
        """Get all interaction outcomes."""
        return self._query_table("interaction_outcome")

    def get_case_types(self) -> list[dict[str, Any]]:
        """Get all case types."""
        return self._query_table("case_type")

    def get_case_type_by_code(self, code: str) -> dict[str, Any] | None:
        """Get a case type by code."""
        types = self.get_case_types()
        return next((t for t in types if t.get("type_code") == code), None)

    def get_complaint_categories(self) -> list[dict[str, Any]]:
        """Get all complaint categories."""
        return self._query_table("complaint_category")

    # =========================================================================
    # Communication Methods
    # =========================================================================

    def get_communication_templates(self) -> list[dict[str, Any]]:
        """Get all communication templates."""
        return self._query_table("communication_template")

    def get_communication_template_by_trigger(self, trigger: str) -> dict[str, Any] | None:
        """
        Get a communication template by trigger event.

        Args:
            trigger: Trigger event type (e.g., "ClaimPaid", "InvoiceIssued")

        Returns:
            Template dict or None
        """
        templates = self.get_communication_templates()
        return next((t for t in templates if t.get("trigger_event") == trigger), None)

    def get_communication_template_by_code(self, code: str) -> dict[str, Any] | None:
        """Get a communication template by code."""
        templates = self.get_communication_templates()
        return next((t for t in templates if t.get("template_code") == code), None)

    # =========================================================================
    # Campaign Methods
    # =========================================================================

    def get_campaign_types(self) -> list[dict[str, Any]]:
        """Get all campaign types."""
        try:
            return self._load_json_fallback("campaign_type.json")
        except FileNotFoundError:
            logger.warning("campaign_type.json not found, returning empty list")
            return []

    def get_campaign_type_by_code(self, code: str) -> dict[str, Any] | None:
        """Get a campaign type by code."""
        types = self.get_campaign_types()
        return next((t for t in types if t.get("type_code") == code), None)

    # =========================================================================
    # Survey Methods
    # =========================================================================

    def get_survey_types(self) -> list[dict[str, Any]]:
        """Get all survey types."""
        try:
            return self._load_json_fallback("survey_type.json")
        except FileNotFoundError:
            logger.warning("survey_type.json not found, returning empty list")
            return []

    def get_survey_type_by_code(self, code: str) -> dict[str, Any] | None:
        """Get a survey type by code."""
        types = self.get_survey_types()
        return next((t for t in types if t.get("type_code") == code), None)

    def get_survey_type_by_trigger(self, trigger: str) -> dict[str, Any] | None:
        """
        Get a survey type by trigger event.

        Args:
            trigger: Trigger event type (e.g., "ClaimPaid", "InteractionCompleted")

        Returns:
            Survey type dict or None
        """
        types = self.get_survey_types()
        return next((t for t in types if t.get("trigger_event") == trigger), None)

    def get_survey_type_by_trigger_event(self, trigger_event: str) -> dict[str, Any] | None:
        """
        Get a survey type by trigger event with normalized matching.

        Normalizes the trigger event for comparison by converting to lowercase
        and removing underscores.

        Args:
            trigger_event: Trigger event type (e.g., "ClaimPaid", "claim_paid", "CLAIM_PAID")

        Returns:
            Matching survey type dict or None
        """
        types = self.get_survey_types()

        # Normalize input: lowercase and remove underscores
        normalized_input = trigger_event.lower().replace("_", "")

        for t in types:
            trigger = t.get("trigger_event")
            if trigger is None:
                continue
            # Normalize the stored trigger event
            normalized_trigger = trigger.lower().replace("_", "")
            if normalized_trigger == normalized_input:
                return t

        return None

    # =========================================================================
    # Product Tier Methods
    # =========================================================================

    def get_product_tier_by_name(self, tier_name: str) -> dict[str, Any] | None:
        """
        Get a product tier by name.

        Args:
            tier_name: Tier name (e.g., "Gold", "Silver", "Bronze", "Basic")
                       Case-insensitive matching.

        Returns:
            Matching tier dict or None
        """
        tiers = self._load_json_fallback("product_tier.json")
        tier_name_lower = tier_name.lower()
        return next(
            (t for t in tiers if t.get("tier_name", "").lower() == tier_name_lower),
            None
        )

    def get_product_tier_order(self) -> list[dict[str, Any]]:
        """
        Get product tiers sorted by tier_level ascending.

        This gives upgrade order: Basic (level 4) -> Bronze (level 3) ->
        Silver (level 2) -> Gold (level 1).

        Returns:
            List of tier dicts sorted by tier_level ascending (highest level number first)
        """
        tiers = self._load_json_fallback("product_tier.json")
        return sorted(tiers, key=lambda t: t.get("tier_level", 0), reverse=True)

    # =========================================================================
    # Excess Methods (Extended)
    # =========================================================================

    def get_default_excess_for_product(self, product_id: int) -> dict[str, Any] | None:
        """
        Get the default active excess option for a product.

        Args:
            product_id: Product ID to get default excess for

        Returns:
            Default excess option dict or None if not found
        """
        options = self.get_excess_options()
        return next(
            (
                opt for opt in options
                if opt.get("product_id") == product_id
                and opt.get("is_default", False) is True
                and opt.get("is_active", True) is True
            ),
            None
        )

    # =========================================================================
    # Campaign Methods (Extended)
    # =========================================================================

    def get_campaign_type_distribution(self) -> dict[str, float]:
        """
        Get campaign type distribution weights as a dict.

        Builds a mapping from type_code (converted to title case) to distribution_weight.
        Only includes active campaign types.

        Returns:
            Dict mapping campaign type name (e.g., "Retention") to distribution weight
        """
        types = self.get_campaign_types()

        result = {}
        for t in types:
            if not t.get("is_active", True):
                continue

            type_code = t.get("type_code", "")
            weight = t.get("distribution_weight", 0.0)

            # Convert type_code to title case (e.g., "RETENTION" -> "Retention")
            # Handle underscores by splitting, title-casing, and rejoining
            name = " ".join(word.capitalize() for word in type_code.split("_"))
            # Simplify to single word if no spaces (e.g., "RETENTION" -> "Retention")
            if " " not in type_code:
                name = type_code.capitalize()

            result[name] = float(weight)

        return result

    # =========================================================================
    # Communication Template Methods (Extended)
    # =========================================================================

    def get_communication_template_by_trigger_normalized(
        self, trigger_event: str
    ) -> dict[str, Any] | None:
        """
        Get a communication template by trigger event with normalized matching.

        Normalizes the trigger event for comparison by converting to lowercase
        and removing underscores.

        Args:
            trigger_event: Trigger event type (e.g., "ClaimPaid", "claim_paid", "CLAIM_PAID")

        Returns:
            Template dict or None
        """
        templates = self.get_communication_templates()

        # Normalize input: lowercase and remove underscores
        normalized_input = trigger_event.lower().replace("_", "")

        for t in templates:
            trigger = t.get("trigger_event")
            if trigger is None:
                continue
            # Normalize the stored trigger event
            normalized_trigger = trigger.lower().replace("_", "")
            if normalized_trigger == normalized_input:
                return t

        return None
