"""
LLM Context Builder for Brickwell Health Simulator.

Builds JSON context for deferred LLM processing of surveys.
"""

from datetime import date, datetime
from typing import Any, Optional


class LLMContextBuilder:
    """
    Builds LLM context for survey generation.

    Creates JSON-serializable context dictionaries that contain all
    the information needed for post-simulation LLM processing.
    """

    def __init__(self, config: Optional[dict] = None):
        """
        Initialize the context builder.

        Args:
            config: LLM configuration with history limits
        """
        self.config = config or {}
        self.max_claims = config.get("max_claims_history", 5) if config else 5
        self.max_interactions = config.get("max_interaction_history", 3) if config else 3
        self.claims_months = config.get("claims_history_months", 12) if config else 12
        self.interaction_months = config.get("interaction_history_months", 6) if config else 6

    def build_nps_context(
        self,
        member_data: dict,
        policy_data: dict,
        trigger_event: str,
        trigger_entity: Optional[dict],
        claims_history: Optional[list[dict]] = None,
        interaction_history: Optional[list[dict]] = None,
        billing_status: Optional[dict] = None,
        digital_engagement: Optional[dict] = None,
        simulation_date: Optional[date] = None,
        coverages: Optional[list[Any]] = None,
        active_policies: Optional[dict] = None,
        policy_id: Optional[Any] = None,
        reference: Optional[Any] = None,
    ) -> dict:
        """
        Build LLM context for NPS survey.

        Returns dict that will be stored as JSONB in pending table.

        Args:
            member_data: Member data dictionary with "member" key
            policy_data: Policy data dictionary with "policy" key
            trigger_event: Event that triggered the survey
            trigger_entity: Details of the trigger entity (claim, etc.)
            claims_history: List of recent claims
            interaction_history: List of recent interactions
            billing_status: Billing/payment status
            digital_engagement: Digital engagement metrics
            simulation_date: Current simulation date

        Returns:
            JSON-serializable context dictionary
        """
        member = member_data.get("member") if member_data else None
        policy = policy_data.get("policy") if policy_data else None

        # Get coverage tier from active_policies if available (more reliable than policy object)
        coverage_tier = None
        if active_policies and policy_id:
            policy_data_dict = active_policies.get(policy_id)
            if policy_data_dict:
                coverage_tier = policy_data_dict.get("tier")
        
        # Fallback to policy object if not in active_policies
        if not coverage_tier:
            coverage_tier = self._safe_value(policy, "tier")

        # Get hospital and extras cover names from coverage objects
        hospital_cover, extras_cover = self._get_product_names_from_coverages(
            coverages, reference, policy
        )

        context = {
            # Member Profile
            "member_name": self._get_member_name(member),
            "member_age": self._calculate_age(
                member.date_of_birth if member and hasattr(member, "date_of_birth") else None,
                simulation_date,
            ),
            "tenure_months": self._calculate_tenure(
                policy.start_date if policy and hasattr(policy, "start_date") else None,
                simulation_date,
            ),
            "state": member.state if member and hasattr(member, "state") else "NSW",
            # Policy Info
            "policy_type": self._safe_value(policy, "policy_type"),
            "coverage_tier": coverage_tier,
            "hospital_cover": hospital_cover,
            "extras_cover": extras_cover,
            "premium_monthly": self._safe_float(policy, "premium_amount"),
            # Survey Context
            "survey_date": simulation_date.isoformat() if simulation_date else None,
            "trigger_event": trigger_event,
            # Current Trigger Entity
            "current_trigger": self._format_trigger_entity(trigger_entity) if trigger_entity else None,
            # Raw Claim History (LLM infers patterns)
            # Note: During simulation, this will be empty array. Post-simulation enrichment
            # will replace it with a summary object containing aggregate counts and amounts.
            "claim_history": [
                self._format_claim(c) for c in (claims_history or [])[:self.max_claims]
            ],
            # Raw Interaction History
            # Note: During simulation, this will be empty array. Post-simulation enrichment
            # will replace it with a summary object containing aggregate counts and durations.
            "interaction_history": [
                self._format_interaction(i)
                for i in (interaction_history or [])[:self.max_interactions]
            ],
            # Billing Status
            "billing_status": billing_status or {},
            # Digital Engagement
            "digital_engagement": digital_engagement or {},
        }

        return context

    def build_csat_context(
        self,
        member_data: dict,
        policy_data: dict,
        interaction_data: dict,
        case_data: Optional[dict] = None,
        simulation_date: Optional[date] = None,
    ) -> dict:
        """
        Build LLM context for CSAT survey.

        Args:
            member_data: Member data dictionary
            policy_data: Policy data dictionary
            interaction_data: Interaction that triggered the survey
            case_data: Optional case data if survey is for case resolution
            simulation_date: Current simulation date

        Returns:
            JSON-serializable context dictionary
        """
        member = member_data.get("member") if member_data else None
        policy = policy_data.get("policy") if policy_data else None

        context = {
            # Member Profile
            "member_name": self._get_member_name(member),
            "member_age": self._calculate_age(
                member.date_of_birth if member and hasattr(member, "date_of_birth") else None,
                simulation_date,
            ),
            "tenure_months": self._calculate_tenure(
                policy.start_date if policy and hasattr(policy, "start_date") else None,
                simulation_date,
            ),
            # Interaction Details
            "interaction_type": interaction_data.get("type") or interaction_data.get("interaction_type"),
            "channel": interaction_data.get("channel"),
            "duration_minutes": (interaction_data.get("duration_seconds", 0) or 0) // 60,
            "wait_time_minutes": (interaction_data.get("wait_time_seconds", 0) or 0) // 60
            if interaction_data.get("wait_time_seconds")
            else None,
            "first_contact_resolution": interaction_data.get("fcr", False)
            or interaction_data.get("first_contact_resolution", False),
            "related_to": interaction_data.get("trigger_event_type"),
            # Case Details (if applicable)
            "case_resolved": case_data.get("resolved") if case_data else None,
            "sla_breached": case_data.get("sla_breached") if case_data else None,
            "case_type": case_data.get("case_type") if case_data else None,
            # Survey Date
            "survey_date": simulation_date.isoformat() if simulation_date else None,
        }

        return context

    def _get_product_names_from_coverages(
        self,
        coverages: Optional[list[Any]],
        reference: Optional[Any],
        policy: Any,
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Get hospital and extras product names from coverage objects.

        Args:
            coverages: List of coverage objects with product_id
            reference: Reference data loader for product lookups
            policy: Policy object for fallback

        Returns:
            Tuple of (hospital_cover_name, extras_cover_name)
        """
        hospital_cover = None
        extras_cover = None

        if coverages and reference:
            for coverage in coverages:
                if not hasattr(coverage, "product_id"):
                    continue
                product = reference.get_product_by_id(coverage.product_id) if reference else None
                if product:
                    product_name = product.get("product_name")
                    # Determine if hospital or extras based on product type
                    if product.get("is_hospital") or product.get("product_type_id") == 1:
                        hospital_cover = product_name
                    elif product.get("is_extras") or product.get("product_type_id") == 2:
                        extras_cover = product_name

        # Fallback to policy object if coverage lookup didn't work
        if not hospital_cover:
            hospital_cover = self._safe_value(policy, "hospital_product_name")
        if not extras_cover:
            extras_cover = self._safe_value(policy, "extras_product_name")

        return hospital_cover, extras_cover

    def _get_member_name(self, member: Any) -> str:
        """Get member name safely."""
        if not member:
            return "Member"
        first_name = getattr(member, "first_name", "") or ""
        surname = getattr(member, "surname", "") or ""
        if first_name and surname:
            return f"{first_name} {surname}"
        return first_name or surname or "Member"

    def _calculate_age(self, dob: Optional[date], as_of: Optional[date]) -> int:
        """Calculate age from date of birth."""
        if not dob or not as_of:
            return 40  # Default

        age = as_of.year - dob.year
        if (as_of.month, as_of.day) < (dob.month, dob.day):
            age -= 1
        return max(0, age)

    def _calculate_tenure(self, start_date: Optional[date], as_of: Optional[date]) -> int:
        """Calculate tenure in months."""
        if not start_date or not as_of:
            return 12  # Default

        months = (as_of.year - start_date.year) * 12 + (as_of.month - start_date.month)
        return max(0, months)

    def _safe_value(self, obj: Any, attr: str) -> Any:
        """Safely get attribute value."""
        if not obj:
            return None
        return getattr(obj, attr, None)

    def _safe_float(self, obj: Any, attr: str) -> Optional[float]:
        """Safely get attribute as float."""
        if not obj:
            return None
        val = getattr(obj, attr, None)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                return None
        return None

    def _format_trigger_entity(self, entity: dict) -> dict:
        """Format trigger entity (claim, invoice, interaction, etc.) for LLM."""
        # Check if this is an interaction trigger (has interaction-specific fields)
        if entity.get("interaction_date") or entity.get("channel") or entity.get("type"):
            return {
                "interaction_date": self._format_date(entity.get("interaction_date") or entity.get("date")),
                "type": entity.get("type") or entity.get("interaction_type"),
                "channel": entity.get("channel"),
                "duration_minutes": self._to_float(entity.get("duration_minutes")),
                "wait_time_minutes": self._to_float(entity.get("wait_time_minutes")),
                "resolved": entity.get("resolved") or entity.get("first_contact_resolution"),
                "related_to": entity.get("related_to") or entity.get("trigger_event_type"),
            }
        
        # Default to claim/invoice format
        return {
            "date": self._format_date(entity.get("service_date") or entity.get("date")),
            "service_type": entity.get("service_type"),
            "clinical_category": entity.get("clinical_category"),
            "charge_amount": self._to_float(entity.get("total_charge") or entity.get("charge_amount")),
            "benefit_paid": self._to_float(entity.get("benefit_paid")),
            "gap_amount": self._to_float(entity.get("gap_amount")),
            "status": entity.get("status"),
            "processing_days": entity.get("processing_days"),
            "rejection_reason": entity.get("rejection_reason"),
        }

    def _format_claim(self, claim: dict) -> dict:
        """Format a claim for LLM context."""
        return {
            "claim_date": self._format_date(claim.get("service_date")),
            "service_type": claim.get("service_type"),
            "clinical_category": claim.get("clinical_category"),
            "charge_amount": self._to_float(claim.get("total_charge")),
            "benefit_paid": self._to_float(claim.get("benefit_paid")),
            "status": claim.get("status"),
            "processing_days": claim.get("processing_days"),
            "rejection_reason": claim.get("rejection_reason"),
        }

    def _format_interaction(self, interaction: dict) -> dict:
        """Format an interaction for LLM context."""
        start_datetime = interaction.get("start_datetime")
        if isinstance(start_datetime, datetime):
            interaction_date = start_datetime.date().isoformat()
        elif isinstance(start_datetime, date):
            interaction_date = start_datetime.isoformat()
        elif isinstance(start_datetime, str):
            interaction_date = start_datetime[:10]
        else:
            interaction_date = None

        return {
            "interaction_date": interaction_date,
            "type": interaction.get("interaction_type") or interaction.get("type"),
            "channel": interaction.get("channel"),
            "duration_minutes": (interaction.get("duration_seconds", 0) or 0) // 60,
            "wait_time_minutes": (interaction.get("wait_time_seconds", 0) or 0) // 60
            if interaction.get("wait_time_seconds")
            else None,
            "resolved": interaction.get("first_contact_resolution") or interaction.get("resolved"),
            "related_to": interaction.get("trigger_event_type"),
        }

    def _format_date(self, value: Any) -> Optional[str]:
        """Format a date value to ISO string."""
        if value is None:
            return None
        if isinstance(value, (date, datetime)):
            return value.isoformat() if isinstance(value, date) else value.date().isoformat()
        return str(value)[:10] if value else None

    def _to_float(self, value: Any) -> Optional[float]:
        """Convert value to float safely."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
