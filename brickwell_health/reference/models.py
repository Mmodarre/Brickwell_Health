"""
Pydantic models for reference data validation.

These models can be used to validate reference data loaded from JSON files.
"""

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class Product(BaseModel):
    """Product reference data model."""

    product_id: int
    product_code: str
    product_name: str
    product_type_id: int
    product_tier_id: Optional[int] = None
    description: Optional[str] = None
    is_hospital: bool = False
    is_extras: bool = False
    is_ambulance: bool = False
    default_excess: Optional[Decimal] = None
    status: str = "Active"
    effective_date: Optional[date] = None
    end_date: Optional[date] = None
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    available_policy_types: Optional[str] = None
    is_community_rated: bool = True
    government_rebate_eligible: bool = True


class ProductType(BaseModel):
    """Product type reference data model."""

    product_type_id: int
    type_code: str
    type_name: str
    description: Optional[str] = None
    is_active: bool = True


class ProductTier(BaseModel):
    """Product tier reference data model."""

    product_tier_id: int
    tier_code: str
    tier_name: str
    tier_level: int
    description: Optional[str] = None
    min_clinical_categories: Optional[int] = None
    effective_date: Optional[date] = None
    is_active: bool = True


class StateTerritory(BaseModel):
    """State/territory reference data model."""

    state_territory_id: int
    state_code: str
    state_name: str
    has_ambulance_scheme: bool = False
    ambulance_scheme_name: Optional[str] = None
    is_active: bool = True


class BenefitCategory(BaseModel):
    """Benefit category reference data model."""

    benefit_category_id: int
    category_code: str
    category_name: str
    parent_category_id: Optional[int] = None
    category_type: str  # Extras or Hospital
    description: Optional[str] = None
    is_active: bool = True


class ClinicalCategory(BaseModel):
    """Clinical category reference data model."""

    clinical_category_id: int
    category_code: str
    category_name: str
    description: Optional[str] = None
    tier_requirement: Optional[str] = None  # Gold/Silver/Bronze/Basic
    is_active: bool = True


class WaitingPeriodRule(BaseModel):
    """Waiting period rule reference data model."""

    waiting_period_rule_id: int
    product_id: int
    waiting_period_type: str
    benefit_category_id: Optional[int] = None
    clinical_category_id: Optional[int] = None
    duration_months: int
    applies_to_upgrade: bool = False
    applies_to_new_member: bool = True
    is_active: bool = True


class ExcessOption(BaseModel):
    """Excess option reference data model."""

    excess_option_id: int
    excess_amount: Decimal
    description: Optional[str] = None
    is_active: bool = True


class Provider(BaseModel):
    """Healthcare provider reference data model."""

    provider_id: int
    provider_number: str
    provider_type_id: int
    provider_category: str  # Individual/Organisation
    title: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    practice_name: Optional[str] = None
    specialty_id: Optional[int] = None
    preferred_provider: bool = False
    status: str = "Active"


class Hospital(BaseModel):
    """Hospital reference data model."""

    hospital_id: int
    hospital_name: str
    hospital_type_id: int
    state_territory_id: int
    suburb: Optional[str] = None
    postcode: Optional[str] = None
    is_contracted: bool = False
    is_active: bool = True


class MBSItem(BaseModel):
    """MBS item reference data model."""

    mbs_item_id: int
    item_number: str
    item_description: str
    mbs_category_id: Optional[int] = None
    schedule_fee: Optional[Decimal] = None
    is_active: bool = True


class ExtrasItemCode(BaseModel):
    """Extras item code reference data model."""

    extras_item_id: int
    item_code: str
    item_description: str
    service_type_id: int
    typical_fee: Optional[Decimal] = None
    is_active: bool = True


class DRGCode(BaseModel):
    """DRG code reference data model."""

    drg_code_id: int
    drg_code: str
    drg_description: str
    mdc_code: Optional[str] = None
    avg_length_of_stay: Optional[Decimal] = None
    is_active: bool = True


class PHIRebateTier(BaseModel):
    """PHI rebate tier reference data model."""

    rebate_tier_id: int
    financial_year: str
    tier_number: int
    tier_name: str
    single_threshold_min: int
    single_threshold_max: Optional[int] = None
    family_threshold_min: int
    family_threshold_max: Optional[int] = None
    rebate_pct_under_65: Decimal
    rebate_pct_65_to_69: Decimal
    rebate_pct_70_plus: Decimal
    mls_percentage: Optional[Decimal] = None
    effective_date: date
    end_date: Optional[date] = None
    is_active: bool = True


class PremiumRate(BaseModel):
    """Premium rate reference data model."""

    premium_rate_id: int
    product_id: int
    state_territory_id: int
    policy_type: str
    age_bracket_min: Optional[int] = None
    age_bracket_max: Optional[int] = None
    excess_option_id: Optional[int] = None
    base_premium_monthly: Decimal
    base_premium_annual: Decimal
    hospital_component: Optional[Decimal] = None
    extras_component: Optional[Decimal] = None
    ambulance_component: Optional[Decimal] = None
    effective_date: date
    end_date: Optional[date] = None
    is_current: bool = True


class ClaimRejectionReason(BaseModel):
    """Claim rejection reason reference data model."""

    rejection_reason_id: int
    reason_code: str
    reason_description: str
    category: Optional[str] = None
    is_active: bool = True


class ProsthesisCategory(BaseModel):
    """Prosthesis category reference data model."""

    prosthesis_category_id: int
    category_code: str
    category_name: str
    description: Optional[str] = None
    body_system: Optional[str] = None
    average_benefit: Optional[Decimal] = None
    is_high_cost: bool = False
    requires_prior_approval: bool = False
    is_active: bool = True


class ProsthesisListItem(BaseModel):
    """Prosthesis list item reference data model."""

    prosthesis_item_id: int
    prosthesis_category_id: int
    billing_code: str
    item_name: str
    manufacturer: Optional[str] = None
    brand_name: Optional[str] = None
    minimum_benefit: Decimal
    maximum_benefit: Optional[Decimal] = None
    no_gap_benefit: Optional[Decimal] = None
    effective_date: date
    end_date: Optional[date] = None
    is_current: bool = True
