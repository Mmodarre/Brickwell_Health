"""
Statistical models for Brickwell Health Simulator.

Provides demographic distributions, claim propensity models, and income models.
"""

from brickwell_health.statistics.abs_demographics import ABSDemographics
from brickwell_health.statistics.distributions import (
    sample_from_distribution,
    sample_age_for_role,
    sample_partner_age,
    sample_num_children,
    sample_child_ages,
)
from brickwell_health.statistics.product_selection import ProductSelectionModel
from brickwell_health.statistics.claim_propensity import ClaimPropensityModel
from brickwell_health.statistics.income_model import IncomeModel
from brickwell_health.statistics.survey_models import (
    SurveyResponsePredictor,
    CRMStatisticalModels,
)
from brickwell_health.statistics.llm_context import LLMContextBuilder

__all__ = [
    "ABSDemographics",
    "sample_from_distribution",
    "sample_age_for_role",
    "sample_partner_age",
    "sample_num_children",
    "sample_child_ages",
    "ProductSelectionModel",
    "ClaimPropensityModel",
    "IncomeModel",
    # Survey Models
    "SurveyResponsePredictor",
    "CRMStatisticalModels",
    "LLMContextBuilder",
]
