"""
Churn prediction model for Brickwell Health Simulator.

Models policy cancellation probability based on member demographics, policy attributes,
claims history, and government incentive lock-in effects.

Based on Australian PHI research showing age-based churn patterns:
- Young adults (18-24): 22% annual churn (highest)
- Elderly (70-74): 3% annual churn (lowest)
- LHC and MLS create retention lock-in effects
"""

from datetime import date
from math import exp
from typing import TYPE_CHECKING

from numpy.random import Generator as RNG

if TYPE_CHECKING:
    from brickwell_health.config.models import SimulationConfig
    from brickwell_health.reference.loader import ReferenceDataLoader


class ChurnPredictionModel:
    """
    Predicts policy churn probability using age-based rates and logistic adjustments.

    The model combines:
    1. Age bracket base rates (research-validated)
    2. Log-odds adjustments for behavioral factors
    3. Retention multipliers for government incentives
    4. Seasonal effects (Q2 premium increase period)

    Usage:
        model = ChurnPredictionModel(rng, reference, config)
        prob = model.predict_churn_probability(member, policy, claims_history, current_date)
        reason = model.sample_cancellation_reason(policy_data)
    """

    # Age bracket boundaries: (min_age, max_age, group_name)
    AGE_GROUP_BOUNDARIES = [
        (0, 17, "0-17"),
        (18, 24, "18-24"),
        (25, 29, "25-29"),
        (30, 34, "30-34"),
        (35, 39, "35-39"),
        (40, 44, "40-44"),
        (45, 49, "45-49"),
        (50, 54, "50-54"),
        (55, 59, "55-59"),
        (60, 64, "60-64"),
        (65, 69, "65-69"),
        (70, 74, "70-74"),
        (75, 79, "75-79"),
        (80, 999, "80+"),
    ]

    # Annual churn rates by age bracket (research-validated)
    # Based on: Netherlands study (10x ratio young:elderly), Australian PHI data
    CHURN_BY_AGE_BRACKET = {
        "0-17": 0.22,   # Dependents follow primary holder
        "18-24": 0.22,  # 22% - highest churn, price sensitive
        "25-29": 0.18,  # 18%
        "30-34": 0.14,  # 14% - LHC effect kicks in at 31
        "35-39": 0.11,  # 11%
        "40-44": 0.09,  # 9%
        "45-49": 0.08,  # 8%
        "50-54": 0.07,  # 7%
        "55-59": 0.06,  # 6%
        "60-64": 0.05,  # 5%
        "65-69": 0.04,  # 4%
        "70-74": 0.03,  # 3% - lowest churn, high utilization
        "75-79": 0.03,  # 3%
        "80+": 0.04,    # 4% - slight uptick due to financial stress
    }

    # Retention multipliers (applied to churn probability)
    RETENTION_MULTIPLIERS = {
        "lhc_loading": 0.80,         # 20% reduction - LHC creates lock-in
        "mls_subject": 0.85,         # 15% reduction - avoid tax surcharge
        "tenure_10_plus": 0.80,      # 20% reduction - established relationship
        "premium_increase_q2": 1.15,  # 15% increase - April-June premium shock
    }

    # Log-odds adjustments for logistic model
    LOG_ODDS_ADJUSTMENTS = {
        "no_recent_claims": 0.10,     # No claims in 6 months - low perceived value
        "dissatisfied": 0.15,         # Claim denials or high OOP costs
        "recent_life_event": 0.25,    # Job loss, divorce, major income change
        "high_claims_value": -0.10,   # Claims >= 50% of premium - validated value
        "long_tenure": -0.08,         # 10+ years tenure - established relationship
    }

    # Base cancellation reason weights
    CANCELLATION_REASON_WEIGHTS = {
        "Price": 0.40,       # Premium too expensive
        "NoValue": 0.25,     # Not using coverage
        "Switching": 0.15,   # Moving to competitor
        "LifeEvent": 0.15,   # Job loss, divorce, etc.
        "Other": 0.05,       # Other reasons
    }

    def __init__(
        self,
        rng: RNG,
        reference: "ReferenceDataLoader | None" = None,
        config: "SimulationConfig | None" = None,
    ):
        """
        Initialize the churn prediction model.

        Args:
            rng: NumPy random number generator
            reference: Optional reference data loader
            config: Optional simulation configuration
        """
        self.rng = rng
        self.reference = reference
        self.config = config

    def _get_age_group(self, age: int) -> str:
        """
        Map age to bracket group name.

        Args:
            age: Age in years

        Returns:
            Age group string (e.g., '18-24', '65-69', '80+')
        """
        for min_age, max_age, group in self.AGE_GROUP_BOUNDARIES:
            if min_age <= age <= max_age:
                return group
        return "80+"  # Default for very old ages

    def get_base_churn_rate(self, age: int) -> float:
        """
        Get annual churn rate for an age.

        Args:
            age: Age in years

        Returns:
            Annual churn probability (0-1)
        """
        age_group = self._get_age_group(age)
        return self.CHURN_BY_AGE_BRACKET.get(age_group, 0.08)  # Default 8%

    def _calculate_log_odds_adjustments(
        self,
        policy_data: dict,
        claims_history: dict,
    ) -> float:
        """
        Calculate total log-odds adjustment based on policy conditions.

        Args:
            policy_data: Dictionary with policy attributes
            claims_history: Dictionary with claims history

        Returns:
            Total log-odds adjustment (can be positive or negative)
        """
        adjustment = 0.0

        # No recent claims (6 months) - perceived lack of value
        if self._has_no_recent_claims(claims_history):
            adjustment += self.LOG_ODDS_ADJUSTMENTS["no_recent_claims"]
            policy_data["no_recent_claims"] = True

        # Dissatisfaction from denials or high out-of-pocket
        if self._is_dissatisfied(claims_history):
            adjustment += self.LOG_ODDS_ADJUSTMENTS["dissatisfied"]
            policy_data["dissatisfied"] = True

        # Recent life event (sampled probabilistically)
        if self._has_life_event():
            adjustment += self.LOG_ODDS_ADJUSTMENTS["recent_life_event"]
            policy_data["has_life_event"] = True

        # High claims value - validates coverage
        if self._has_high_claims_value(policy_data, claims_history):
            adjustment += self.LOG_ODDS_ADJUSTMENTS["high_claims_value"]
            policy_data["high_claims_value"] = True

        # Long tenure - established relationship
        tenure_years = policy_data.get("tenure_years", 0)
        if tenure_years >= 10:
            adjustment += self.LOG_ODDS_ADJUSTMENTS["long_tenure"]
            policy_data["long_tenure"] = True

        return adjustment

    def _apply_retention_multipliers(
        self,
        base_prob: float,
        policy_data: dict,
        current_date: date,
    ) -> float:
        """
        Apply retention multipliers to base probability.

        Args:
            base_prob: Base churn probability
            policy_data: Dictionary with policy attributes
            current_date: Current simulation date

        Returns:
            Adjusted churn probability
        """
        prob = base_prob

        # LHC loading creates lock-in (reduces churn)
        if policy_data.get("has_lhc_loading", False):
            prob *= self.RETENTION_MULTIPLIERS["lhc_loading"]

        # MLS subject - avoid tax surcharge (reduces churn)
        if policy_data.get("mls_subject", False):
            prob *= self.RETENTION_MULTIPLIERS["mls_subject"]

        # Long tenure creates lock-in
        # Note: Already applied in log-odds, but multiplier stacks
        tenure_years = policy_data.get("tenure_years", 0)
        if tenure_years >= 10:
            prob *= self.RETENTION_MULTIPLIERS["tenure_10_plus"]

        # Q2 premium increase effect (April-June)
        if self._is_premium_increase_period(current_date):
            prob *= self.RETENTION_MULTIPLIERS["premium_increase_q2"]

        # Ensure probability stays in valid range
        return max(0.0, min(1.0, prob))

    def _has_no_recent_claims(self, claims_history: dict) -> bool:
        """
        Check if there are no claims in the recent period.

        Args:
            claims_history: Dictionary with claims data

        Returns:
            True if no claims in the configured period (default 6 months)
        """
        # Get config threshold or default to 6 months
        months_threshold = 6
        if self.config and hasattr(self.config, "churn"):
            months_threshold = getattr(self.config.churn, "no_claims_months", 6)

        # Check claims history
        last_claim_days = claims_history.get("days_since_last_claim")
        if last_claim_days is None:
            # No claim record found - treat as no claims
            return True

        return last_claim_days > (months_threshold * 30)

    def _is_dissatisfied(self, claims_history: dict) -> bool:
        """
        Check if member shows dissatisfaction indicators.

        Dissatisfaction is triggered by:
        - Claim denials/rejections
        - High out-of-pocket expenses

        Args:
            claims_history: Dictionary with claims data

        Returns:
            True if dissatisfaction indicators present
        """
        # Get denial threshold from config or default to 1
        denial_threshold = 1
        if self.config and hasattr(self.config, "churn"):
            denial_threshold = getattr(
                self.config.churn, "dissatisfaction_denial_threshold", 1
            )

        denial_count = claims_history.get("denial_count", 0)
        high_oop = claims_history.get("high_out_of_pocket", False)

        return denial_count >= denial_threshold or high_oop

    def _has_life_event(self) -> bool:
        """
        Sample whether a life event occurred.

        Life events include job loss, divorce, major income change.
        Sampled probabilistically based on annual rate converted to daily.

        Returns:
            True if life event occurred (probabilistic)
        """
        # Get annual probability from config or default to 8%
        annual_prob = 0.08
        if self.config and hasattr(self.config, "churn"):
            annual_prob = getattr(
                self.config.churn, "life_event_annual_probability", 0.08
            )

        # Convert annual to daily probability
        # P(daily) = 1 - (1 - P(annual))^(1/365)
        daily_prob = 1 - (1 - annual_prob) ** (1 / 365)

        return self.rng.random() < daily_prob

    def _has_high_claims_value(
        self,
        policy_data: dict,
        claims_history: dict,
    ) -> bool:
        """
        Check if claims-to-premium ratio indicates high value.

        Args:
            policy_data: Dictionary with policy attributes
            claims_history: Dictionary with claims data

        Returns:
            True if claims >= threshold of premium paid
        """
        # Get threshold from config or default to 50%
        threshold = 0.50
        if self.config and hasattr(self.config, "churn"):
            threshold = getattr(self.config.churn, "high_claims_threshold", 0.50)

        total_claims = claims_history.get("total_claims_amount", 0)
        annual_premium = policy_data.get("annual_premium", 0)

        if annual_premium <= 0:
            return False

        claims_ratio = total_claims / annual_premium
        return claims_ratio >= threshold

    def _is_premium_increase_period(self, current_date: date) -> bool:
        """
        Check if current date is in Q2 premium increase period (April-June).

        Premium increases take effect April 1 in Australia.

        Args:
            current_date: Current simulation date

        Returns:
            True if in April, May, or June
        """
        return current_date.month in (4, 5, 6)

    def predict_churn_probability(
        self,
        member_age: int,
        policy_data: dict,
        claims_history: dict,
        current_date: date,
    ) -> float:
        """
        Predict annual churn probability for a policy.

        Combines:
        1. Age-based base rate
        2. Log-odds adjustments converted to probability
        3. Retention multipliers

        Args:
            member_age: Primary member's age
            policy_data: Dictionary with policy attributes:
                - tenure_years: Years since policy start
                - annual_premium: Annual premium amount
                - has_lhc_loading: Whether LHC loading applies
                - mls_subject: Whether MLS applies
            claims_history: Dictionary with claims data:
                - days_since_last_claim: Days since last claim
                - denial_count: Number of claim denials
                - high_out_of_pocket: Whether OOP exceeded threshold
                - total_claims_amount: Total claims value in period
            current_date: Current simulation date

        Returns:
            Annual churn probability (0-1)
        """
        # Step 1: Get base rate from age bracket
        base_rate = self.get_base_churn_rate(member_age)

        # Step 2: Convert to log-odds for adjustments
        # log_odds = log(p / (1 - p))
        if base_rate <= 0:
            base_rate = 0.001
        if base_rate >= 1:
            base_rate = 0.999
        base_log_odds = _probability_to_log_odds(base_rate)

        # Step 3: Calculate and apply log-odds adjustments
        adjustments = self._calculate_log_odds_adjustments(policy_data, claims_history)
        adjusted_log_odds = base_log_odds + adjustments

        # Step 4: Convert back to probability
        adjusted_prob = _log_odds_to_probability(adjusted_log_odds)

        # Step 5: Apply retention multipliers
        final_prob = self._apply_retention_multipliers(
            adjusted_prob, policy_data, current_date
        )

        return final_prob

    def predict_daily_churn_probability(
        self,
        member_age: int,
        policy_data: dict,
        claims_history: dict,
        current_date: date,
    ) -> float:
        """
        Predict daily churn probability for use in event selection.

        Converts annual probability to daily.

        Args:
            Same as predict_churn_probability

        Returns:
            Daily churn probability (0-1)
        """
        annual_prob = self.predict_churn_probability(
            member_age, policy_data, claims_history, current_date
        )

        # Convert annual to daily: P(daily) = 1 - (1 - P(annual))^(1/365)
        daily_prob = 1 - (1 - annual_prob) ** (1 / 365)
        return daily_prob

    def sample_cancellation_reason(self, policy_data: dict) -> str:
        """
        Sample a cancellation reason based on policy conditions.

        Weights are adjusted based on detected conditions:
        - Life event detected → higher LifeEvent weight
        - No recent claims → higher NoValue weight
        - Dissatisfied → higher Price weight

        Args:
            policy_data: Dictionary with policy attributes and flags set
                        during probability calculation

        Returns:
            Cancellation reason string
        """
        # Start with base weights
        weights = dict(self.CANCELLATION_REASON_WEIGHTS)

        # Adjust weights based on detected conditions
        if policy_data.get("has_life_event", False):
            weights["LifeEvent"] = 0.60
            weights["Price"] = 0.20
            weights["NoValue"] = 0.10

        elif policy_data.get("no_recent_claims", False):
            weights["NoValue"] = 0.45
            weights["Price"] = 0.30
            weights["LifeEvent"] = 0.10

        elif policy_data.get("dissatisfied", False):
            weights["Price"] = 0.50
            weights["Switching"] = 0.25
            weights["NoValue"] = 0.15

        # Normalize weights
        total = sum(weights.values())
        probs = [w / total for w in weights.values()]
        reasons = list(weights.keys())

        # Sample reason
        idx = self.rng.choice(len(reasons), p=probs)
        return reasons[idx]


def _probability_to_log_odds(p: float) -> float:
    """
    Convert probability to log-odds.

    Args:
        p: Probability (0 < p < 1)

    Returns:
        Log-odds value
    """
    if p <= 0:
        p = 0.001
    if p >= 1:
        p = 0.999
    return float(__import__("math").log(p / (1 - p)))


def _log_odds_to_probability(log_odds: float) -> float:
    """
    Convert log-odds to probability using logistic function.

    Args:
        log_odds: Log-odds value

    Returns:
        Probability (0-1)
    """
    return 1 / (1 + exp(-log_odds))
