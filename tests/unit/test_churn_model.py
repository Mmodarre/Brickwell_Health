"""
Unit tests for ChurnPredictionModel with age-based churn and retention factors.
"""

from datetime import date

import numpy as np
import pytest

from brickwell_health.statistics.churn_model import (
    ChurnPredictionModel,
    _probability_to_log_odds,
    _log_odds_to_probability,
)


class TestAgeGroupMapping:
    """Tests for age group bracket mapping functionality."""

    def test_age_group_child(self, test_rng: np.random.Generator):
        """Child (0-17) maps to correct age bracket."""
        model = ChurnPredictionModel(test_rng)

        assert model._get_age_group(5) == "0-17"
        assert model._get_age_group(17) == "0-17"

    def test_age_group_young_adult(self, test_rng: np.random.Generator):
        """Young adult (18-24) maps to correct age bracket."""
        model = ChurnPredictionModel(test_rng)

        assert model._get_age_group(18) == "18-24"
        assert model._get_age_group(22) == "18-24"
        assert model._get_age_group(24) == "18-24"

    def test_age_group_late_twenties(self, test_rng: np.random.Generator):
        """Late twenties (25-29) maps to correct age bracket."""
        model = ChurnPredictionModel(test_rng)

        assert model._get_age_group(25) == "25-29"
        assert model._get_age_group(29) == "25-29"

    def test_age_group_thirties(self, test_rng: np.random.Generator):
        """Thirties (30-34, 35-39) map to correct age brackets."""
        model = ChurnPredictionModel(test_rng)

        assert model._get_age_group(30) == "30-34"
        assert model._get_age_group(34) == "30-34"
        assert model._get_age_group(35) == "35-39"
        assert model._get_age_group(39) == "35-39"

    def test_age_group_senior(self, test_rng: np.random.Generator):
        """Senior ages (65-69, 70-74) map to correct age brackets."""
        model = ChurnPredictionModel(test_rng)

        assert model._get_age_group(65) == "65-69"
        assert model._get_age_group(69) == "65-69"
        assert model._get_age_group(70) == "70-74"
        assert model._get_age_group(74) == "70-74"

    def test_age_group_elderly(self, test_rng: np.random.Generator):
        """Elderly (80+) maps to correct age bracket."""
        model = ChurnPredictionModel(test_rng)

        assert model._get_age_group(80) == "80+"
        assert model._get_age_group(95) == "80+"
        assert model._get_age_group(100) == "80+"


class TestBaseChurnRates:
    """Tests for base churn rate lookup by age bracket."""

    def test_churn_rate_young_adult_highest(self, test_rng: np.random.Generator):
        """Young adults (18-24) should have highest churn rate (22%)."""
        model = ChurnPredictionModel(test_rng)

        rate = model.get_base_churn_rate(20)

        assert rate == 0.22

    def test_churn_rate_decreases_with_age(self, test_rng: np.random.Generator):
        """Churn rate should generally decrease with age."""
        model = ChurnPredictionModel(test_rng)

        rate_20 = model.get_base_churn_rate(20)
        rate_35 = model.get_base_churn_rate(35)
        rate_50 = model.get_base_churn_rate(50)
        rate_65 = model.get_base_churn_rate(65)
        rate_72 = model.get_base_churn_rate(72)

        assert rate_20 > rate_35 > rate_50 > rate_65 > rate_72

    def test_churn_rate_lowest_at_70_74(self, test_rng: np.random.Generator):
        """70-74 age bracket should have lowest churn rate (3%)."""
        model = ChurnPredictionModel(test_rng)

        rate = model.get_base_churn_rate(72)

        assert rate == 0.03

    def test_churn_rate_uptick_at_80_plus(self, test_rng: np.random.Generator):
        """80+ should have slight uptick (4%) due to financial stress."""
        model = ChurnPredictionModel(test_rng)

        rate_75 = model.get_base_churn_rate(77)  # 75-79
        rate_80 = model.get_base_churn_rate(82)  # 80+

        assert rate_75 == 0.03
        assert rate_80 == 0.04
        assert rate_80 > rate_75

    def test_churn_rate_lhc_effect_visible(self, test_rng: np.random.Generator):
        """LHC effect at age 31 should show significant drop in churn."""
        model = ChurnPredictionModel(test_rng)

        rate_25_29 = model.get_base_churn_rate(27)  # 18%
        rate_30_34 = model.get_base_churn_rate(32)  # 14%

        assert rate_25_29 == 0.18
        assert rate_30_34 == 0.14
        assert rate_30_34 < rate_25_29


class TestLogOddsConversion:
    """Tests for probability to log-odds conversion functions."""

    def test_probability_to_log_odds_50_percent(self):
        """50% probability should give log-odds of 0."""
        log_odds = _probability_to_log_odds(0.5)
        assert abs(log_odds) < 0.001

    def test_probability_to_log_odds_high_prob(self):
        """High probability should give positive log-odds."""
        log_odds = _probability_to_log_odds(0.9)
        assert log_odds > 0

    def test_probability_to_log_odds_low_prob(self):
        """Low probability should give negative log-odds."""
        log_odds = _probability_to_log_odds(0.1)
        assert log_odds < 0

    def test_log_odds_to_probability_zero(self):
        """Zero log-odds should give 50% probability."""
        prob = _log_odds_to_probability(0)
        assert abs(prob - 0.5) < 0.001

    def test_roundtrip_conversion(self):
        """Converting probability -> log-odds -> probability should roundtrip."""
        original_prob = 0.22
        log_odds = _probability_to_log_odds(original_prob)
        recovered_prob = _log_odds_to_probability(log_odds)

        assert abs(recovered_prob - original_prob) < 0.0001


class TestRetentionMultipliers:
    """Tests for retention multiplier application."""

    def test_lhc_loading_reduces_churn(self, test_rng: np.random.Generator):
        """LHC loading should reduce churn by 20%."""
        model = ChurnPredictionModel(test_rng)

        base_prob = 0.10
        policy_data = {"has_lhc_loading": True}
        current_date = date(2024, 8, 15)  # Not Q2

        adjusted_prob = model._apply_retention_multipliers(
            base_prob, policy_data, current_date
        )

        assert adjusted_prob == pytest.approx(0.10 * 0.80, rel=0.01)

    def test_mls_subject_reduces_churn(self, test_rng: np.random.Generator):
        """MLS subject status should reduce churn by 15%."""
        model = ChurnPredictionModel(test_rng)

        base_prob = 0.10
        policy_data = {"mls_subject": True}
        current_date = date(2024, 8, 15)

        adjusted_prob = model._apply_retention_multipliers(
            base_prob, policy_data, current_date
        )

        assert adjusted_prob == pytest.approx(0.10 * 0.85, rel=0.01)

    def test_long_tenure_reduces_churn(self, test_rng: np.random.Generator):
        """10+ years tenure should reduce churn by 20%."""
        model = ChurnPredictionModel(test_rng)

        base_prob = 0.10
        policy_data = {"tenure_years": 12}
        current_date = date(2024, 8, 15)

        adjusted_prob = model._apply_retention_multipliers(
            base_prob, policy_data, current_date
        )

        assert adjusted_prob == pytest.approx(0.10 * 0.80, rel=0.01)

    def test_q2_increases_churn(self, test_rng: np.random.Generator):
        """Q2 (April-June) premium increase period should increase churn by 15%."""
        model = ChurnPredictionModel(test_rng)

        base_prob = 0.10
        policy_data = {}
        
        # Q2 dates (April-June)
        for month in [4, 5, 6]:
            current_date = date(2024, month, 15)
            adjusted_prob = model._apply_retention_multipliers(
                base_prob, policy_data, current_date
            )
            assert adjusted_prob == pytest.approx(0.10 * 1.15, rel=0.01)

    def test_non_q2_no_seasonal_effect(self, test_rng: np.random.Generator):
        """Non-Q2 months should have no seasonal effect."""
        model = ChurnPredictionModel(test_rng)

        base_prob = 0.10
        policy_data = {}
        
        # Non-Q2 dates
        for month in [1, 2, 3, 7, 8, 9, 10, 11, 12]:
            current_date = date(2024, month, 15)
            adjusted_prob = model._apply_retention_multipliers(
                base_prob, policy_data, current_date
            )
            assert adjusted_prob == pytest.approx(0.10, rel=0.01)

    def test_multipliers_stack(self, test_rng: np.random.Generator):
        """Multiple retention factors should stack multiplicatively."""
        model = ChurnPredictionModel(test_rng)

        base_prob = 0.20
        policy_data = {
            "has_lhc_loading": True,  # 0.80
            "mls_subject": True,       # 0.85
            "tenure_years": 15,        # 0.80
        }
        current_date = date(2024, 5, 15)  # Q2: 1.15

        adjusted_prob = model._apply_retention_multipliers(
            base_prob, policy_data, current_date
        )

        expected = 0.20 * 0.80 * 0.85 * 0.80 * 1.15
        assert adjusted_prob == pytest.approx(expected, rel=0.01)


class TestDissatisfactionDetection:
    """Tests for dissatisfaction indicator detection."""

    def test_no_dissatisfaction_by_default(self, test_rng: np.random.Generator):
        """No dissatisfaction when claims history is clean."""
        model = ChurnPredictionModel(test_rng)

        claims_history = {
            "denial_count": 0,
            "high_out_of_pocket": False,
        }

        assert model._is_dissatisfied(claims_history) is False

    def test_dissatisfaction_from_denials(self, test_rng: np.random.Generator):
        """Dissatisfaction should trigger from claim denials."""
        model = ChurnPredictionModel(test_rng)

        claims_history = {
            "denial_count": 1,
            "high_out_of_pocket": False,
        }

        assert model._is_dissatisfied(claims_history) is True

    def test_dissatisfaction_from_high_oop(self, test_rng: np.random.Generator):
        """Dissatisfaction should trigger from high out-of-pocket."""
        model = ChurnPredictionModel(test_rng)

        claims_history = {
            "denial_count": 0,
            "high_out_of_pocket": True,
        }

        assert model._is_dissatisfied(claims_history) is True


class TestNoRecentClaimsDetection:
    """Tests for no recent claims detection."""

    def test_no_claims_when_none_recorded(self, test_rng: np.random.Generator):
        """No recent claims when no claim record exists."""
        model = ChurnPredictionModel(test_rng)

        claims_history = {"days_since_last_claim": None}

        assert model._has_no_recent_claims(claims_history) is True

    def test_no_claims_after_threshold(self, test_rng: np.random.Generator):
        """No recent claims when beyond 6 month threshold."""
        model = ChurnPredictionModel(test_rng)

        # 200 days > 6 months (180 days)
        claims_history = {"days_since_last_claim": 200}

        assert model._has_no_recent_claims(claims_history) is True

    def test_has_claims_within_threshold(self, test_rng: np.random.Generator):
        """Recent claims when within 6 month threshold."""
        model = ChurnPredictionModel(test_rng)

        # 100 days < 6 months (180 days)
        claims_history = {"days_since_last_claim": 100}

        assert model._has_no_recent_claims(claims_history) is False


class TestHighClaimsValue:
    """Tests for high claims value detection."""

    def test_high_claims_above_threshold(self, test_rng: np.random.Generator):
        """High claims when ratio >= 50% of premium."""
        model = ChurnPredictionModel(test_rng)

        policy_data = {"annual_premium": 2400}
        claims_history = {"total_claims_amount": 1500}  # 62.5%

        assert model._has_high_claims_value(policy_data, claims_history) is True

    def test_not_high_claims_below_threshold(self, test_rng: np.random.Generator):
        """Not high claims when ratio < 50% of premium."""
        model = ChurnPredictionModel(test_rng)

        policy_data = {"annual_premium": 2400}
        claims_history = {"total_claims_amount": 500}  # 20.8%

        assert model._has_high_claims_value(policy_data, claims_history) is False

    def test_not_high_claims_zero_premium(self, test_rng: np.random.Generator):
        """Not high claims when premium is zero (avoids division by zero)."""
        model = ChurnPredictionModel(test_rng)

        policy_data = {"annual_premium": 0}
        claims_history = {"total_claims_amount": 1000}

        assert model._has_high_claims_value(policy_data, claims_history) is False


class TestPredictChurnProbability:
    """Tests for full churn probability prediction."""

    def test_young_adult_high_churn(self, test_rng: np.random.Generator):
        """Young adults should have high churn probability."""
        model = ChurnPredictionModel(test_rng)

        member_age = 22
        policy_data = {"tenure_years": 1, "annual_premium": 2400}
        claims_history = {}
        current_date = date(2024, 8, 15)

        prob = model.predict_churn_probability(
            member_age, policy_data, claims_history, current_date
        )

        # Should be around 22% or higher with adjustments
        assert 0.15 <= prob <= 0.40

    def test_elderly_low_churn(self, test_rng: np.random.Generator):
        """Elderly with good retention factors should have low churn."""
        model = ChurnPredictionModel(test_rng)

        member_age = 72
        policy_data = {
            "tenure_years": 15,
            "annual_premium": 3000,
            "has_lhc_loading": False,  # They've had 10+ years, loading removed
            "mls_subject": False,
        }
        claims_history = {
            "days_since_last_claim": 60,  # Recent claims
            "total_claims_amount": 2000,  # High value
            "denial_count": 0,
            "high_out_of_pocket": False,
        }
        current_date = date(2024, 8, 15)

        prob = model.predict_churn_probability(
            member_age, policy_data, claims_history, current_date
        )

        # Should be very low - base 3% with long tenure reduction
        assert prob < 0.05

    def test_daily_probability_much_lower(self, test_rng: np.random.Generator):
        """Daily probability should be much lower than annual."""
        model = ChurnPredictionModel(test_rng)

        member_age = 40
        policy_data = {"tenure_years": 5, "annual_premium": 2400}
        claims_history = {}
        current_date = date(2024, 8, 15)

        annual_prob = model.predict_churn_probability(
            member_age, policy_data, claims_history, current_date
        )
        daily_prob = model.predict_daily_churn_probability(
            member_age, policy_data, claims_history, current_date
        )

        # Daily should be roughly annual / 365
        assert daily_prob < annual_prob / 300
        assert daily_prob > annual_prob / 400


class TestCancellationReasonSampling:
    """Tests for cancellation reason sampling."""

    def test_samples_valid_reason(self, test_rng: np.random.Generator):
        """Should sample a valid cancellation reason."""
        model = ChurnPredictionModel(test_rng)

        reason = model.sample_cancellation_reason({})

        valid_reasons = ["Price", "NoValue", "Switching", "LifeEvent", "Other"]
        assert reason in valid_reasons

    def test_life_event_increases_life_event_reason(self, test_rng: np.random.Generator):
        """Life event flag should increase LifeEvent reason probability."""
        # Use fixed seed for reproducibility
        rng = np.random.default_rng(42)
        model = ChurnPredictionModel(rng)

        policy_data = {"has_life_event": True}

        # Sample many reasons
        n_samples = 1000
        reasons = [model.sample_cancellation_reason(policy_data) for _ in range(n_samples)]

        life_event_count = sum(1 for r in reasons if r == "LifeEvent")
        proportion = life_event_count / n_samples

        # Should be around 60% when life event detected
        assert proportion > 0.45

    def test_no_claims_increases_no_value_reason(self, test_rng: np.random.Generator):
        """No recent claims should increase NoValue reason probability."""
        # Use fixed seed for reproducibility
        rng = np.random.default_rng(42)
        model = ChurnPredictionModel(rng)

        policy_data = {"no_recent_claims": True}

        # Sample many reasons
        n_samples = 1000
        reasons = [model.sample_cancellation_reason(policy_data) for _ in range(n_samples)]

        no_value_count = sum(1 for r in reasons if r == "NoValue")
        proportion = no_value_count / n_samples

        # Should be around 45% when no recent claims
        assert proportion > 0.35

    def test_dissatisfied_increases_price_reason(self, test_rng: np.random.Generator):
        """Dissatisfaction should increase Price/Switching reasons."""
        # Use fixed seed for reproducibility
        rng = np.random.default_rng(42)
        model = ChurnPredictionModel(rng)

        policy_data = {"dissatisfied": True}

        # Sample many reasons
        n_samples = 1000
        reasons = [model.sample_cancellation_reason(policy_data) for _ in range(n_samples)]

        price_count = sum(1 for r in reasons if r == "Price")
        proportion = price_count / n_samples

        # Should be around 50% when dissatisfied
        assert proportion > 0.40


class TestPremiumIncreasePeriod:
    """Tests for Q2 premium increase period detection."""

    def test_april_is_q2(self, test_rng: np.random.Generator):
        """April should be detected as Q2."""
        model = ChurnPredictionModel(test_rng)
        assert model._is_premium_increase_period(date(2024, 4, 1)) is True
        assert model._is_premium_increase_period(date(2024, 4, 30)) is True

    def test_may_is_q2(self, test_rng: np.random.Generator):
        """May should be detected as Q2."""
        model = ChurnPredictionModel(test_rng)
        assert model._is_premium_increase_period(date(2024, 5, 15)) is True

    def test_june_is_q2(self, test_rng: np.random.Generator):
        """June should be detected as Q2."""
        model = ChurnPredictionModel(test_rng)
        assert model._is_premium_increase_period(date(2024, 6, 30)) is True

    def test_march_not_q2(self, test_rng: np.random.Generator):
        """March (before Q2) should not be detected as Q2."""
        model = ChurnPredictionModel(test_rng)
        assert model._is_premium_increase_period(date(2024, 3, 31)) is False

    def test_july_not_q2(self, test_rng: np.random.Generator):
        """July (after Q2) should not be detected as Q2."""
        model = ChurnPredictionModel(test_rng)
        assert model._is_premium_increase_period(date(2024, 7, 1)) is False
