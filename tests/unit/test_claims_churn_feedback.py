"""
Unit tests for the claims-history-to-churn feedback loop.

Tests two components:
1. ClaimsProcess._update_policy_claims_stats() — writes claim outcomes to policy dict
2. PolicyLifecycleProcess._get_claims_history() — reads rolling 12-month metrics
"""

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch
from uuid import uuid4

import pytest

from brickwell_health.core.shared_state import SharedState

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def shared_state():
    """Create a fresh SharedState."""
    return SharedState()


@pytest.fixture
def policy_id():
    """A fixed policy UUID for testing."""
    return uuid4()


@pytest.fixture
def policy_in_state(shared_state, policy_id):
    """Register a minimal policy in SharedState and return the dict."""
    policy_data = {
        "status": "Active",
        "tier": "Gold",
        "product_id": 1,
        "excess": Decimal("500"),
    }
    shared_state.active_policies[policy_id] = policy_data
    return policy_data


@pytest.fixture
def mock_claims_process(test_rng, test_config, sim_env, shared_state):
    """Create a ClaimsProcess with patched __init__ for unit testing."""
    from brickwell_health.core.processes.claims import ClaimsProcess

    with patch.object(ClaimsProcess, "__init__", lambda self, *args, **kwargs: None):
        process = ClaimsProcess()
        process.rng = test_rng
        process.config = test_config
        process.sim_env = sim_env
        process.shared_state = shared_state
        return process


@pytest.fixture
def mock_lifecycle_process(test_rng, test_config, sim_env, shared_state):
    """Create a PolicyLifecycleProcess with patched __init__ for unit testing."""
    from brickwell_health.core.processes.policy_lifecycle import PolicyLifecycleProcess

    with patch.object(
        PolicyLifecycleProcess, "__init__", lambda self, *args, **kwargs: None
    ):
        process = PolicyLifecycleProcess()
        process.rng = test_rng
        process.config = test_config
        process.sim_env = sim_env
        process.shared_state = shared_state
        return process


# =============================================================================
# Tests: _update_policy_claims_stats (ClaimsProcess write-back)
# =============================================================================


class TestUpdatePolicyClaimsStatsPaid:
    """Tests for PAID claim write-back to policy dict."""

    def test_paid_sets_last_claim_date(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """PAID claim should set last_claim_date on the policy dict."""
        data = {
            "benefit_amount": Decimal("150.00"),
            "claim_total_gap": Decimal("30.00"),
            "claim_total_benefit": Decimal("150.00"),
        }
        claim_date = date(2024, 6, 15)

        mock_claims_process._update_policy_claims_stats(
            policy_id, data, "PAID", claim_date
        )

        assert policy_in_state["last_claim_date"] == claim_date

    def test_paid_appends_to_paid_claim_log(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """PAID claim should append (date, benefit, gap) tuple to paid_claim_log."""
        data = {
            "benefit_amount": Decimal("200.00"),
            "claim_total_gap": Decimal("50.00"),
            "claim_total_benefit": Decimal("200.00"),
        }
        claim_date = date(2024, 6, 15)

        mock_claims_process._update_policy_claims_stats(
            policy_id, data, "PAID", claim_date
        )

        log = policy_in_state["paid_claim_log"]
        assert len(log) == 1
        assert log[0] == (claim_date, 200.0, 50.0)

    def test_paid_accumulates_multiple_claims(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """Multiple PAID claims should accumulate in paid_claim_log."""
        for i in range(3):
            data = {
                "benefit_amount": Decimal("100.00"),
                "claim_total_gap": Decimal("20.00"),
                "claim_total_benefit": Decimal("100.00"),
            }
            mock_claims_process._update_policy_claims_stats(
                policy_id, data, "PAID", date(2024, 3 + i, 1)
            )

        assert len(policy_in_state["paid_claim_log"]) == 3

    def test_paid_extras_gap_uses_capping_info(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """Extras claims with capping should use original_gap + additional_gap."""
        data = {
            "benefit_amount": Decimal("80.00"),
            "original_gap": Decimal("25.00"),
            "additional_gap": Decimal("15.00"),
            "claim_total_gap": Decimal("10.00"),  # Should be ignored
            "claim_total_benefit": Decimal("80.00"),
        }

        mock_claims_process._update_policy_claims_stats(
            policy_id, data, "PAID", date(2024, 6, 15)
        )

        log = policy_in_state["paid_claim_log"]
        assert log[0][2] == 40.0  # 25 + 15, not 10

    def test_paid_hospital_gap_uses_claim_total_gap(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """Hospital claims without capping info should use claim_total_gap."""
        data = {
            "benefit_amount": Decimal("3000.00"),
            "claim_total_gap": Decimal("500.00"),
            "claim_total_benefit": Decimal("3000.00"),
            # No original_gap/additional_gap (hospital claim)
        }

        mock_claims_process._update_policy_claims_stats(
            policy_id, data, "PAID", date(2024, 6, 15)
        )

        log = policy_in_state["paid_claim_log"]
        assert log[0][2] == 500.0


class TestUpdatePolicyClaimsStatsRejected:
    """Tests for REJECTED claim write-back to policy dict."""

    def test_rejected_appends_to_denial_log(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """REJECTED claim should append date to denial_log."""
        data = {"benefit_amount": Decimal("0")}
        denial_date = date(2024, 7, 20)

        mock_claims_process._update_policy_claims_stats(
            policy_id, data, "REJECTED", denial_date
        )

        assert policy_in_state["denial_log"] == [denial_date]

    def test_rejected_accumulates_denials(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """Multiple rejections should accumulate in denial_log."""
        for i in range(3):
            mock_claims_process._update_policy_claims_stats(
                policy_id, {}, "REJECTED", date(2024, 4 + i, 1)
            )

        assert len(policy_in_state["denial_log"]) == 3

    def test_rejected_does_not_affect_paid_log(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """REJECTED claims should not modify paid_claim_log or last_claim_date."""
        mock_claims_process._update_policy_claims_stats(
            policy_id, {}, "REJECTED", date(2024, 6, 15)
        )

        assert "paid_claim_log" not in policy_in_state
        assert "last_claim_date" not in policy_in_state


class TestUpdatePolicyClaimsStatsEdgeCases:
    """Tests for edge cases in write-back."""

    def test_noop_when_policy_missing(self, mock_claims_process, shared_state):
        """Write-back should be a no-op if policy was removed from SharedState."""
        missing_id = uuid4()
        # Should not raise
        mock_claims_process._update_policy_claims_stats(
            missing_id, {"benefit_amount": Decimal("100")}, "PAID", date(2024, 6, 15)
        )

    def test_noop_when_shared_state_none(self, mock_claims_process, policy_id):
        """Write-back should be a no-op if shared_state is None."""
        mock_claims_process.shared_state = None
        # Should not raise
        mock_claims_process._update_policy_claims_stats(
            policy_id, {}, "PAID", date(2024, 6, 15)
        )

    def test_paid_with_none_amounts(
        self, mock_claims_process, shared_state, policy_id, policy_in_state
    ):
        """PAID with None benefit/gap should default to 0."""
        data = {
            "benefit_amount": None,
            "claim_total_gap": None,
            "claim_total_benefit": None,
        }

        mock_claims_process._update_policy_claims_stats(
            policy_id, data, "PAID", date(2024, 6, 15)
        )

        log = policy_in_state["paid_claim_log"]
        assert log[0] == (date(2024, 6, 15), 0.0, 0.0)


# =============================================================================
# Tests: _get_claims_history (PolicyLifecycleProcess rolling window)
# =============================================================================


class TestGetClaimsHistoryDaysSince:
    """Tests for days_since_last_claim computation."""

    def test_days_since_from_last_claim_date(
        self, mock_lifecycle_process, sim_env
    ):
        """Should compute days_since_last_claim from last_claim_date."""
        # sim_env starts at 2024-01-01, advance 100 days
        sim_env.env.run(until=100)

        policy = {"last_claim_date": date(2024, 3, 1)}
        result = mock_lifecycle_process._get_claims_history(policy)

        expected_days = (sim_env.current_date - date(2024, 3, 1)).days
        assert result["days_since_last_claim"] == expected_days

    def test_days_since_none_when_no_claims(self, mock_lifecycle_process):
        """Should return None when no last_claim_date exists."""
        policy = {}
        result = mock_lifecycle_process._get_claims_history(policy)
        assert result["days_since_last_claim"] is None


class TestGetClaimsHistoryDenialCount:
    """Tests for rolling 12-month denial_count."""

    def test_denial_count_within_window(self, mock_lifecycle_process, sim_env):
        """Denials within 12 months should be counted."""
        sim_env.env.run(until=200)  # July 2024
        current = sim_env.current_date

        policy = {
            "denial_log": [
                current - timedelta(days=30),
                current - timedelta(days=90),
                current - timedelta(days=180),
            ]
        }
        result = mock_lifecycle_process._get_claims_history(policy)
        assert result["denial_count"] == 3

    def test_denial_count_excludes_old_events(self, mock_lifecycle_process, sim_env):
        """Denials older than 12 months should be excluded."""
        sim_env.env.run(until=400)  # ~Feb 2025
        current = sim_env.current_date

        policy = {
            "denial_log": [
                current - timedelta(days=30),   # Recent — included
                current - timedelta(days=400),  # Old — excluded
            ]
        }
        result = mock_lifecycle_process._get_claims_history(policy)
        assert result["denial_count"] == 1

    def test_denial_count_zero_when_empty(self, mock_lifecycle_process):
        """Should return 0 when no denial_log exists."""
        result = mock_lifecycle_process._get_claims_history({})
        assert result["denial_count"] == 0


class TestGetClaimsHistoryHighOOP:
    """Tests for high_out_of_pocket computation from rolling gap."""

    def test_high_oop_true_when_gap_exceeds_threshold(
        self, mock_lifecycle_process, sim_env
    ):
        """Should return True when 12-month gap > threshold ($500 default)."""
        sim_env.env.run(until=200)
        current = sim_env.current_date

        policy = {
            "paid_claim_log": [
                (current - timedelta(days=30), 500.0, 300.0),
                (current - timedelta(days=60), 400.0, 250.0),
            ]
        }
        result = mock_lifecycle_process._get_claims_history(policy)
        # Cumulative gap = 300 + 250 = 550 > 500
        assert result["high_out_of_pocket"] is True

    def test_high_oop_false_when_gap_below_threshold(
        self, mock_lifecycle_process, sim_env
    ):
        """Should return False when 12-month gap < threshold."""
        sim_env.env.run(until=200)
        current = sim_env.current_date

        policy = {
            "paid_claim_log": [
                (current - timedelta(days=30), 500.0, 100.0),
                (current - timedelta(days=60), 400.0, 50.0),
            ]
        }
        result = mock_lifecycle_process._get_claims_history(policy)
        # Cumulative gap = 100 + 50 = 150 < 500
        assert result["high_out_of_pocket"] is False

    def test_high_oop_excludes_old_gap_events(
        self, mock_lifecycle_process, sim_env
    ):
        """Old gap events should not count toward threshold."""
        sim_env.env.run(until=400)
        current = sim_env.current_date

        policy = {
            "paid_claim_log": [
                (current - timedelta(days=30), 200.0, 100.0),   # Recent
                (current - timedelta(days=400), 500.0, 600.0),  # Old — excluded
            ]
        }
        result = mock_lifecycle_process._get_claims_history(policy)
        # Only recent gap = 100 < 500
        assert result["high_out_of_pocket"] is False

    def test_high_oop_false_when_no_claims(self, mock_lifecycle_process):
        """Should return False when no paid_claim_log exists."""
        result = mock_lifecycle_process._get_claims_history({})
        assert result["high_out_of_pocket"] is False


class TestGetClaimsHistoryTotalAmount:
    """Tests for rolling 12-month total_claims_amount."""

    def test_total_claims_sums_recent_benefits(
        self, mock_lifecycle_process, sim_env
    ):
        """Should sum benefit amounts from the last 12 months."""
        sim_env.env.run(until=200)
        current = sim_env.current_date

        policy = {
            "paid_claim_log": [
                (current - timedelta(days=30), 500.0, 50.0),
                (current - timedelta(days=60), 300.0, 30.0),
            ]
        }
        result = mock_lifecycle_process._get_claims_history(policy)
        assert result["total_claims_amount"] == 800.0

    def test_total_claims_excludes_old(self, mock_lifecycle_process, sim_env):
        """Benefit amounts older than 12 months should be excluded."""
        sim_env.env.run(until=400)
        current = sim_env.current_date

        policy = {
            "paid_claim_log": [
                (current - timedelta(days=30), 200.0, 20.0),
                (current - timedelta(days=400), 1000.0, 100.0),  # Excluded
            ]
        }
        result = mock_lifecycle_process._get_claims_history(policy)
        assert result["total_claims_amount"] == 200.0


class TestGetClaimsHistoryPruning:
    """Tests for lazy pruning of old events."""

    def test_old_paid_events_pruned_from_list(
        self, mock_lifecycle_process, sim_env
    ):
        """Old entries should be removed from paid_claim_log after computation."""
        sim_env.env.run(until=400)
        current = sim_env.current_date

        policy = {
            "paid_claim_log": [
                (current - timedelta(days=30), 200.0, 20.0),
                (current - timedelta(days=400), 1000.0, 100.0),
            ]
        }
        mock_lifecycle_process._get_claims_history(policy)

        # Old entry should be pruned
        assert len(policy["paid_claim_log"]) == 1

    def test_old_denial_events_pruned_from_list(
        self, mock_lifecycle_process, sim_env
    ):
        """Old entries should be removed from denial_log after computation."""
        sim_env.env.run(until=400)
        current = sim_env.current_date

        policy = {
            "denial_log": [
                current - timedelta(days=30),
                current - timedelta(days=400),
            ]
        }
        mock_lifecycle_process._get_claims_history(policy)

        assert len(policy["denial_log"]) == 1


class TestGetClaimsHistoryDefaults:
    """Tests for default values when policy dict has no claims fields."""

    def test_all_defaults(self, mock_lifecycle_process):
        """Empty policy dict should return safe defaults."""
        result = mock_lifecycle_process._get_claims_history({})

        assert result["days_since_last_claim"] is None
        assert result["denial_count"] == 0
        assert result["high_out_of_pocket"] is False
        assert result["total_claims_amount"] == 0
