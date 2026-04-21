"""Unit tests for the IFRS 17 posting rule engine."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from brickwell_health.ifrs17.posting import (
    PostingRule,
    PostingRuleEngine,
    PostingRuleError,
)


REAL_POSTING_RULES = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "reference" / "ifrs17_posting_rules.yaml"
)


# Minimal chart-of-accounts sufficient for the shipped posting rules.
_MIN_ACCOUNTS: dict[str, int] = {
    "1100-03": 101,
    "2130-01": 201,
    "2140-01": 202,
    "2300-01": 301,
    "2300-02": 302,
    "4100-01": 401,
    "5200-01": 501,
    "5300-01": 502,
    "5300-08": 503,
}


class TestFromYaml:
    def test_loads_shipped_rules(self):
        rules = PostingRuleEngine.from_yaml(REAL_POSTING_RULES)
        # Must cover every bucket the engine emits.
        assert set(rules.buckets) >= {
            "insurance_revenue",
            "premiums_received",
            "claims_incurred",
            "dac_amortised",
            "loss_component_recognised",
            "loss_component_reversed",
        }

    def test_rule_for_returns_pair(self):
        rules = PostingRuleEngine.from_yaml(REAL_POSTING_RULES)
        rule = rules.rule_for("insurance_revenue")
        assert isinstance(rule, PostingRule)
        assert rule.debit_account_code == "2130-01"
        assert rule.credit_account_code == "4100-01"

    def test_validates_against_real_chart(self):
        # Should not raise when all referenced codes exist.
        rules = PostingRuleEngine.from_yaml(REAL_POSTING_RULES)
        rules.validate_against_accounts(_MIN_ACCOUNTS)

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            PostingRuleEngine.from_yaml(tmp_path / "nope.yaml")

    def test_empty_rules_raises(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("posting_rules: []\n")
        with pytest.raises(PostingRuleError):
            PostingRuleEngine.from_yaml(p)

    def test_missing_required_key_raises(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(
            "posting_rules:\n"
            "  - bucket: x\n"
            "    debit_account_code: '1'\n"
            "    # credit_account_code missing\n"
        )
        with pytest.raises(PostingRuleError):
            PostingRuleEngine.from_yaml(p)

    def test_duplicate_bucket_raises(self):
        with pytest.raises(PostingRuleError):
            PostingRuleEngine(
                [
                    PostingRule("x", "1", "2"),
                    PostingRule("x", "3", "4"),
                ]
            )


class TestValidation:
    def test_unknown_debit_code_raises(self):
        rules = PostingRuleEngine([PostingRule("x", "UNKNOWN", "2130-01")])
        with pytest.raises(PostingRuleError, match="UNKNOWN"):
            rules.validate_against_accounts(_MIN_ACCOUNTS)

    def test_unknown_credit_code_raises(self):
        rules = PostingRuleEngine([PostingRule("x", "2130-01", "UNKNOWN")])
        with pytest.raises(PostingRuleError, match="UNKNOWN"):
            rules.validate_against_accounts(_MIN_ACCOUNTS)


class TestRuleFor:
    def test_unknown_bucket_raises(self):
        rules = PostingRuleEngine([PostingRule("a", "1", "2")])
        with pytest.raises(PostingRuleError, match="not-here"):
            rules.rule_for("not-here")


class TestBuildLines:
    def test_emits_two_lines_per_nonzero_bucket(self):
        rules = PostingRuleEngine.from_yaml(REAL_POSTING_RULES)
        movement = {
            "insurance_revenue": Decimal("100"),
            "premiums_received": Decimal("200"),
            "claims_incurred": Decimal("0"),  # skipped
            "dac_amortised": Decimal("10"),
            "loss_component_recognised": Decimal("0"),  # skipped
            "loss_component_reversed": Decimal("0"),  # skipped
        }
        lines = rules.build_lines(
            cohort_id="C1",
            reporting_month=date(2025, 7, 31),
            gl_period_id=42,
            gl_account_by_code=_MIN_ACCOUNTS,
            movement=movement,
        )
        # 3 non-zero buckets × 2 lines each
        assert len(lines) == 6
        # Every line has exactly one side non-zero.
        for line in lines:
            assert (line.debit_amount == 0) != (line.credit_amount == 0)
            assert line.gl_period_id == 42
            assert line.cohort_id == "C1"
            assert line.reporting_month == date(2025, 7, 31)
            assert line.cost_centre_id is None

        # Debits total equals credits total per bucket.
        buckets = {}
        for line in lines:
            buckets.setdefault(line.movement_bucket, [Decimal("0"), Decimal("0")])
            buckets[line.movement_bucket][0] += line.debit_amount
            buckets[line.movement_bucket][1] += line.credit_amount
        for bucket, (d, c) in buckets.items():
            assert d == c, f"unbalanced debits vs credits for {bucket}"

    def test_negative_amount_skipped(self):
        rules = PostingRuleEngine([PostingRule("x", "2130-01", "4100-01")])
        lines = rules.build_lines(
            cohort_id="C1",
            reporting_month=date(2025, 7, 31),
            gl_period_id=1,
            gl_account_by_code=_MIN_ACCOUNTS,
            movement={"x": Decimal("-1")},
        )
        assert lines == []

    def test_missing_bucket_skipped(self):
        rules = PostingRuleEngine([PostingRule("x", "2130-01", "4100-01")])
        lines = rules.build_lines(
            cohort_id="C1",
            reporting_month=date(2025, 7, 31),
            gl_period_id=1,
            gl_account_by_code=_MIN_ACCOUNTS,
            movement={},
        )
        assert lines == []

    def test_debit_and_credit_target_expected_accounts(self):
        rules = PostingRuleEngine([PostingRule("x", "2130-01", "4100-01")])
        lines = rules.build_lines(
            cohort_id="C1",
            reporting_month=date(2025, 7, 31),
            gl_period_id=1,
            gl_account_by_code=_MIN_ACCOUNTS,
            movement={"x": Decimal("50")},
        )
        assert len(lines) == 2
        debit = next(line for line in lines if line.debit_amount > 0)
        credit = next(line for line in lines if line.credit_amount > 0)
        assert debit.gl_account_id == _MIN_ACCOUNTS["2130-01"]
        assert credit.gl_account_id == _MIN_ACCOUNTS["4100-01"]
        assert debit.debit_amount == Decimal("50")
        assert credit.credit_amount == Decimal("50")
