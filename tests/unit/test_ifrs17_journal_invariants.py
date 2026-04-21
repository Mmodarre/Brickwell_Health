"""
Invariant tests for the IFRS 17 journal-line emission.

These run against the same SQLite fixture as ``test_ifrs17_engine.py`` and
assert properties that should hold for any engine run, regardless of the
specific movement amounts:

* every journal_line resolves to a valid gl_period and gl_account
* sum(debit_amount) == sum(credit_amount) per (cohort, month)
* every non-zero movement bucket produces exactly one debit + one credit line
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from brickwell_health.config.ifrs17 import IFRS17Config
from brickwell_health.ifrs17.assumptions import IFRS17Assumptions
from brickwell_health.ifrs17.engine import IFRS17Engine


REAL_POSTING_RULES_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "reference" / "ifrs17_posting_rules.yaml"
)

GL_ACCOUNT_SEED: list[tuple[int, str]] = [
    (101, "1100-03"),
    (201, "2130-01"),
    (202, "2140-01"),
    (301, "2300-01"),
    (302, "2300-02"),
    (401, "4100-01"),
    (501, "5200-01"),
    (502, "5300-01"),
    (503, "5300-08"),
]


@pytest.fixture
def sqlite_engine(tmp_path):
    import calendar as _cal
    import sqlite3

    main = tmp_path / "main.sqlite"

    def _creator():
        conn = sqlite3.connect(str(main))
        for s in ["policy", "billing", "claims", "ifrs17", "reference"]:
            conn.execute(f"ATTACH DATABASE '{tmp_path / (s + '.sqlite')}' AS {s}")
        return conn

    engine = create_engine("sqlite://", creator=_creator, future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE policy.policy (policy_id TEXT PRIMARY KEY, ifrs17_cohort_id TEXT, product_id INTEGER, policy_status TEXT, effective_date DATE, end_date DATE, premium_amount NUMERIC, payment_frequency TEXT)"))
        conn.execute(text("CREATE TABLE billing.payment (payment_id TEXT PRIMARY KEY, policy_id TEXT, payment_date DATE, payment_amount NUMERIC, payment_status TEXT)"))
        conn.execute(text("CREATE TABLE billing.acquisition_cost (acquisition_cost_id TEXT PRIMARY KEY, policy_id TEXT, commission_type TEXT, distribution_channel TEXT, gross_written_premium NUMERIC, commission_rate NUMERIC, commission_amount NUMERIC, incurred_date DATE, amortisation_start_date DATE, amortisation_end_date DATE, status TEXT)"))
        conn.execute(text("CREATE TABLE claims.claim (claim_id TEXT PRIMARY KEY, policy_id TEXT, service_date DATE, lodgement_date DATE, payment_date DATE, total_charge NUMERIC, total_benefit NUMERIC, claim_status TEXT)"))
        conn.execute(text("CREATE TABLE ifrs17.cohort (cohort_id TEXT PRIMARY KEY, portfolio TEXT, afy_label TEXT, afy_start_date DATE, afy_end_date DATE, is_onerous_at_inception INTEGER DEFAULT 0, onerous_first_detected_month DATE)"))
        conn.execute(text("CREATE TABLE ifrs17.monthly_balance (monthly_balance_id TEXT PRIMARY KEY, cohort_id TEXT, reporting_month DATE, policy_count INTEGER, in_force_premium NUMERIC, lrc_excl_loss_component NUMERIC, loss_component NUMERIC, lrc_total NUMERIC, lic_best_estimate NUMERIC, lic_risk_adjustment NUMERIC, lic_ibnr NUMERIC, lic_total NUMERIC, deferred_acquisition_cost NUMERIC, is_onerous INTEGER, gl_period_id INTEGER, created_at TEXT)"))
        conn.execute(text("CREATE TABLE ifrs17.monthly_movement (monthly_movement_id TEXT PRIMARY KEY, cohort_id TEXT, reporting_month DATE, opening_lrc NUMERIC, premiums_received NUMERIC, insurance_revenue NUMERIC, insurance_service_expense NUMERIC, claims_incurred NUMERIC, acquisition_cost_amortised NUMERIC, loss_component_recognised NUMERIC, loss_component_reversed NUMERIC, closing_lrc NUMERIC, insurance_service_result NUMERIC, gl_period_id INTEGER, created_at TEXT)"))
        conn.execute(text("CREATE TABLE ifrs17.onerous_assessment (assessment_id TEXT PRIMARY KEY, cohort_id TEXT, reporting_month DATE, expected_remaining_premium NUMERIC, expected_remaining_claims NUMERIC, expected_remaining_expenses NUMERIC, expected_combined_ratio NUMERIC, onerous_threshold_crossed INTEGER, loss_component_change NUMERIC, notes TEXT, gl_period_id INTEGER, created_at TEXT)"))
        conn.execute(text("CREATE TABLE reference.gl_account (account_id INTEGER PRIMARY KEY, account_code TEXT UNIQUE, account_name TEXT)"))
        conn.execute(text("CREATE TABLE reference.gl_period (period_id INTEGER PRIMARY KEY, period_code TEXT UNIQUE, start_date DATE, end_date DATE)"))
        conn.execute(text("CREATE TABLE ifrs17.journal_line (journal_line_id TEXT PRIMARY KEY, cohort_id TEXT, reporting_month DATE, gl_period_id INTEGER, gl_account_id INTEGER, cost_centre_id INTEGER, movement_bucket TEXT, debit_amount NUMERIC, credit_amount NUMERIC, journal_source TEXT, created_at TEXT)"))

        for acc_id, code in GL_ACCOUNT_SEED:
            conn.execute(
                text("INSERT INTO reference.gl_account (account_id, account_code, account_name) VALUES (:i, :c, :n)"),
                {"i": acc_id, "c": code, "n": f"test-{code}"},
            )

        period_id = 1
        for year in range(2025, 2028):
            for month in range(1, 13):
                last = _cal.monthrange(year, month)[1]
                conn.execute(
                    text("INSERT INTO reference.gl_period (period_id, period_code, start_date, end_date) VALUES (:i, :c, :s, :e)"),
                    {
                        "i": period_id,
                        "c": f"{year:04d}-{month:02d}",
                        "s": date(year, month, 1),
                        "e": date(year, month, last),
                    },
                )
                period_id += 1
    yield engine


def _assumptions() -> IFRS17Assumptions:
    return IFRS17Assumptions(
        commission_rates_by_channel={"Online": Decimal("0.015")},
        commission_default_rate=Decimal("0.02"),
        commission_amortisation_months=12,
        ra_uplift_pct=Decimal("0.06"),
        ra_confidence_level=0.75,
        ibnr_lookback_months=3,
        ibnr_lag_factor=Decimal("0.15"),
        earning_method="straight_line",
        earning_seasonal_weights=None,
        onerous_threshold=Decimal("1.00"),
        onerous_hysteresis_band=Decimal("0.05"),
        onerous_min_policies=1,
        onerous_min_months_history=1,
        onerous_default_loss_ratio=Decimal("0.85"),
        onerous_per_policy_monthly_admin=Decimal("0.00"),
        discounting_enabled=False,
        discount_rate=Decimal("0.035"),
        portfolios=["HOSPITAL_ONLY", "EXTRAS_ONLY", "COMBINED", "AMBULANCE_ONLY"],
        afy_start_month=7,
    )


def _seed_minimal(conn):
    """One active policy with premium inflows so several buckets are non-zero."""
    conn.execute(text("""
        INSERT INTO ifrs17.cohort
        (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
        VALUES ('HOSPITAL_ONLY-AFY26', 'HOSPITAL_ONLY', 'AFY26', '2025-07-01', '2026-06-30')
    """))
    conn.execute(text("""
        INSERT INTO policy.policy VALUES
        ('P1', 'HOSPITAL_ONLY-AFY26', 1, 'Active', '2025-07-01', NULL, 200, 'Monthly')
    """))
    # Cash inflow so premiums_received fires.
    conn.execute(text("""
        INSERT INTO billing.payment VALUES
        ('PAY1', 'P1', '2025-07-05', 200, 'Completed')
    """))
    # Commission to exercise dac_amortised bucket.
    conn.execute(text("""
        INSERT INTO billing.acquisition_cost VALUES
        ('A1', 'P1', 'Upfront', 'Online', 2400, 0.015, 36,
         '2025-07-01', '2025-07-01', '2026-07-01', 'Active')
    """))
    # One claim so claims_incurred fires.
    conn.execute(text("""
        INSERT INTO claims.claim VALUES
        ('C1', 'P1', '2025-07-15', '2025-07-15', '2025-07-20', 100, 100, 'Paid')
    """))


def _run_engine(sqlite_engine, tmp_path, sim_end=date(2025, 8, 31)):
    return IFRS17Engine(
        config=IFRS17Config(
            enabled=True,
            assumptions_path=tmp_path / "unused.yaml",
            posting_rules_path=REAL_POSTING_RULES_PATH,
            csv_export_dir=tmp_path / "out",
            csv_export_enabled=False,
        ),
        db_engine=sqlite_engine,
        sim_start=date(2025, 7, 1),
        sim_end=sim_end,
        assumptions=_assumptions(),
    ).run()


class TestJournalInvariants:
    def test_debits_equal_credits_per_cohort_month(self, sqlite_engine, tmp_path):
        with sqlite_engine.begin() as conn:
            _seed_minimal(conn)

        _run_engine(sqlite_engine, tmp_path)

        with sqlite_engine.connect() as conn:
            rows = list(conn.execute(text("""
                SELECT cohort_id, reporting_month,
                       SUM(debit_amount) AS dr, SUM(credit_amount) AS cr
                FROM ifrs17.journal_line
                GROUP BY cohort_id, reporting_month
            """)))
            assert rows, "engine produced no journal lines"
            for r in rows:
                assert Decimal(str(r.dr)) == Decimal(str(r.cr)), (
                    f"unbalanced in {r.cohort_id} {r.reporting_month}: "
                    f"dr={r.dr} cr={r.cr}"
                )

    def test_every_line_resolves_fk_targets(self, sqlite_engine, tmp_path):
        with sqlite_engine.begin() as conn:
            _seed_minimal(conn)

        _run_engine(sqlite_engine, tmp_path)

        with sqlite_engine.connect() as conn:
            broken = conn.execute(text("""
                SELECT COUNT(*) FROM ifrs17.journal_line jl
                LEFT JOIN reference.gl_account a ON a.account_id = jl.gl_account_id
                LEFT JOIN reference.gl_period p ON p.period_id = jl.gl_period_id
                WHERE a.account_id IS NULL OR p.period_id IS NULL
            """)).scalar()
            assert broken == 0

    def test_bucket_debits_match_credits(self, sqlite_engine, tmp_path):
        """Every bucket should have equal debit-side and credit-side counts."""
        with sqlite_engine.begin() as conn:
            _seed_minimal(conn)

        _run_engine(sqlite_engine, tmp_path)

        with sqlite_engine.connect() as conn:
            rows = list(conn.execute(text("""
                SELECT movement_bucket,
                       SUM(CASE WHEN debit_amount > 0 THEN 1 ELSE 0 END) AS d_count,
                       SUM(CASE WHEN credit_amount > 0 THEN 1 ELSE 0 END) AS c_count
                FROM ifrs17.journal_line
                GROUP BY movement_bucket
            """)))
            assert rows
            for r in rows:
                assert r.d_count == r.c_count, (
                    f"bucket {r.movement_bucket} debits={r.d_count} credits={r.c_count}"
                )

    def test_exactly_one_side_nonzero_per_line(self, sqlite_engine, tmp_path):
        with sqlite_engine.begin() as conn:
            _seed_minimal(conn)

        _run_engine(sqlite_engine, tmp_path)

        with sqlite_engine.connect() as conn:
            bad = conn.execute(text("""
                SELECT COUNT(*) FROM ifrs17.journal_line
                WHERE (debit_amount = 0) = (credit_amount = 0)
            """)).scalar()
            assert bad == 0, "journal_line CHECK constraint violated"

    def test_zero_buckets_produce_no_lines(self, sqlite_engine, tmp_path):
        """A month with no policies should emit no journal lines for that cohort."""
        with sqlite_engine.begin() as conn:
            # Cohort only — no policies, payments, claims or commissions.
            conn.execute(text("""
                INSERT INTO ifrs17.cohort
                (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
                VALUES ('HOSPITAL_ONLY-AFY26', 'HOSPITAL_ONLY', 'AFY26',
                        '2025-07-01', '2026-06-30')
            """))

        _run_engine(sqlite_engine, tmp_path, sim_end=date(2025, 7, 31))

        with sqlite_engine.connect() as conn:
            count = conn.execute(text("""
                SELECT COUNT(*) FROM ifrs17.journal_line
                WHERE cohort_id='HOSPITAL_ONLY-AFY26'
            """)).scalar()
            assert count == 0
