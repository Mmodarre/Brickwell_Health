"""
Unit tests for the IFRS 17 onerous-contract detection logic.

Tests run against SQLite in-memory — the engine's onerous logic is pure Python
applied to aggregated SQL results, so a tiny stub fixture seeded with specific
claim/premium shapes is enough to exercise each branch.
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


# Reuse the SQLite setup pattern from test_ifrs17_engine.py.
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
        for year in range(2025, 2029):
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


def _assumptions(**kw):
    base = dict(
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
    base.update(kw)
    return IFRS17Assumptions(**base)


def _config(tmp_path):
    return IFRS17Config(
        enabled=True,
        assumptions_path=tmp_path / "unused.yaml",
        posting_rules_path=REAL_POSTING_RULES_PATH,
        csv_export_dir=tmp_path / "out",
        csv_export_enabled=False,
    )


def _seed_cohort(conn, cohort_id="EXTRAS_ONLY-AFY26"):
    conn.execute(text("""
        INSERT INTO ifrs17.cohort
        (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
        VALUES (:cid, 'EXTRAS_ONLY', 'AFY26', '2025-07-01', '2026-06-30')
    """), {"cid": cohort_id})


def _seed_policy(
    conn, policy_id, cohort_id, premium=Decimal("80"), frequency="Monthly",
    effective=date(2025, 7, 1), end_date=None,
):
    conn.execute(text("""
        INSERT INTO policy.policy
        (policy_id, ifrs17_cohort_id, product_id, policy_status, effective_date,
         end_date, premium_amount, payment_frequency)
        VALUES (:pid, :cid, 1, 'Active', :eff, :end, :prem, :freq)
    """), {
        "pid": policy_id, "cid": cohort_id, "eff": effective,
        "end": end_date, "prem": float(premium), "freq": frequency,
    })


def _seed_claim(conn, claim_id, policy_id, service_date, charge):
    conn.execute(text("""
        INSERT INTO claims.claim
        (claim_id, policy_id, service_date, lodgement_date, total_charge,
         total_benefit, claim_status, payment_date)
        VALUES (:cid, :pid, :svc, :svc, :chg, :chg, 'Paid', :svc)
    """), {"cid": claim_id, "pid": policy_id, "svc": service_date, "chg": float(charge)})


class TestNormalCohortNotOnerous:
    def test_low_claim_cohort_has_zero_loss_component(self, sqlite_engine, tmp_path):
        """A cohort with low claims should never cross the onerous threshold."""
        with sqlite_engine.begin() as conn:
            _seed_cohort(conn)
            _seed_policy(conn, "p1", "EXTRAS_ONLY-AFY26", premium=Decimal("200"))

        engine = IFRS17Engine(
            config=_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2025, 12, 31),
            assumptions=_assumptions(),
        )
        engine.run()

        with sqlite_engine.connect() as conn:
            rows = list(conn.execute(text("""
                SELECT loss_component FROM ifrs17.monthly_balance
                WHERE cohort_id='EXTRAS_ONLY-AFY26'
                ORDER BY reporting_month
            """)))
            assert all(Decimal(str(r.loss_component)) == Decimal("0.00") for r in rows)


class TestHighClaimCohortRecognisesLossComponent:
    def test_high_claims_flip_onerous_and_record_lc(self, sqlite_engine, tmp_path):
        """A cohort where claims exceed ~100% combined ratio should go onerous
        and show a positive loss component."""
        with sqlite_engine.begin() as conn:
            _seed_cohort(conn)
            # Policy with substantial premium so LRC stays positive.
            _seed_policy(
                conn, "p1", "EXTRAS_ONLY-AFY26", premium=Decimal("1000"),
            )
            # Annual pre-payment, keeps LRC positive.
            conn.execute(text("""
                INSERT INTO billing.payment
                (payment_id, policy_id, payment_date, payment_amount, payment_status)
                VALUES ('pp1', 'p1', '2025-07-02', 12000, 'Completed')
            """))
            # Claims well in excess of earned premium month-on-month.
            for i, m in enumerate([7, 8, 9, 10, 11, 12], start=1):
                _seed_claim(
                    conn, f"c{i}", "p1", date(2025, m, 15), Decimal("5000"),
                )

        engine = IFRS17Engine(
            config=_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2025, 12, 31),
            assumptions=_assumptions(),
        )
        engine.run()

        with sqlite_engine.connect() as conn:
            rows = list(conn.execute(text("""
                SELECT reporting_month, loss_component, is_onerous
                FROM ifrs17.monthly_balance
                WHERE cohort_id='EXTRAS_ONLY-AFY26'
                ORDER BY reporting_month
            """)))
            # At least one month should be flagged onerous with LC > 0.
            onerous_months = [
                r for r in rows
                if bool(r.is_onerous) and Decimal(str(r.loss_component)) > Decimal("0")
            ]
            assert len(onerous_months) > 0


class TestLossComponentNeverNegative:
    def test_reversal_clamped_at_zero(self, sqlite_engine, tmp_path):
        """A large reversal must never drive the loss component below zero."""
        # Seed high claims early then zero claims — reversal attempted on
        # a small LC should clamp at zero.
        with sqlite_engine.begin() as conn:
            _seed_cohort(conn)
            _seed_policy(
                conn, "p1", "EXTRAS_ONLY-AFY26", premium=Decimal("80"),
            )
            _seed_claim(conn, "c1", "p1", date(2025, 7, 15), Decimal("500"))
            _seed_claim(conn, "c2", "p1", date(2025, 8, 15), Decimal("500"))

        engine = IFRS17Engine(
            config=_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2026, 6, 30),
            assumptions=_assumptions(),
        )
        engine.run()

        with sqlite_engine.connect() as conn:
            negatives = conn.execute(text("""
                SELECT COUNT(*) AS n FROM ifrs17.monthly_balance
                WHERE loss_component < 0
            """)).scalar()
            assert negatives == 0


class TestOnerousFirstDetectedMonthPersisted:
    def test_first_detection_stamped_on_cohort(self, sqlite_engine, tmp_path):
        """Once a cohort goes onerous, ``onerous_first_detected_month`` should
        be stamped on ifrs17.cohort and never cleared."""
        with sqlite_engine.begin() as conn:
            _seed_cohort(conn)
            _seed_policy(conn, "p1", "EXTRAS_ONLY-AFY26", premium=Decimal("1000"))
            conn.execute(text("""
                INSERT INTO billing.payment
                (payment_id, policy_id, payment_date, payment_amount, payment_status)
                VALUES ('pp1', 'p1', '2025-07-02', 12000, 'Completed')
            """))
            for i, m in enumerate([7, 8, 9], start=1):
                _seed_claim(conn, f"c{i}", "p1", date(2025, m, 15), Decimal("5000"))

        engine = IFRS17Engine(
            config=_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2025, 12, 31),
            assumptions=_assumptions(),
        )
        engine.run()

        with sqlite_engine.connect() as conn:
            detected = conn.execute(text("""
                SELECT onerous_first_detected_month FROM ifrs17.cohort
                WHERE cohort_id='EXTRAS_ONLY-AFY26'
            """)).scalar()
            assert detected is not None
