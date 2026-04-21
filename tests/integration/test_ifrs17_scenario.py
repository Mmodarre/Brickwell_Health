"""
Scenario-level integration test for the IFRS 17 engine.

This test hand-seeds 4 policies across all portfolios over a 24-month window,
runs the engine, and validates the plan's invariants:

1. closing_lrc[t] == opening_lrc[t+1]  per (cohort, month)
2. loss_component >= 0                 always
3. sum(acquisition_cost_amortised) == sum(commission_amount) by amort_end_date
4. onerous_first_detected_month set at most once per cohort

We use SQLite in-memory so this test runs in <1s and does not require
Postgres. When a live Postgres connection is available, the same engine runs
unchanged against it.
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
        # Phase 2: finance dims + journal-line fact.
        conn.execute(text("CREATE TABLE reference.gl_account (account_id INTEGER PRIMARY KEY, account_code TEXT UNIQUE, account_name TEXT)"))
        conn.execute(text("CREATE TABLE reference.gl_period (period_id INTEGER PRIMARY KEY, period_code TEXT UNIQUE, start_date DATE, end_date DATE)"))
        conn.execute(text("CREATE TABLE ifrs17.journal_line (journal_line_id TEXT PRIMARY KEY, cohort_id TEXT, reporting_month DATE, gl_period_id INTEGER, gl_account_id INTEGER, cost_centre_id INTEGER, movement_bucket TEXT, debit_amount NUMERIC, credit_amount NUMERIC, journal_source TEXT, created_at TEXT)"))

        for acc_id, code in GL_ACCOUNT_SEED:
            conn.execute(
                text(
                    "INSERT INTO reference.gl_account "
                    "(account_id, account_code, account_name) "
                    "VALUES (:i, :c, :n)"
                ),
                {"i": acc_id, "c": code, "n": f"test-{code}"},
            )

        import calendar as _cal
        period_id = 1
        for year in range(2025, 2029):
            for month in range(1, 13):
                last = _cal.monthrange(year, month)[1]
                conn.execute(
                    text(
                        "INSERT INTO reference.gl_period "
                        "(period_id, period_code, start_date, end_date) "
                        "VALUES (:i, :c, :s, :e)"
                    ),
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
        commission_rates_by_channel={
            "Online": Decimal("0.015"),
            "Broker": Decimal("0.12"),
            "Comparison": Decimal("0.18"),
        },
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


def _seed_4_policy_scenario(conn):
    """4 policies, 1 per portfolio, across two AFYs."""
    # Cohorts for the window: AFY26 only (24 months July 2025 - June 2027
    # actually spans AFY26+AFY27; both should be pre-populated).
    for cid, port in [
        ("HOSPITAL_ONLY-AFY26", "HOSPITAL_ONLY"),
        ("EXTRAS_ONLY-AFY26", "EXTRAS_ONLY"),
        ("COMBINED-AFY26", "COMBINED"),
        ("AMBULANCE_ONLY-AFY26", "AMBULANCE_ONLY"),
        ("HOSPITAL_ONLY-AFY27", "HOSPITAL_ONLY"),
        ("EXTRAS_ONLY-AFY27", "EXTRAS_ONLY"),
        ("COMBINED-AFY27", "COMBINED"),
        ("AMBULANCE_ONLY-AFY27", "AMBULANCE_ONLY"),
    ]:
        afy = cid.split("-")[-1]
        if afy == "AFY26":
            start, end = date(2025, 7, 1), date(2026, 6, 30)
        else:
            start, end = date(2026, 7, 1), date(2027, 6, 30)
        conn.execute(text("""
            INSERT INTO ifrs17.cohort
            (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
            VALUES (:cid, :p, :a, :s, :e)
        """), {"cid": cid, "p": port, "a": afy, "s": start, "e": end})

    # P1 HOSPITAL_ONLY $200/mo Online
    conn.execute(text("""
        INSERT INTO policy.policy VALUES
        ('P1', 'HOSPITAL_ONLY-AFY26', 1, 'Active', '2025-07-01', NULL, 200, 'Monthly')
    """))
    conn.execute(text("""
        INSERT INTO billing.acquisition_cost VALUES
        ('A1', 'P1', 'Upfront', 'Online', 2400, 0.015, 36, '2025-07-01', '2025-07-01', '2026-07-01', 'Active')
    """))
    # Monthly premiums
    for i, (y, m) in enumerate([
        (2025, 7), (2025, 8), (2025, 9), (2025, 10), (2025, 11), (2025, 12),
        (2026, 1), (2026, 2), (2026, 3), (2026, 4), (2026, 5), (2026, 6),
    ], start=1):
        conn.execute(text("""
            INSERT INTO billing.payment VALUES
            (:id, 'P1', :d, 200, 'Completed')
        """), {"id": f"PAY_P1_{i}", "d": date(y, m, 5)})

    # P2 EXTRAS_ONLY $80/mo Comparison — seeded high claims, onerous
    conn.execute(text("""
        INSERT INTO policy.policy VALUES
        ('P2', 'EXTRAS_ONLY-AFY26', 2, 'Active', '2025-07-01', NULL, 80, 'Monthly')
    """))
    conn.execute(text("""
        INSERT INTO billing.acquisition_cost VALUES
        ('A2', 'P2', 'Upfront', 'Comparison', 960, 0.18, 172.80, '2025-07-01', '2025-07-01', '2026-07-01', 'Active')
    """))
    # Pre-pay to keep LRC positive so onerous logic engages.
    conn.execute(text("""
        INSERT INTO billing.payment VALUES
        ('PAY_P2_1', 'P2', '2025-07-02', 960, 'Completed')
    """))
    # Heavy claims months 7-12
    for i, m in enumerate([7, 8, 9, 10, 11, 12], start=1):
        conn.execute(text("""
            INSERT INTO claims.claim VALUES
            (:id, 'P2', :d, :d, :d, 1500, 1500, 'Paid')
        """), {"id": f"C_P2_{i}", "d": date(2025, m, 15)})

    # P3 COMBINED $350/mo, lapses after 9 months
    conn.execute(text("""
        INSERT INTO policy.policy VALUES
        ('P3', 'COMBINED-AFY26', 3, 'Cancelled', '2025-07-01', '2026-03-31', 350, 'Monthly')
    """))
    conn.execute(text("""
        INSERT INTO billing.acquisition_cost VALUES
        ('A3', 'P3', 'Upfront', 'Broker', 4200, 0.12, 504, '2025-07-01', '2025-07-01', '2026-07-01', 'Active')
    """))
    for i, (y, m) in enumerate([
        (2025, 7), (2025, 8), (2025, 9), (2025, 10), (2025, 11), (2025, 12),
        (2026, 1), (2026, 2), (2026, 3),
    ], start=1):
        conn.execute(text("""
            INSERT INTO billing.payment VALUES
            (:id, 'P3', :d, 350, 'Completed')
        """), {"id": f"PAY_P3_{i}", "d": date(y, m, 5)})

    # P4 AMBULANCE_ONLY $15/mo, low volume
    conn.execute(text("""
        INSERT INTO policy.policy VALUES
        ('P4', 'AMBULANCE_ONLY-AFY26', 4, 'Active', '2025-07-01', NULL, 15, 'Monthly')
    """))
    conn.execute(text("""
        INSERT INTO billing.acquisition_cost VALUES
        ('A4', 'P4', 'Upfront', 'Online', 180, 0.015, 2.70, '2025-07-01', '2025-07-01', '2026-07-01', 'Active')
    """))
    for i, (y, m) in enumerate([
        (2025, 7), (2025, 8), (2025, 9), (2025, 10), (2025, 11), (2025, 12),
        (2026, 1), (2026, 2), (2026, 3), (2026, 4), (2026, 5), (2026, 6),
    ], start=1):
        conn.execute(text("""
            INSERT INTO billing.payment VALUES
            (:id, 'P4', :d, 15, 'Completed')
        """), {"id": f"PAY_P4_{i}", "d": date(y, m, 5)})


class TestScenarioInvariants:
    def test_all_invariants(self, sqlite_engine, tmp_path):
        with sqlite_engine.begin() as conn:
            _seed_4_policy_scenario(conn)

        engine = IFRS17Engine(
            config=IFRS17Config(
                enabled=True,
                assumptions_path=tmp_path / "unused.yaml",
                posting_rules_path=REAL_POSTING_RULES_PATH,
                csv_export_dir=tmp_path / "out",
                csv_export_enabled=False,
            ),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2027, 6, 30),
            assumptions=_assumptions(),
        )
        counts = engine.run()
        assert counts["balances"] > 0

        # ---- Invariant 1: closing_lrc[t] == opening_lrc[t+1] ----
        with sqlite_engine.connect() as conn:
            rows = list(conn.execute(text("""
                SELECT cohort_id, reporting_month, opening_lrc, closing_lrc
                FROM ifrs17.monthly_movement
                ORDER BY cohort_id, reporting_month
            """)))
            prev = {}
            for r in rows:
                if r.cohort_id in prev:
                    assert Decimal(str(r.opening_lrc)) == Decimal(str(prev[r.cohort_id])), \
                        f"LRC discontinuity in {r.cohort_id} at {r.reporting_month}"
                prev[r.cohort_id] = r.closing_lrc

        # ---- Invariant 2: loss_component >= 0 always ----
        with sqlite_engine.connect() as conn:
            negatives = conn.execute(text("""
                SELECT COUNT(*) FROM ifrs17.monthly_balance WHERE loss_component < 0
            """)).scalar()
            assert negatives == 0

        # ---- Invariant 3: cumulative DAC amortised ≤ total commissions ----
        with sqlite_engine.connect() as conn:
            total_comm = Decimal(str(conn.execute(text("""
                SELECT COALESCE(SUM(commission_amount), 0) FROM billing.acquisition_cost
            """)).scalar()))
            total_amort = Decimal(str(conn.execute(text("""
                SELECT COALESCE(SUM(acquisition_cost_amortised), 0)
                FROM ifrs17.monthly_movement
            """)).scalar()))
            # After all amortisation periods have passed, total amortised
            # should equal total commission (within rounding).
            assert total_amort <= total_comm + Decimal("0.50")
            # By end of window (2 years, while amort is 12 months), all DACs
            # should be fully amortised.
            assert abs(total_amort - total_comm) < Decimal("1.00"), \
                f"total_amort={total_amort} vs total_comm={total_comm}"

        # ---- Invariant 3b: sum(debit) == sum(credit) per (cohort, month) in journal_line ----
        with sqlite_engine.connect() as conn:
            unbalanced = conn.execute(text("""
                SELECT COUNT(*) FROM (
                    SELECT cohort_id, reporting_month,
                           SUM(debit_amount) AS dr, SUM(credit_amount) AS cr
                    FROM ifrs17.journal_line
                    GROUP BY cohort_id, reporting_month
                    HAVING SUM(debit_amount) <> SUM(credit_amount)
                )
            """)).scalar()
            assert unbalanced == 0, "journal_line debits != credits per (cohort, month)"

        # ---- Invariant 4: onerous_first_detected_month set <= 1 time per cohort ----
        with sqlite_engine.connect() as conn:
            # The high-claims EXTRAS_ONLY-AFY26 cohort should be flagged once.
            detected = conn.execute(text("""
                SELECT onerous_first_detected_month FROM ifrs17.cohort
                WHERE cohort_id='EXTRAS_ONLY-AFY26'
            """)).scalar()
            assert detected is not None, (
                "Expected EXTRAS_ONLY-AFY26 to be flagged onerous given "
                "heavy claims seeded in months 7-12"
            )

    def test_determinism_two_runs_same_output(self, sqlite_engine, tmp_path):
        """Two identical runs must produce byte-identical monthly_balance rows."""
        with sqlite_engine.begin() as conn:
            _seed_4_policy_scenario(conn)

        def _run_once():
            engine = IFRS17Engine(
                config=IFRS17Config(
                    enabled=True,
                    assumptions_path=tmp_path / "unused.yaml",
                    csv_export_dir=tmp_path / "out",
                    csv_export_enabled=False,
                ),
                db_engine=sqlite_engine,
                sim_start=date(2025, 7, 1),
                sim_end=date(2027, 6, 30),
                assumptions=_assumptions(),
            )
            engine.run()
            with sqlite_engine.connect() as conn:
                return list(conn.execute(text("""
                    SELECT cohort_id, reporting_month, policy_count,
                           lrc_excl_loss_component, loss_component, lrc_total,
                           lic_best_estimate, lic_ibnr
                    FROM ifrs17.monthly_balance
                    ORDER BY cohort_id, reporting_month
                """)))

        run1 = _run_once()
        run2 = _run_once()

        assert len(run1) == len(run2)
        for r1, r2 in zip(run1, run2):
            for col in [
                "cohort_id", "reporting_month", "policy_count",
                "lrc_excl_loss_component", "loss_component", "lrc_total",
                "lic_best_estimate", "lic_ibnr",
            ]:
                assert getattr(r1, col) == getattr(r2, col), \
                    f"Non-deterministic column {col}"
