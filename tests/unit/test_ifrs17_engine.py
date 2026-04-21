"""
Unit tests for the IFRS 17 engine.

These tests run against SQLite in-memory so the suite stays fast and does not
require Postgres. The engine SQL is written to be dialect-neutral; the few
Postgres-only constructs (TRUNCATE, schema qualification) are handled by the
engine itself (falls back to DELETE on SQLite and quoted 'schema.table'
identifiers via the explicit schema-bridge tables created here).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from brickwell_health.config.ifrs17 import IFRS17Config
from brickwell_health.ifrs17.assumptions import IFRS17Assumptions
from brickwell_health.ifrs17.engine import (
    IFRS17Engine,
    _months_between,
    month_range,
    periods_per_year,
)


REAL_POSTING_RULES_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "reference" / "ifrs17_posting_rules.yaml"
)


# Account codes referenced by the shipped posting rules; IDs are arbitrary.
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


# Mapping to let SQLite hold schema-qualified table names (policy.policy,
# billing.payment, etc.) via ATTACH DATABASE — we bridge via attached schemas.
SCHEMA_DEFS = [
    ("policy", "policy"),
    ("billing", "payment"),
    ("billing", "acquisition_cost"),
    ("claims", "claim"),
    ("ifrs17", "cohort"),
    ("ifrs17", "monthly_balance"),
    ("ifrs17", "monthly_movement"),
    ("ifrs17", "onerous_assessment"),
    ("ifrs17", "journal_line"),
    ("reference", "gl_account"),
    ("reference", "gl_period"),
]


@pytest.fixture
def sqlite_engine(tmp_path):
    """
    SQLite engine with schemas attached so schema-qualified table names work.

    We use file-based databases for each logical schema, attach them all to a
    main connection, and create the engine pointed at the main DB with a
    ``creator`` hook that re-attaches on every new connection (SQLAlchemy
    may open multiple connections).
    """
    main = tmp_path / "main.sqlite"
    schemas = ["policy", "billing", "claims", "ifrs17", "reference"]

    import sqlite3

    def _creator():
        conn = sqlite3.connect(str(main))
        for s in schemas:
            conn.execute(f"ATTACH DATABASE '{tmp_path / (s + '.sqlite')}' AS {s}")
        # SQLAlchemy expects isolation_level=None so autocommit flows
        return conn

    engine = create_engine(
        "sqlite://", creator=_creator, future=True,
    )

    # Create tables. SQLite doesn't support DECIMAL natively but treats it as
    # NUMERIC which preserves arithmetic for our purposes.
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE policy.policy (
                policy_id TEXT PRIMARY KEY,
                ifrs17_cohort_id TEXT,
                product_id INTEGER,
                policy_status TEXT,
                effective_date DATE,
                end_date DATE,
                premium_amount NUMERIC,
                payment_frequency TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE billing.payment (
                payment_id TEXT PRIMARY KEY,
                policy_id TEXT,
                payment_date DATE,
                payment_amount NUMERIC,
                payment_status TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE billing.acquisition_cost (
                acquisition_cost_id TEXT PRIMARY KEY,
                policy_id TEXT,
                commission_type TEXT,
                distribution_channel TEXT,
                gross_written_premium NUMERIC,
                commission_rate NUMERIC,
                commission_amount NUMERIC,
                incurred_date DATE,
                amortisation_start_date DATE,
                amortisation_end_date DATE,
                status TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE claims.claim (
                claim_id TEXT PRIMARY KEY,
                policy_id TEXT,
                service_date DATE,
                lodgement_date DATE,
                payment_date DATE,
                total_charge NUMERIC,
                total_benefit NUMERIC,
                claim_status TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE ifrs17.cohort (
                cohort_id TEXT PRIMARY KEY,
                portfolio TEXT,
                afy_label TEXT,
                afy_start_date DATE,
                afy_end_date DATE,
                is_onerous_at_inception INTEGER DEFAULT 0,
                onerous_first_detected_month DATE
            )
        """))
        conn.execute(text("""
            CREATE TABLE ifrs17.monthly_balance (
                monthly_balance_id TEXT PRIMARY KEY,
                cohort_id TEXT,
                reporting_month DATE,
                policy_count INTEGER,
                in_force_premium NUMERIC,
                lrc_excl_loss_component NUMERIC,
                loss_component NUMERIC,
                lrc_total NUMERIC,
                lic_best_estimate NUMERIC,
                lic_risk_adjustment NUMERIC,
                lic_ibnr NUMERIC,
                lic_total NUMERIC,
                deferred_acquisition_cost NUMERIC,
                is_onerous INTEGER,
                created_at TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE ifrs17.monthly_movement (
                monthly_movement_id TEXT PRIMARY KEY,
                cohort_id TEXT,
                reporting_month DATE,
                opening_lrc NUMERIC,
                premiums_received NUMERIC,
                insurance_revenue NUMERIC,
                insurance_service_expense NUMERIC,
                claims_incurred NUMERIC,
                acquisition_cost_amortised NUMERIC,
                loss_component_recognised NUMERIC,
                loss_component_reversed NUMERIC,
                closing_lrc NUMERIC,
                insurance_service_result NUMERIC,
                created_at TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE ifrs17.onerous_assessment (
                assessment_id TEXT PRIMARY KEY,
                cohort_id TEXT,
                reporting_month DATE,
                expected_remaining_premium NUMERIC,
                expected_remaining_claims NUMERIC,
                expected_remaining_expenses NUMERIC,
                expected_combined_ratio NUMERIC,
                onerous_threshold_crossed INTEGER,
                loss_component_change NUMERIC,
                notes TEXT,
                gl_period_id INTEGER,
                created_at TEXT
            )
        """))
        # Phase 2: finance dims + journal-line fact. NUMERIC column for debit/
        # credit is sufficient under SQLite; journal_line_id is TEXT since we
        # store UUIDs as strings (same as the other fact tables).
        conn.execute(text("""
            CREATE TABLE reference.gl_account (
                account_id INTEGER PRIMARY KEY,
                account_code TEXT UNIQUE,
                account_name TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE reference.gl_period (
                period_id INTEGER PRIMARY KEY,
                period_code TEXT UNIQUE,
                start_date DATE,
                end_date DATE
            )
        """))
        conn.execute(text("""
            CREATE TABLE ifrs17.journal_line (
                journal_line_id TEXT PRIMARY KEY,
                cohort_id TEXT,
                reporting_month DATE,
                gl_period_id INTEGER,
                gl_account_id INTEGER,
                cost_centre_id INTEGER,
                movement_bucket TEXT,
                debit_amount NUMERIC,
                credit_amount NUMERIC,
                journal_source TEXT,
                created_at TEXT
            )
        """))
        # Update the monthly_balance + monthly_movement tables retroactively so
        # they accept the gl_period_id column the engine now always populates.
        conn.execute(text("ALTER TABLE ifrs17.monthly_balance ADD COLUMN gl_period_id INTEGER"))
        conn.execute(text("ALTER TABLE ifrs17.monthly_movement ADD COLUMN gl_period_id INTEGER"))

        # Seed the chart of accounts with the codes the posting rules reference.
        for acc_id, code in GL_ACCOUNT_SEED:
            conn.execute(
                text(
                    "INSERT INTO reference.gl_account "
                    "(account_id, account_code, account_name) "
                    "VALUES (:i, :c, :n)"
                ),
                {"i": acc_id, "c": code, "n": f"test-{code}"},
            )

        # Seed gl_period for every month from 2025-01 to 2028-12 — covers every
        # sim window used by the tests.
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


def _assumptions(**overrides) -> IFRS17Assumptions:
    defaults = dict(
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
        onerous_min_policies=1,  # lower for tests
        onerous_min_months_history=1,
        onerous_default_loss_ratio=Decimal("0.85"),
        onerous_per_policy_monthly_admin=Decimal("5.00"),
        discounting_enabled=False,
        discount_rate=Decimal("0.035"),
        portfolios=["HOSPITAL_ONLY", "EXTRAS_ONLY", "COMBINED", "AMBULANCE_ONLY"],
        afy_start_month=7,
    )
    defaults.update(overrides)
    return IFRS17Assumptions(**defaults)


def _ifrs17_config(tmp_path) -> IFRS17Config:
    return IFRS17Config(
        enabled=True,
        assumptions_path=tmp_path / "does_not_matter.yaml",
        posting_rules_path=REAL_POSTING_RULES_PATH,
        csv_export_dir=tmp_path / "out",
        csv_export_enabled=False,
    )


class TestHelpers:
    def test_month_range_end_of_month(self):
        months = list(month_range(date(2025, 1, 15), date(2025, 3, 1)))
        assert months == [
            (date(2025, 1, 1), date(2025, 1, 31)),
            (date(2025, 2, 1), date(2025, 2, 28)),
            (date(2025, 3, 1), date(2025, 3, 31)),
        ]

    def test_months_between(self):
        assert _months_between(date(2025, 1, 1), date(2026, 1, 1)) == 12
        assert _months_between(date(2025, 1, 1), date(2025, 1, 1)) == 0

    def test_periods_per_year(self):
        assert periods_per_year("Monthly") == Decimal("12")
        assert periods_per_year("Quarterly") == Decimal("4")
        assert periods_per_year("unknown_frequency") == Decimal("12")


class TestEngineStraightLineEarning:
    def test_single_policy_full_month_earns_annual_over_12(self, sqlite_engine, tmp_path):
        """A policy in force for a full month should earn annual_premium × days/365."""
        with sqlite_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ifrs17.cohort (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
                VALUES ('HOSPITAL_ONLY-AFY26', 'HOSPITAL_ONLY', 'AFY26', '2025-07-01', '2026-06-30')
            """))
            conn.execute(text("""
                INSERT INTO policy.policy
                (policy_id, ifrs17_cohort_id, product_id, policy_status, effective_date,
                 end_date, premium_amount, payment_frequency)
                VALUES ('p1', 'HOSPITAL_ONLY-AFY26', 1, 'Active', '2025-07-01',
                        NULL, 200.0, 'Monthly')
            """))

        engine = IFRS17Engine(
            config=_ifrs17_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2025, 7, 31),
            assumptions=_assumptions(),
        )
        counts = engine.run()
        # 1 cohort × 1 month
        assert counts["balances"] == 1

        with sqlite_engine.connect() as conn:
            row = conn.execute(text("""
                SELECT insurance_revenue FROM ifrs17.monthly_movement
                WHERE cohort_id='HOSPITAL_ONLY-AFY26'
            """)).one()
            revenue = Decimal(str(row.insurance_revenue))
            # 200 * 12 = 2400 annual, July has 31 days -> 2400 * 31/365 ≈ 203.84
            expected = Decimal("2400") * Decimal(31) / Decimal(365)
            assert abs(revenue - expected) < Decimal("0.02")


class TestEngineIBNRScaling:
    def test_ibnr_scales_with_history(self, sqlite_engine, tmp_path):
        """With <3 months of claims history, IBNR should be scaled down."""
        with sqlite_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ifrs17.cohort (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
                VALUES ('HOSPITAL_ONLY-AFY26', 'HOSPITAL_ONLY', 'AFY26', '2025-07-01', '2026-06-30')
            """))
            conn.execute(text("""
                INSERT INTO policy.policy
                (policy_id, ifrs17_cohort_id, product_id, policy_status, effective_date,
                 end_date, premium_amount, payment_frequency)
                VALUES ('p1', 'HOSPITAL_ONLY-AFY26', 1, 'Active', '2025-07-01',
                        NULL, 200.0, 'Monthly')
            """))
            # Claim in month 1 so that IBNR computes from 1-month history, scaled by 1/3.
            conn.execute(text("""
                INSERT INTO claims.claim
                (claim_id, policy_id, service_date, lodgement_date, total_charge,
                 total_benefit, claim_status, payment_date)
                VALUES ('c1', 'p1', '2025-07-15', '2025-07-15', 1000.0,
                        1000.0, 'Paid', '2025-07-20')
            """))

        engine = IFRS17Engine(
            config=_ifrs17_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2025, 7, 31),
            assumptions=_assumptions(),
        )
        engine.run()

        with sqlite_engine.connect() as conn:
            ibnr = Decimal(
                str(conn.execute(text("""
                    SELECT lic_ibnr FROM ifrs17.monthly_balance
                    WHERE cohort_id='HOSPITAL_ONLY-AFY26'
                """)).scalar())
            )
            # Expected: avg(1000) * 0.15 * (1/3) = 50
            assert ibnr == Decimal("50.00")


class TestEngineDACAmortisation:
    def test_dac_amortises_straight_line(self, sqlite_engine, tmp_path):
        """An upfront commission of $120 amortised over 12 months should
        recognise $10 per month."""
        with sqlite_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ifrs17.cohort (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
                VALUES ('HOSPITAL_ONLY-AFY26', 'HOSPITAL_ONLY', 'AFY26', '2025-07-01', '2026-06-30')
            """))
            conn.execute(text("""
                INSERT INTO policy.policy
                (policy_id, ifrs17_cohort_id, product_id, policy_status, effective_date,
                 end_date, premium_amount, payment_frequency)
                VALUES ('p1', 'HOSPITAL_ONLY-AFY26', 1, 'Active', '2025-07-01',
                        NULL, 200.0, 'Monthly')
            """))
            conn.execute(text("""
                INSERT INTO billing.acquisition_cost
                (acquisition_cost_id, policy_id, commission_type, distribution_channel,
                 gross_written_premium, commission_rate, commission_amount,
                 incurred_date, amortisation_start_date, amortisation_end_date, status)
                VALUES ('a1', 'p1', 'Upfront', 'Online', 2400.0, 0.05, 120.0,
                        '2025-07-01', '2025-07-01', '2026-07-01', 'Active')
            """))

        engine = IFRS17Engine(
            config=_ifrs17_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2025, 8, 31),
            assumptions=_assumptions(),
        )
        engine.run()

        with sqlite_engine.connect() as conn:
            amort = [
                Decimal(str(r[0]))
                for r in conn.execute(text("""
                    SELECT acquisition_cost_amortised
                    FROM ifrs17.monthly_movement
                    WHERE cohort_id='HOSPITAL_ONLY-AFY26'
                    ORDER BY reporting_month
                """))
            ]
            assert amort == [Decimal("10.00"), Decimal("10.00")]


class TestEngineLapseDerecognition:
    def test_residual_dac_recognised_at_lapse(self, sqlite_engine, tmp_path):
        """When a policy lapses mid-coverage, remaining DAC should be
        recognised in the lapse month."""
        with sqlite_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ifrs17.cohort (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
                VALUES ('HOSPITAL_ONLY-AFY26', 'HOSPITAL_ONLY', 'AFY26', '2025-07-01', '2026-06-30')
            """))
            # Policy lapses after 3 months (Sep 30, 2025).
            conn.execute(text("""
                INSERT INTO policy.policy
                (policy_id, ifrs17_cohort_id, product_id, policy_status, effective_date,
                 end_date, premium_amount, payment_frequency)
                VALUES ('p1', 'HOSPITAL_ONLY-AFY26', 1, 'Cancelled', '2025-07-01',
                        '2025-09-30', 200.0, 'Monthly')
            """))
            conn.execute(text("""
                INSERT INTO billing.acquisition_cost
                (acquisition_cost_id, policy_id, commission_type, distribution_channel,
                 gross_written_premium, commission_rate, commission_amount,
                 incurred_date, amortisation_start_date, amortisation_end_date, status)
                VALUES ('a1', 'p1', 'Upfront', 'Online', 2400.0, 0.05, 120.0,
                        '2025-07-01', '2025-07-01', '2026-07-01', 'Active')
            """))

        engine = IFRS17Engine(
            config=_ifrs17_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2025, 12, 31),
            assumptions=_assumptions(),
        )
        engine.run()

        with sqlite_engine.connect() as conn:
            rows = list(conn.execute(text("""
                SELECT reporting_month, acquisition_cost_amortised
                FROM ifrs17.monthly_movement
                WHERE cohort_id='HOSPITAL_ONLY-AFY26'
                ORDER BY reporting_month
            """)))
            # First 2 months: $10 regular amortisation.
            assert Decimal(str(rows[0].acquisition_cost_amortised)) == Decimal("10.00")
            assert Decimal(str(rows[1].acquisition_cost_amortised)) == Decimal("10.00")
            # Month 3 (lapse month): residual DAC = 10 * 10 = 100 recognised.
            assert Decimal(str(rows[2].acquisition_cost_amortised)) == Decimal("100.00")
            # After lapse: zero amortisation.
            assert Decimal(str(rows[3].acquisition_cost_amortised)) == Decimal("0.00")


class TestEngineRollForwardContinuity:
    def test_closing_equals_next_opening(self, sqlite_engine, tmp_path):
        """closing_lrc[t] must equal opening_lrc[t+1] for every cohort."""
        with sqlite_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ifrs17.cohort (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
                VALUES ('HOSPITAL_ONLY-AFY26', 'HOSPITAL_ONLY', 'AFY26', '2025-07-01', '2026-06-30')
            """))
            conn.execute(text("""
                INSERT INTO policy.policy
                (policy_id, ifrs17_cohort_id, product_id, policy_status, effective_date,
                 end_date, premium_amount, payment_frequency)
                VALUES ('p1', 'HOSPITAL_ONLY-AFY26', 1, 'Active', '2025-07-01',
                        NULL, 200.0, 'Monthly')
            """))
            conn.execute(text("""
                INSERT INTO billing.payment
                (payment_id, policy_id, payment_date, payment_amount, payment_status)
                VALUES ('pay1', 'p1', '2025-07-10', 200.0, 'Completed'),
                       ('pay2', 'p1', '2025-08-10', 200.0, 'Completed'),
                       ('pay3', 'p1', '2025-09-10', 200.0, 'Completed')
            """))

        engine = IFRS17Engine(
            config=_ifrs17_config(tmp_path),
            db_engine=sqlite_engine,
            sim_start=date(2025, 7, 1),
            sim_end=date(2025, 10, 31),
            assumptions=_assumptions(),
        )
        engine.run()

        with sqlite_engine.connect() as conn:
            rows = list(conn.execute(text("""
                SELECT reporting_month, opening_lrc, closing_lrc
                FROM ifrs17.monthly_movement
                WHERE cohort_id='HOSPITAL_ONLY-AFY26'
                ORDER BY reporting_month
            """)))
            assert len(rows) == 4
            for i in range(1, len(rows)):
                assert Decimal(str(rows[i].opening_lrc)) == Decimal(
                    str(rows[i - 1].closing_lrc)
                )
            # Month 1 opening is zero
            assert Decimal(str(rows[0].opening_lrc)) == Decimal("0.00")
