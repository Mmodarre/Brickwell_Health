"""
IFRS 17 / PAA LRC engine.

Single-process, month-major iterator that computes LRC balance + movement +
onerous assessment per (cohort, reporting_month) from the transactional tables
produced by the simulation.

SQL is written to work on both Postgres (production) and SQLite (unit tests).
The queries therefore avoid Postgres-only constructs and always aggregate via
plain SQL expressions rather than window functions or CTE FILTERs.

Writes use the BatchWriter for consistency with the rest of the project and to
amortise COPY overhead. The fact tables (``ifrs17.monthly_balance``,
``ifrs17.monthly_movement``, ``ifrs17.onerous_assessment``) are TRUNCATEd at
the start of a run so reruns are idempotent.
"""

from __future__ import annotations

import calendar
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Iterator, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.engine import Engine

from brickwell_health.config.ifrs17 import IFRS17Config
from brickwell_health.db.writer import BatchWriter
from brickwell_health.domain.ifrs17 import (
    IFRS17JournalLineCreate,
    IFRS17MonthlyBalance,
    IFRS17MonthlyMovement,
    OnerousAssessment,
)
from brickwell_health.ifrs17.assumptions import IFRS17Assumptions, load_assumptions
from brickwell_health.ifrs17.posting import PostingRuleEngine, PostingRuleError


class _SimpleSQLAlchemyWriter:
    """Fallback writer for non-Postgres dialects (SQLite in tests).

    Buffers rows per-table and flushes via SQLAlchemy ``INSERT`` statements on
    ``flush_all()``. Intentionally tiny — no COPY, no dependency-ordering — so
    engine tests don't need psycopg.
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self._buffers: dict[str, list[dict]] = {}

    def add(self, table_name: str, record: dict) -> None:
        self._buffers.setdefault(table_name, []).append(record)

    def flush_all(self) -> None:
        for table, records in list(self._buffers.items()):
            if not records:
                continue
            cols = list(records[0].keys())
            col_list = ", ".join(cols)
            placeholders = ", ".join(f":{c}" for c in cols)
            sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
            with self.engine.begin() as conn:
                for row in records:
                    conn.execute(text(sql), _serialise(row))
            self._buffers[table] = []


def _serialise(row: dict) -> dict:
    """Convert values to types SQLite can bind directly (UUID->str, Decimal->float)."""
    import uuid as _uuid
    out = {}
    for k, v in row.items():
        if isinstance(v, _uuid.UUID):
            out[k] = str(v)
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, bool):
            out[k] = int(v)
        else:
            out[k] = v
    return out


logger = structlog.get_logger()

ZERO = Decimal("0")
HUNDRED = Decimal("100")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def last_day_of_month(year: int, month: int) -> date:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, last)


def first_day_of_month(year: int, month: int) -> date:
    return date(year, month, 1)


def month_range(start: date, end: date) -> Iterator[tuple[date, date]]:
    """Yield (first_of_month, last_of_month) covering every month that
    overlaps the closed interval [start, end]."""
    y, m = start.year, start.month
    end_y, end_m = end.year, end.month
    while (y, m) <= (end_y, end_m):
        yield first_day_of_month(y, m), last_day_of_month(y, m)
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def _d(value) -> Decimal:
    """Coerce DB-returned number/None to a Decimal (non-None)."""
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _date(value) -> Optional[date]:
    """Coerce DB-returned value (date/datetime/str/None) to a date (or None)."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    # SQLite returns ISO strings
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def _annualise_factor(frequency: str) -> Decimal:
    """Return months-per-period × 1 expressed as 12/N.

    Monthly=1, Fortnightly=13/6, Quarterly=3, etc. Callers typically want
    premium_amount × _annualise_factor / 12 = monthly, or × 12/_annualise_factor
    for annual GWP. We expose ``periods_per_year`` below instead for clarity.
    """
    return Decimal("1")


_PERIODS_PER_YEAR: dict[str, Decimal] = {
    "Monthly": Decimal("12"),
    "Fortnightly": Decimal("26"),
    "Weekly": Decimal("52"),
    "Quarterly": Decimal("4"),
    "Half-Yearly": Decimal("2"),
    "HalfYearly": Decimal("2"),
    "Annually": Decimal("1"),
    "Annual": Decimal("1"),
    "Yearly": Decimal("1"),
}


def periods_per_year(frequency: str) -> Decimal:
    return _PERIODS_PER_YEAR.get(frequency, Decimal("12"))


# ---------------------------------------------------------------------------
# Per-cohort state carried month-to-month
# ---------------------------------------------------------------------------


@dataclass
class _CohortState:
    """Rolling per-cohort state (stays in memory across months)."""

    cohort_id: str
    closing_lrc_excl_lc: Decimal = ZERO
    loss_component: Decimal = ZERO
    dac_balance: Decimal = ZERO
    months_history: int = 0
    # Track consecutive months below (threshold - hysteresis_band) to decide
    # when a loss component may be reversed.
    consecutive_months_below_reverse_threshold: int = 0
    # Track claims incurred per month for rolling-lag IBNR computation.
    claims_history: list[Decimal] = field(default_factory=list)
    onerous_first_detected: Optional[date] = None


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class IFRS17Engine:
    """Post-simulation IFRS 17 / PAA LRC compute."""

    def __init__(
        self,
        config: IFRS17Config,
        db_engine: Engine,
        sim_start: date,
        sim_end: date,
        assumptions: Optional[IFRS17Assumptions] = None,
        batch_writer: Optional[BatchWriter] = None,
    ):
        self.config = config
        self.engine = db_engine
        self.sim_start = sim_start
        self.sim_end = sim_end
        self.assumptions = assumptions or load_assumptions(str(config.assumptions_path))
        if batch_writer is not None:
            self.batch_writer = batch_writer
        elif db_engine.dialect.name == "postgresql":
            self.batch_writer = BatchWriter(db_engine, batch_size=10000)
        else:
            self.batch_writer = _SimpleSQLAlchemyWriter(db_engine)

        # Per-cohort rolling state keyed by cohort_id
        self._state: dict[str, _CohortState] = {}

        # Phase 2 wiring: posting rules + GL dim maps for journal-line emission.
        # Validation happens immediately so an unresolved account code fails at
        # engine startup rather than mid-run.
        self._posting = PostingRuleEngine.from_yaml(config.posting_rules_path)
        self._gl_account_by_code: dict[str, int] = self._load_account_code_map()
        self._gl_period_by_month: dict[date, int] = self._load_period_map()
        self._posting.validate_against_accounts(self._gl_account_by_code)

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def run(self) -> dict[str, int]:
        """Execute the engine end-to-end. Returns row counts for each fact table."""
        logger.info("ifrs17_engine_starting")
        self._truncate_fact_tables()

        cohorts = self._load_cohorts()
        if not cohorts:
            logger.warning("ifrs17_engine_no_cohorts")
            return {"balances": 0, "movements": 0, "assessments": 0}

        # Iterate month-major so each month's prior closing is available as
        # the current opening.
        months = list(month_range(self.sim_start, self.sim_end))
        counts = {"balances": 0, "movements": 0, "assessments": 0}

        for month_start, month_end in months:
            for cohort in cohorts:
                cohort_id = cohort["cohort_id"]
                state = self._state.setdefault(
                    cohort_id, _CohortState(cohort_id=cohort_id)
                )

                metrics = self._compute_month(cohort, month_start, month_end, state)
                self._write_month(cohort_id, month_end, metrics)
                counts["balances"] += 1
                counts["movements"] += 1
                counts["assessments"] += 1

                # Persist onerous_first_detected on the cohort table once.
                if (
                    state.onerous_first_detected is not None
                    and cohort.get("onerous_first_detected_month") is None
                ):
                    self._persist_cohort_onerous_first_detected(
                        cohort_id, state.onerous_first_detected
                    )
                    cohort["onerous_first_detected_month"] = state.onerous_first_detected

        self.batch_writer.flush_all()
        logger.info("ifrs17_engine_finished", **counts)
        return counts

    # ------------------------------------------------------------------
    # DB preparation
    # ------------------------------------------------------------------

    def _load_account_code_map(self) -> dict[str, int]:
        """Return ``{account_code: account_id}`` from ``reference.gl_account``."""
        with self.engine.connect() as conn:
            try:
                rs = conn.execute(
                    text(
                        "SELECT account_code, account_id FROM reference.gl_account"
                    )
                )
                return {str(row[0]): int(row[1]) for row in rs}
            except Exception as e:
                raise PostingRuleError(
                    f"Failed to load reference.gl_account for posting-rule "
                    f"validation: {e}"
                ) from e

    def _load_period_map(self) -> dict[date, int]:
        """Return ``{end_date: period_id}`` keyed by month-end date."""
        with self.engine.connect() as conn:
            try:
                rs = conn.execute(
                    text(
                        "SELECT end_date, period_id FROM reference.gl_period"
                    )
                )
                out: dict[date, int] = {}
                for row in rs:
                    end_d = _date(row[0])
                    if end_d is not None:
                        out[end_d] = int(row[1])
                return out
            except Exception as e:
                raise PostingRuleError(
                    f"Failed to load reference.gl_period for journal-line "
                    f"emission: {e}"
                ) from e

    def _truncate_fact_tables(self) -> None:
        """Clear fact tables so reruns are idempotent."""
        dialect = self.engine.dialect.name
        # journal_line first (no dependents); then the 3 originals.
        fact_tables = [
            "ifrs17.journal_line",
            "ifrs17.onerous_assessment",
            "ifrs17.monthly_movement",
            "ifrs17.monthly_balance",
        ]
        with self.engine.connect() as conn:
            for tbl in fact_tables:
                try:
                    if dialect == "sqlite":
                        # SQLite with ATTACHed schemas accepts "schema.table"
                        # unquoted for DELETE.
                        conn.execute(text(f"DELETE FROM {tbl}"))
                    else:
                        conn.execute(text(f"TRUNCATE {tbl}"))
                except Exception as e:
                    logger.warning("ifrs17_truncate_failed", table=tbl, error=str(e))
            conn.commit()

    def _load_cohorts(self) -> list[dict]:
        """Load all cohorts (engine does not care which have policies — zeros
        are perfectly valid rows to emit for grid completeness)."""
        with self.engine.connect() as conn:
            rs = conn.execute(
                text(
                    """
                    SELECT cohort_id, portfolio, afy_label,
                           afy_start_date, afy_end_date,
                           onerous_first_detected_month
                    FROM ifrs17.cohort
                    ORDER BY afy_start_date, portfolio
                    """
                )
            )
            return [dict(row._mapping) for row in rs]

    def _persist_cohort_onerous_first_detected(
        self, cohort_id: str, detected_month: date
    ) -> None:
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    UPDATE ifrs17.cohort
                    SET onerous_first_detected_month = :m
                    WHERE cohort_id = :c
                    """
                ),
                {"m": detected_month, "c": cohort_id},
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Per-month compute
    # ------------------------------------------------------------------

    def _compute_month(
        self,
        cohort: dict,
        month_start: date,
        month_end: date,
        state: _CohortState,
    ) -> dict:
        """Compute all metrics for a single (cohort, month). Returns a dict
        with keys consumed by ``_write_month``."""
        cohort_id = cohort["cohort_id"]
        days_in_month = (month_end - month_start).days + 1

        # ----- Policy / in-force -----
        policy_count, in_force_annual_premium = self._in_force_stats(cohort_id, month_end)

        # ----- Premiums received this month -----
        premiums_received = self._premiums_received(cohort_id, month_start, month_end)

        # ----- Insurance revenue (earned premium, straight-line proration) ----
        insurance_revenue = self._earned_premium_straight_line(
            cohort_id, month_start, month_end
        )
        # Apply optional seasonal multiplier
        if self.assumptions.earning_seasonal_weights:
            idx = month_start.month - 1
            weights = self.assumptions.earning_seasonal_weights
            if 0 <= idx < len(weights):
                insurance_revenue = (insurance_revenue * Decimal(str(weights[idx]))).quantize(
                    Decimal("0.01")
                )

        # ----- Claims incurred (service_date in month) -----
        claims_incurred = self._claims_incurred(cohort_id, month_start, month_end)

        # ----- DAC amortisation + residual on lapses -----
        dac_amort_regular = self._dac_amortised_regular(
            cohort_id, month_start, month_end
        )
        dac_amort_lapse = self._dac_amortised_lapses(
            cohort_id, month_start, month_end
        )
        dac_amortised = (dac_amort_regular + dac_amort_lapse).quantize(Decimal("0.01"))

        # ----- Outstanding LIC best estimate (assessed/approved but not paid) -
        lic_be = self._lic_best_estimate(cohort_id, month_end)
        lic_ra = (lic_be * self.assumptions.ra_uplift_pct).quantize(Decimal("0.01"))

        # ----- IBNR (rolling lag, scaled when insufficient history) ---------
        state.claims_history.append(claims_incurred)
        if len(state.claims_history) > self.assumptions.ibnr_lookback_months:
            state.claims_history = state.claims_history[
                -self.assumptions.ibnr_lookback_months :
            ]
        history_months = len(state.claims_history)
        scale = Decimal(min(history_months, 3)) / Decimal(3)
        if history_months > 0:
            avg_recent = sum(state.claims_history, ZERO) / Decimal(history_months)
        else:
            avg_recent = ZERO
        lic_ibnr = (avg_recent * self.assumptions.ibnr_lag_factor * scale).quantize(
            Decimal("0.01")
        )
        lic_total = (lic_be + lic_ra + lic_ibnr).quantize(Decimal("0.01"))

        # ----- LRC roll-forward -----
        opening_lrc_excl_lc = state.closing_lrc_excl_lc
        opening_loss_component = state.loss_component
        opening_lrc_total = (opening_lrc_excl_lc + opening_loss_component).quantize(
            Decimal("0.01")
        )

        closing_lrc_excl_lc = (
            opening_lrc_excl_lc
            + premiums_received
            - insurance_revenue
            - dac_amortised
        ).quantize(Decimal("0.01"))

        # DAC balance update
        new_dac_issued = self._new_dac_issued(cohort_id, month_start, month_end)
        state.dac_balance = (
            state.dac_balance + new_dac_issued - dac_amortised
        ).quantize(Decimal("0.01"))

        # ----- Onerous assessment (skipped when below thresholds) -----
        state.months_history += 1
        (
            onerous_result,
            loss_component_recognised,
            loss_component_reversed,
        ) = self._assess_onerous(
            cohort_id=cohort_id,
            month_end=month_end,
            policy_count=policy_count,
            state=state,
            closing_lrc_excl_lc=closing_lrc_excl_lc,
            in_force_annual_premium=in_force_annual_premium,
            claims_incurred_this_month=claims_incurred,
        )

        # Update loss component on state
        new_loss_component = (
            opening_loss_component
            + loss_component_recognised
            - loss_component_reversed
        )
        if new_loss_component < ZERO:
            new_loss_component = ZERO
        state.loss_component = new_loss_component.quantize(Decimal("0.01"))

        closing_lrc_total = (closing_lrc_excl_lc + state.loss_component).quantize(
            Decimal("0.01")
        )

        # Track first detection
        if state.loss_component > ZERO and state.onerous_first_detected is None:
            state.onerous_first_detected = month_end

        # ----- Save closing for next month -----
        state.closing_lrc_excl_lc = closing_lrc_excl_lc

        # Insurance service expense = claims incurred + DAC amortised +
        # loss component recognised - loss component reversed. Reversal
        # decreases expense (shows as a negative contribution).
        insurance_service_expense = (
            claims_incurred
            + dac_amortised
            + loss_component_recognised
            - loss_component_reversed
        ).quantize(Decimal("0.01"))

        insurance_service_result = (
            insurance_revenue - insurance_service_expense
        ).quantize(Decimal("0.01"))

        return {
            "policy_count": policy_count,
            "in_force_premium": in_force_annual_premium,
            "premiums_received": premiums_received,
            "insurance_revenue": insurance_revenue,
            "insurance_service_expense": insurance_service_expense,
            "claims_incurred": claims_incurred,
            "dac_amortised": dac_amortised,
            "dac_balance": state.dac_balance,
            "loss_component": state.loss_component,
            "loss_component_recognised": loss_component_recognised.quantize(
                Decimal("0.01")
            ),
            "loss_component_reversed": loss_component_reversed.quantize(
                Decimal("0.01")
            ),
            "lic_be": lic_be,
            "lic_ra": lic_ra,
            "lic_ibnr": lic_ibnr,
            "lic_total": lic_total,
            "opening_lrc": opening_lrc_total,
            "closing_lrc_excl_lc": closing_lrc_excl_lc,
            "closing_lrc_total": closing_lrc_total,
            "insurance_service_result": insurance_service_result,
            "onerous": onerous_result,
        }

    # ------------------------------------------------------------------
    # SQL aggregations
    # ------------------------------------------------------------------

    def _in_force_stats(self, cohort_id: str, month_end: date) -> tuple[int, Decimal]:
        """Return (policy_count, annualised in-force premium at month_end)."""
        sql = text(
            """
            SELECT
                COUNT(*) AS n,
                COALESCE(SUM(
                    p.premium_amount * CASE LOWER(p.payment_frequency)
                        WHEN 'monthly' THEN 12
                        WHEN 'fortnightly' THEN 26
                        WHEN 'weekly' THEN 52
                        WHEN 'quarterly' THEN 4
                        WHEN 'half-yearly' THEN 2
                        WHEN 'halfyearly' THEN 2
                        WHEN 'annually' THEN 1
                        WHEN 'annual' THEN 1
                        WHEN 'yearly' THEN 1
                        ELSE 12
                    END
                ), 0) AS annual_premium
            FROM policy.policy p
            WHERE p.ifrs17_cohort_id = :cohort
              AND p.effective_date <= :month_end
              AND (p.end_date IS NULL OR p.end_date > :month_end)
              AND p.policy_status IN ('Active', 'Suspended')
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(
                sql, {"cohort": cohort_id, "month_end": month_end}
            ).one()
            return int(row.n or 0), _d(row.annual_premium)

    def _premiums_received(
        self, cohort_id: str, month_start: date, month_end: date
    ) -> Decimal:
        sql = text(
            """
            SELECT COALESCE(SUM(pay.payment_amount), 0) AS total
            FROM billing.payment pay
            JOIN policy.policy p ON p.policy_id = pay.policy_id
            WHERE p.ifrs17_cohort_id = :cohort
              AND pay.payment_date BETWEEN :start AND :end
              AND pay.payment_status IN ('Completed', 'Cleared', 'Settled')
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(
                sql, {"cohort": cohort_id, "start": month_start, "end": month_end}
            ).one()
            return _d(row.total).quantize(Decimal("0.01"))

    def _earned_premium_straight_line(
        self, cohort_id: str, month_start: date, month_end: date
    ) -> Decimal:
        """
        Straight-line earned premium for policies in-force during the month.

        For each policy, compute annual_premium × days_in_force_in_month / 365.
        This pro-rates partial months at both the policy start (late-month
        effective dates) and policy end (mid-month lapses).
        """
        sql = text(
            """
            SELECT p.policy_id, p.premium_amount, p.payment_frequency,
                   p.effective_date, p.end_date
            FROM policy.policy p
            WHERE p.ifrs17_cohort_id = :cohort
              AND p.effective_date <= :month_end
              AND (p.end_date IS NULL OR p.end_date >= :month_start)
            """
        )
        total = ZERO
        with self.engine.connect() as conn:
            rs = conn.execute(
                sql, {"cohort": cohort_id, "month_start": month_start, "month_end": month_end}
            )
            for row in rs:
                eff = _date(row.effective_date)
                end = _date(row.end_date)
                period_start = max(eff, month_start)
                period_end = month_end if end is None else min(end, month_end)
                if period_end < period_start:
                    continue
                days = (period_end - period_start).days + 1
                annual = _d(row.premium_amount) * periods_per_year(row.payment_frequency)
                earned = annual * Decimal(days) / Decimal(365)
                total += earned
        return total.quantize(Decimal("0.01"))

    def _claims_incurred(
        self, cohort_id: str, month_start: date, month_end: date
    ) -> Decimal:
        """Claims incurred this month (service_date in month, any status)."""
        sql = text(
            """
            SELECT COALESCE(SUM(c.total_charge), 0) AS total
            FROM claims.claim c
            JOIN policy.policy p ON p.policy_id = c.policy_id
            WHERE p.ifrs17_cohort_id = :cohort
              AND c.service_date BETWEEN :start AND :end
              AND c.claim_status <> 'Rejected'
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(
                sql, {"cohort": cohort_id, "start": month_start, "end": month_end}
            ).one()
            return _d(row.total).quantize(Decimal("0.01"))

    def _dac_amortised_regular(
        self, cohort_id: str, month_start: date, month_end: date
    ) -> Decimal:
        """Straight-line DAC amortisation for policies NOT lapsing this month.

        The end-of-window filter uses ``amortisation_end_date > :month_end``
        (not ``> :month_start``) so that a policy whose amort window ends
        mid-month is correctly excluded from the month that contains its end
        — otherwise we would recognise one extra ``commission_amount / N``
        cycle after the amort period has lapsed, causing cumulative DAC
        amortised to exceed commission incurred.
        """
        sql = text(
            """
            SELECT a.commission_amount, a.amortisation_start_date,
                   a.amortisation_end_date, p.end_date
            FROM billing.acquisition_cost a
            JOIN policy.policy p ON p.policy_id = a.policy_id
            WHERE p.ifrs17_cohort_id = :cohort
              AND a.amortisation_start_date <= :month_end
              AND a.amortisation_end_date > :month_end
              AND (p.end_date IS NULL OR p.end_date > :month_end)
              AND a.status = 'Active'
            """
        )
        total = ZERO
        with self.engine.connect() as conn:
            rs = conn.execute(
                sql, {"cohort": cohort_id, "month_start": month_start, "month_end": month_end}
            )
            for row in rs:
                a_start = _date(row.amortisation_start_date)
                a_end = _date(row.amortisation_end_date)
                months = _months_between(a_start, a_end)
                if months <= 0:
                    continue
                monthly = _d(row.commission_amount) / Decimal(months)
                total += monthly
        return total.quantize(Decimal("0.01"))

    def _dac_amortised_lapses(
        self, cohort_id: str, month_start: date, month_end: date
    ) -> Decimal:
        """
        Residual DAC recognised in the lapse month.

        For policies where ``end_date`` falls within the month, any remaining
        un-amortised commission is recognised immediately (IFRS 17 PAA
        derecognition). Computed as:
            remaining = commission_amount × months_remaining / total_months
        where months_remaining is from month_start to amortisation_end_date.
        """
        sql = text(
            """
            SELECT a.commission_amount, a.amortisation_start_date,
                   a.amortisation_end_date, p.end_date
            FROM billing.acquisition_cost a
            JOIN policy.policy p ON p.policy_id = a.policy_id
            WHERE p.ifrs17_cohort_id = :cohort
              AND p.end_date BETWEEN :month_start AND :month_end
              AND a.status = 'Active'
            """
        )
        total = ZERO
        with self.engine.connect() as conn:
            rs = conn.execute(
                sql, {"cohort": cohort_id, "month_start": month_start, "month_end": month_end}
            )
            for row in rs:
                a_start = _date(row.amortisation_start_date)
                a_end = _date(row.amortisation_end_date)
                total_months = _months_between(a_start, a_end)
                if total_months <= 0:
                    continue
                # Months already amortised by month_start (exclusive)
                months_elapsed = max(
                    0,
                    _months_between(a_start, month_start),
                )
                if months_elapsed >= total_months:
                    continue
                months_remaining = total_months - months_elapsed
                monthly = _d(row.commission_amount) / Decimal(total_months)
                residual = monthly * Decimal(months_remaining)
                total += residual
        return total.quantize(Decimal("0.01"))

    def _new_dac_issued(
        self, cohort_id: str, month_start: date, month_end: date
    ) -> Decimal:
        """Commission amounts incurred (policies issued) this month."""
        sql = text(
            """
            SELECT COALESCE(SUM(a.commission_amount), 0) AS total
            FROM billing.acquisition_cost a
            JOIN policy.policy p ON p.policy_id = a.policy_id
            WHERE p.ifrs17_cohort_id = :cohort
              AND a.incurred_date BETWEEN :start AND :end
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(
                sql, {"cohort": cohort_id, "start": month_start, "end": month_end}
            ).one()
            return _d(row.total).quantize(Decimal("0.01"))

    def _lic_best_estimate(self, cohort_id: str, month_end: date) -> Decimal:
        """
        LIC best estimate = approved/assessed claims not yet paid as of
        month_end. We approximate this by summing ``total_benefit`` (or
        ``total_charge`` when benefit null) for claims whose ``service_date``
        is on or before month_end with status not in terminal-paid/rejected.
        """
        sql = text(
            """
            SELECT COALESCE(SUM(COALESCE(c.total_benefit, c.total_charge)), 0) AS total
            FROM claims.claim c
            JOIN policy.policy p ON p.policy_id = c.policy_id
            WHERE p.ifrs17_cohort_id = :cohort
              AND c.service_date <= :month_end
              AND c.claim_status IN ('Submitted', 'Assessed', 'Approved')
              AND (c.payment_date IS NULL OR c.payment_date > :month_end)
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(
                sql, {"cohort": cohort_id, "month_end": month_end}
            ).one()
            return _d(row.total).quantize(Decimal("0.01"))

    # ------------------------------------------------------------------
    # Onerous assessment
    # ------------------------------------------------------------------

    def _assess_onerous(
        self,
        cohort_id: str,
        month_end: date,
        policy_count: int,
        state: _CohortState,
        closing_lrc_excl_lc: Decimal,
        in_force_annual_premium: Decimal,
        claims_incurred_this_month: Decimal,
    ) -> tuple[dict, Decimal, Decimal]:
        """
        Decide whether the cohort is onerous this month and produce the
        ``OnerousAssessment`` payload plus (recognised, reversed) loss
        component deltas.
        """
        assumptions = self.assumptions
        threshold = assumptions.onerous_threshold
        hysteresis = assumptions.onerous_hysteresis_band
        reverse_threshold = threshold - hysteresis

        # Skip when too few policies or too little history.
        skipped = (
            policy_count < assumptions.onerous_min_policies
            or state.months_history < assumptions.onerous_min_months_history
        )

        result: dict = {
            "expected_remaining_premium": ZERO,
            "expected_remaining_claims": ZERO,
            "expected_remaining_expenses": ZERO,
            "expected_combined_ratio": None,
            "onerous_threshold_crossed": False,
            "notes": None,
        }

        if skipped:
            result["notes"] = (
                f"skipped: policies={policy_count} "
                f"history={state.months_history}"
            )
            return result, ZERO, ZERO

        # Expected remaining premium = closing LRC excl LC (straight-line
        # proxy) or remaining annual premium / 12 × remaining_months, but the
        # LRC value already represents this so we use it directly.
        expected_remaining_premium = max(closing_lrc_excl_lc, ZERO)

        # Expected remaining claims. Use rolling loss ratio when history ok;
        # else the default loss ratio.
        if state.claims_history:
            total_claims_recent = sum(state.claims_history, ZERO)
            # Rough recent-revenue proxy: assume in_force_annual_premium earned
            # uniformly over len(claims_history) months.
            recent_earned = (
                in_force_annual_premium
                * Decimal(len(state.claims_history))
                / Decimal(12)
            )
            if recent_earned > 0:
                loss_ratio = total_claims_recent / recent_earned
            else:
                loss_ratio = assumptions.onerous_default_loss_ratio
        else:
            loss_ratio = assumptions.onerous_default_loss_ratio

        expected_remaining_claims = (expected_remaining_premium * loss_ratio).quantize(
            Decimal("0.01")
        )

        # Expected remaining expenses: unamortised DAC + per-policy admin
        # over the remaining period (approximated as closing DAC + admin × 12
        # × policy_count pro-rated by the in-force months fraction).
        admin_monthly = assumptions.onerous_per_policy_monthly_admin
        # Assume a conservative 12-month horizon worth of admin for cohort
        # remainder estimate; this dominates only for very thin cohorts.
        expected_remaining_expenses = (
            state.dac_balance
            + admin_monthly * Decimal(policy_count) * Decimal(12)
        ).quantize(Decimal("0.01"))

        if expected_remaining_premium > ZERO:
            combined_ratio = (
                expected_remaining_claims + expected_remaining_expenses
            ) / expected_remaining_premium
        else:
            combined_ratio = Decimal("99.99")

        result["expected_remaining_premium"] = expected_remaining_premium
        result["expected_remaining_claims"] = expected_remaining_claims
        result["expected_remaining_expenses"] = expected_remaining_expenses
        result["expected_combined_ratio"] = combined_ratio.quantize(Decimal("0.0001"))

        crossed = combined_ratio > threshold
        result["onerous_threshold_crossed"] = bool(crossed)

        recognised = ZERO
        reversed_amt = ZERO

        if crossed:
            # Target loss component = excess of expected outflows over LRC
            target_lc = (
                expected_remaining_claims
                + expected_remaining_expenses
                - expected_remaining_premium
            )
            if target_lc < ZERO:
                target_lc = ZERO
            delta = target_lc - state.loss_component
            if delta > ZERO:
                recognised = delta
            state.consecutive_months_below_reverse_threshold = 0
        else:
            # Below threshold but only reverse when strictly below
            # (threshold - hysteresis) for 2 consecutive months.
            if combined_ratio < reverse_threshold:
                state.consecutive_months_below_reverse_threshold += 1
                if (
                    state.consecutive_months_below_reverse_threshold >= 2
                    and state.loss_component > ZERO
                ):
                    # Gradual reversal: 50% of current LC
                    reversed_amt = state.loss_component / Decimal(2)
            else:
                state.consecutive_months_below_reverse_threshold = 0

        result["notes"] = (
            f"threshold={threshold} band={hysteresis} cr={combined_ratio:.4f}"
        )
        return result, recognised, reversed_amt

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _write_month(
        self, cohort_id: str, reporting_month: date, m: dict
    ) -> None:
        # Resolve gl_period_id from the reporting_month (month-end date).
        gl_period_id = self._gl_period_by_month.get(reporting_month)
        if gl_period_id is None:
            # A missing period indicates gl_period extension at init-db did not
            # cover the sim window. Log once per missing month and continue with
            # NULL so journal emission is skipped for this month.
            logger.warning(
                "ifrs17_missing_gl_period",
                cohort_id=cohort_id,
                reporting_month=str(reporting_month),
            )

        balance = IFRS17MonthlyBalance(
            monthly_balance_id=uuid.uuid4(),
            cohort_id=cohort_id,
            reporting_month=reporting_month,
            policy_count=m["policy_count"],
            in_force_premium=m["in_force_premium"],
            lrc_excl_loss_component=m["closing_lrc_excl_lc"],
            loss_component=m["loss_component"],
            lrc_total=m["closing_lrc_total"],
            lic_best_estimate=m["lic_be"],
            lic_risk_adjustment=m["lic_ra"],
            lic_ibnr=m["lic_ibnr"],
            lic_total=m["lic_total"],
            deferred_acquisition_cost=m["dac_balance"],
            is_onerous=bool(m["loss_component"] > ZERO),
        )

        movement = IFRS17MonthlyMovement(
            monthly_movement_id=uuid.uuid4(),
            cohort_id=cohort_id,
            reporting_month=reporting_month,
            opening_lrc=m["opening_lrc"],
            premiums_received=m["premiums_received"],
            insurance_revenue=m["insurance_revenue"],
            insurance_service_expense=m["insurance_service_expense"],
            claims_incurred=m["claims_incurred"],
            acquisition_cost_amortised=m["dac_amortised"],
            loss_component_recognised=m["loss_component_recognised"],
            loss_component_reversed=m["loss_component_reversed"],
            closing_lrc=m["closing_lrc_total"],
            insurance_service_result=m["insurance_service_result"],
        )

        assessment = OnerousAssessment(
            assessment_id=uuid.uuid4(),
            cohort_id=cohort_id,
            reporting_month=reporting_month,
            expected_remaining_premium=m["onerous"]["expected_remaining_premium"],
            expected_remaining_claims=m["onerous"]["expected_remaining_claims"],
            expected_remaining_expenses=m["onerous"]["expected_remaining_expenses"],
            expected_combined_ratio=m["onerous"]["expected_combined_ratio"],
            onerous_threshold_crossed=m["onerous"]["onerous_threshold_crossed"],
            loss_component_change=(
                m["loss_component_recognised"] - m["loss_component_reversed"]
            ),
            notes=m["onerous"].get("notes"),
        )

        # Phase 2: attach GL period to the 3 existing facts.
        balance_row = balance.model_dump_db()
        balance_row["gl_period_id"] = gl_period_id
        movement_row = movement.model_dump_db()
        movement_row["gl_period_id"] = gl_period_id
        assessment_row = assessment.model_dump_db()
        assessment_row["gl_period_id"] = gl_period_id

        self.batch_writer.add("ifrs17.monthly_balance", balance_row)
        self.batch_writer.add("ifrs17.monthly_movement", movement_row)
        self.batch_writer.add("ifrs17.onerous_assessment", assessment_row)

        # Phase 2: emit double-entry journal lines for every non-zero bucket.
        # Requires a resolved gl_period_id (enforced NOT NULL in Postgres).
        if gl_period_id is not None:
            lines = self._posting.build_lines(
                cohort_id=cohort_id,
                reporting_month=reporting_month,
                gl_period_id=gl_period_id,
                gl_account_by_code=self._gl_account_by_code,
                movement=m,
            )
            for line in lines:
                self.batch_writer.add(
                    "ifrs17.journal_line", line.model_dump_db()
                )


def _months_between(start: date, end: date) -> int:
    """Approx number of whole months between two dates (>=0)."""
    if end <= start:
        return 0
    return (end.year - start.year) * 12 + (end.month - start.month)
