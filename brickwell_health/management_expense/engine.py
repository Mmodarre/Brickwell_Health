"""
Management Expense Engine.

Post-simulation engine that generates fund-level monthly management expense
GL journal entries. Reads policy/member/premium data from the simulation
database and applies configurable cost driver rates per expense category.

Follows the same architectural pattern as the IFRS17Engine: single-process,
month-major iteration, BatchWriter for database writes.
"""

from __future__ import annotations

import calendar
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import text
from sqlalchemy.engine import Engine

from brickwell_health.config.management_expense import ManagementExpenseConfig
from brickwell_health.db.writer import BatchWriter
from brickwell_health.domain.management_expense import FinanceJournalLineCreate
from brickwell_health.management_expense.categories import (
    ExpenseCategory,
    load_categories,
    validate_against_gl,
)

logger = structlog.get_logger()

ZERO = Decimal("0")
TWELVE = Decimal("12")
TWO_DP = Decimal("0.01")


# ---------------------------------------------------------------------------
# Helpers (reused from ifrs17.engine patterns)
# ---------------------------------------------------------------------------

def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _first_day_of_month(year: int, month: int) -> date:
    return date(year, month, 1)


def _month_range(start: date, end: date) -> list[tuple[date, date]]:
    """Return list of (first_of_month, last_of_month) for every month in [start, end]."""
    months: list[tuple[date, date]] = []
    y, m = start.year, start.month
    end_y, end_m = end.year, end.month
    while (y, m) <= (end_y, end_m):
        months.append((_first_day_of_month(y, m), _last_day_of_month(y, m)))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def _d(value) -> Decimal:
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


# ---------------------------------------------------------------------------
# Annualisation factors for premium frequency
# ---------------------------------------------------------------------------

_PERIODS_PER_YEAR = {
    "monthly": Decimal("12"),
    "fortnightly": Decimal("26"),
    "weekly": Decimal("52"),
    "quarterly": Decimal("4"),
    "half-yearly": Decimal("2"),
    "halfyearly": Decimal("2"),
    "annually": Decimal("1"),
    "annual": Decimal("1"),
    "yearly": Decimal("1"),
}


def _periods_per_year(frequency: str) -> Decimal:
    return _PERIODS_PER_YEAR.get(frequency.lower(), Decimal("12"))


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class ManagementExpenseEngine:
    """Post-simulation management expense journal line generator."""

    def __init__(
        self,
        config: ManagementExpenseConfig,
        db_engine: Engine,
        sim_start: date,
        sim_end: date,
    ):
        self.config = config
        self.engine = db_engine
        self.sim_start = sim_start
        self.sim_end = sim_end

        # Load and validate categories
        self.categories = load_categories(str(config.categories_path))

        # Build lookup maps from reference tables
        self._gl_account_by_code = self._load_gl_account_map()
        self._cost_centre_by_code = self._load_cost_centre_map()
        self._gl_period_by_month = self._load_gl_period_map()

        # Validate all category references resolve
        validate_against_gl(
            self.categories,
            self._gl_account_by_code,
            self._cost_centre_by_code,
        )

        # BatchWriter for high-performance writes
        self.batch_writer = BatchWriter(db_engine, batch_size=5000)

        logger.info(
            "management_expense_engine_init",
            categories=len(self.categories),
            gl_accounts=len(self._gl_account_by_code),
            cost_centres=len(self._cost_centre_by_code),
        )

    def run(self) -> dict[str, int]:
        """Execute end-to-end. Returns counts dict."""
        self._truncate_fact_table()

        months = _month_range(self.sim_start, self.sim_end)
        total_lines = 0

        logger.info("management_expense_engine_starting", months=len(months))

        for month_start, month_end in months:
            total_lines += self._process_month(month_start, month_end)

        self.batch_writer.flush_all()

        logger.info(
            "management_expense_engine_finished",
            journal_lines=total_lines,
        )
        return {"journal_lines": total_lines}

    # ------------------------------------------------------------------
    # Month processing
    # ------------------------------------------------------------------

    def _process_month(self, month_start: date, month_end: date) -> int:
        """Compute and write journal lines for one month."""
        gl_period_id = self._gl_period_by_month.get(month_end)
        if gl_period_id is None:
            return 0

        # Fund-level metrics
        policy_count = self._policy_count_at(month_end)
        member_count = self._member_count_at(month_end)
        monthly_nep = self._net_earned_premium(month_start, month_end)

        lines_written = 0
        for cat in self.categories:
            amount = self._compute_amount(
                cat, policy_count, member_count, monthly_nep
            )
            if amount <= ZERO:
                continue
            self._write_journal_pair(cat, month_end, gl_period_id, amount)
            lines_written += 2

        return lines_written

    def _compute_amount(
        self,
        cat: ExpenseCategory,
        policy_count: int,
        member_count: int,
        monthly_nep: Decimal,
    ) -> Decimal:
        """Compute the monthly expense amount for a category."""
        rate = cat.annual_rate_per_unit
        if cat.cost_driver == "per_policy":
            return (rate * Decimal(policy_count) / TWELVE).quantize(TWO_DP)
        elif cat.cost_driver == "per_member":
            return (rate * Decimal(member_count) / TWELVE).quantize(TWO_DP)
        elif cat.cost_driver == "pct_nep":
            return (rate * monthly_nep).quantize(TWO_DP)
        elif cat.cost_driver == "fixed":
            return (rate / TWELVE).quantize(TWO_DP)
        return ZERO

    def _write_journal_pair(
        self,
        cat: ExpenseCategory,
        reporting_month: date,
        gl_period_id: int,
        amount: Decimal,
    ) -> None:
        """Write one debit + one credit FinanceJournalLineCreate."""
        cost_centre_id = self._cost_centre_by_code[cat.cost_centre_code]

        debit = FinanceJournalLineCreate(
            journal_line_id=uuid.uuid4(),
            reporting_month=reporting_month,
            gl_period_id=gl_period_id,
            gl_account_id=self._gl_account_by_code[cat.debit_account_code],
            cost_centre_id=cost_centre_id,
            expense_category=cat.category_id,
            debit_amount=amount,
            credit_amount=ZERO,
            description=cat.description,
        )
        credit = FinanceJournalLineCreate(
            journal_line_id=uuid.uuid4(),
            reporting_month=reporting_month,
            gl_period_id=gl_period_id,
            gl_account_id=self._gl_account_by_code[cat.credit_account_code],
            cost_centre_id=cost_centre_id,
            expense_category=cat.category_id,
            debit_amount=ZERO,
            credit_amount=amount,
            description=cat.description,
        )
        self.batch_writer.add("finance.journal_line", debit.model_dump_db())
        self.batch_writer.add("finance.journal_line", credit.model_dump_db())

    # ------------------------------------------------------------------
    # SQL queries — fund-level metrics
    # ------------------------------------------------------------------

    def _policy_count_at(self, month_end: date) -> int:
        sql = text("""
            SELECT COUNT(*) AS n
            FROM policy.policy p
            WHERE p.effective_date <= :month_end
              AND (p.end_date IS NULL OR p.end_date > :month_end)
              AND p.policy_status IN ('Active', 'Suspended')
        """)
        with self.engine.connect() as conn:
            row = conn.execute(sql, {"month_end": month_end}).one()
            return int(row.n or 0)

    def _member_count_at(self, month_end: date) -> int:
        sql = text("""
            SELECT COUNT(DISTINCT pm.member_id) AS n
            FROM policy.policy_member pm
            JOIN policy.policy p ON p.policy_id = pm.policy_id
            WHERE p.effective_date <= :month_end
              AND (p.end_date IS NULL OR p.end_date > :month_end)
              AND p.policy_status IN ('Active', 'Suspended')
        """)
        with self.engine.connect() as conn:
            row = conn.execute(sql, {"month_end": month_end}).one()
            return int(row.n or 0)

    def _net_earned_premium(
        self, month_start: date, month_end: date
    ) -> Decimal:
        """Fund-level NEP for the month using straight-line proration."""
        sql = text("""
            SELECT p.premium_amount, p.payment_frequency,
                   p.effective_date, p.end_date
            FROM policy.policy p
            WHERE p.effective_date <= :month_end
              AND (p.end_date IS NULL OR p.end_date >= :month_start)
              AND p.policy_status IN ('Active', 'Suspended')
        """)
        total = ZERO
        with self.engine.connect() as conn:
            rs = conn.execute(
                sql, {"month_start": month_start, "month_end": month_end}
            )
            for row in rs:
                eff = row.effective_date
                if hasattr(eff, "date"):
                    eff = eff.date()
                end = row.end_date
                if end is not None and hasattr(end, "date"):
                    end = end.date()

                period_start = max(eff, month_start)
                period_end = month_end if end is None else min(end, month_end)
                if period_end < period_start:
                    continue
                days = (period_end - period_start).days + 1
                annual = _d(row.premium_amount) * _periods_per_year(
                    row.payment_frequency
                )
                earned = annual * Decimal(days) / Decimal(365)
                total += earned
        return total.quantize(TWO_DP)

    # ------------------------------------------------------------------
    # Reference data loading
    # ------------------------------------------------------------------

    def _load_gl_account_map(self) -> dict[str, int]:
        """Build {account_code: account_id} from reference.gl_account."""
        sql = text(
            "SELECT account_id, account_code FROM reference.gl_account "
            "WHERE is_active = TRUE"
        )
        result: dict[str, int] = {}
        duplicates: list[str] = []
        with self.engine.connect() as conn:
            for row in conn.execute(sql):
                code = row.account_code
                if code in result:
                    duplicates.append(code)
                result[code] = row.account_id
        if duplicates:
            logger.warning(
                "management_expense_duplicate_account_codes",
                codes=duplicates,
                msg="Last-wins resolution applied; fix gl_account.json to remove ambiguity",
            )
        return result

    def _load_cost_centre_map(self) -> dict[str, int]:
        """Build {cost_centre_code: cost_centre_id} from reference.cost_centre."""
        sql = text(
            "SELECT cost_centre_id, cost_centre_code FROM reference.cost_centre "
            "WHERE is_active = TRUE"
        )
        with self.engine.connect() as conn:
            return {
                row.cost_centre_code: row.cost_centre_id
                for row in conn.execute(sql)
            }

    def _load_gl_period_map(self) -> dict[date, int]:
        """Build {end_date: period_id} from reference.gl_period."""
        sql = text(
            "SELECT period_id, end_date FROM reference.gl_period"
        )
        with self.engine.connect() as conn:
            result: dict[date, int] = {}
            for row in conn.execute(sql):
                end = row.end_date
                if hasattr(end, "date"):
                    end = end.date()
                result[end] = row.period_id
            return result

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def _truncate_fact_table(self) -> None:
        """Clear prior run data for idempotent re-runs."""
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM finance.journal_line"))
        logger.info("management_expense_table_truncated")
