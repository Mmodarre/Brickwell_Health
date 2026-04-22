"""
CSV export for management expense journal lines.

Reuses the low-level export helpers from the IFRS17 export module.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import structlog
from sqlalchemy import text
from sqlalchemy.engine import Engine

from brickwell_health.ifrs17.export import (
    _export_table,
    _export_table_filtered,
    _get_distinct_months,
)

logger = structlog.get_logger()

_EXPORT_TARGETS: list[tuple[str, str]] = [
    ("finance.journal_line", "finance_journal_line"),
]

_MONTHLY_TABLES: dict[str, str] = {
    "finance.journal_line": "reporting_month",
}


def export_all(
    db_engine: Engine,
    out_dir: Path | str,
    run_id: str,
    mode: Literal["copy", "pandas"] = "copy",
) -> list[Path]:
    """Export finance.journal_line to a single CSV."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for table, stem in _EXPORT_TARGETS:
        out_path = out_dir / f"{stem}_{run_id}.csv"
        _export_table(db_engine, table, out_path, mode)
        written.append(out_path)
    return written


def export_monthly(
    db_engine: Engine,
    out_dir: Path | str,
    run_id: str,
    mode: Literal["copy", "pandas"] = "copy",
) -> list[Path]:
    """Export finance.journal_line split by reporting month into YYYY-MM subdirs."""
    out_dir = Path(out_dir)
    written: list[Path] = []

    months = _get_distinct_months(
        db_engine, "finance.journal_line", "reporting_month"
    )
    if not months:
        logger.warning("management_expense_monthly_export_no_data")
        return written

    logger.info("management_expense_monthly_export_starting", months=len(months))

    for table, stem in _EXPORT_TARGETS:
        date_col = _MONTHLY_TABLES.get(table)
        if date_col is not None:
            for month_str in months:
                month_dir = out_dir / month_str
                month_dir.mkdir(parents=True, exist_ok=True)
                out_path = month_dir / f"{stem}_{month_str}.csv"
                where = (
                    f"{date_col} >= '{month_str}-01'::date "
                    f"AND {date_col} < ('{month_str}-01'::date + INTERVAL '1 month')"
                )
                _export_table_filtered(db_engine, table, out_path, where, mode)
                written.append(out_path)

    logger.info("management_expense_monthly_export_completed", files=len(written))
    return written
