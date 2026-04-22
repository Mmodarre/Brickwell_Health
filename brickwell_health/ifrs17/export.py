"""
CSV export for IFRS 17 fact/dim tables.

Primary path: ``cursor.copy_expert("COPY ... TO STDOUT WITH CSV HEADER", file)``
via the psycopg connection underlying the SQLAlchemy engine — streaming and
memory-safe.

Fallback for managed Postgres instances where COPY is not permitted:
``pandas.read_sql(...).to_csv(...)`` triggered by ``mode='pandas'``.

Two export granularities are supported:

* **full** (default): one CSV per table containing all months.
* **monthly**: one CSV per table per reporting month, written into
  ``YYYY-MM/`` subdirectories so that downstream pipelines can ingest
  data incrementally — mirroring a real-life monthly close cadence.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import structlog
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = structlog.get_logger()


# Tables in the order they are exported. Each entry is (table, csv_stem).
_EXPORT_TARGETS: list[tuple[str, str]] = [
    ("ifrs17.cohort", "ifrs17_cohort"),
    ("ifrs17.monthly_balance", "ifrs17_monthly_balance"),
    ("ifrs17.monthly_movement", "ifrs17_monthly_movement"),
    ("ifrs17.onerous_assessment", "ifrs17_onerous_assessment"),
    ("billing.acquisition_cost", "ifrs17_acquisition_cost"),
    # Phase 2: finance dim snapshots + journal-line fact
    ("reference.gl_account", "gl_account_snapshot"),
    ("reference.gl_account_hierarchy", "gl_account_hierarchy_snapshot"),
    ("reference.gl_period", "gl_period_snapshot"),
    ("reference.cost_centre", "cost_centre_snapshot"),
    ("ifrs17.journal_line", "ifrs17_journal_line"),
]

# Fact tables that carry a reporting_month column and should be split in
# monthly export mode.  Maps table name → date column used for filtering.
_MONTHLY_TABLES: dict[str, str] = {
    "ifrs17.monthly_balance": "reporting_month",
    "ifrs17.monthly_movement": "reporting_month",
    "ifrs17.onerous_assessment": "reporting_month",
    "ifrs17.journal_line": "reporting_month",
    "billing.acquisition_cost": "incurred_date",
}


# ── full export ──────────────────────────────────────────────────────────────


def export_all(
    db_engine: Engine,
    out_dir: Path | str,
    run_id: str,
    mode: Literal["copy", "pandas"] = "copy",
) -> list[Path]:
    """Export every IFRS 17 table to CSV under ``out_dir``.

    Returns the list of files written.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for table, stem in _EXPORT_TARGETS:
        filename = f"{stem}_{run_id}.csv"
        out_path = out_dir / filename
        _export_table(db_engine, table, out_path, mode)
        written.append(out_path)
    return written


# ── monthly export ───────────────────────────────────────────────────────────


def export_monthly(
    db_engine: Engine,
    out_dir: Path | str,
    run_id: str,
    mode: Literal["copy", "pandas"] = "copy",
) -> list[Path]:
    """Export IFRS 17 tables split by reporting month.

    Fact tables (those in ``_MONTHLY_TABLES``) are written as one CSV per
    month under ``<out_dir>/YYYY-MM/<stem>_YYYY-MM.csv``.

    Dimension / reference tables are written once into
    ``<out_dir>/_reference/<stem>.csv``.

    Returns the full list of files written (all months + reference).
    """
    out_dir = Path(out_dir)
    written: list[Path] = []

    # Discover distinct months from the primary fact table.
    months = _get_distinct_months(db_engine, "ifrs17.monthly_balance", "reporting_month")
    if not months:
        logger.warning("ifrs17_monthly_export_no_data")
        return written

    logger.info("ifrs17_monthly_export_starting", months=len(months))

    for table, stem in _EXPORT_TARGETS:
        date_col = _MONTHLY_TABLES.get(table)
        if date_col is not None:
            # ── fact table: one file per month ───────────────────────────
            for month_str in months:
                month_dir = out_dir / month_str
                month_dir.mkdir(parents=True, exist_ok=True)
                out_path = month_dir / f"{stem}_{month_str}.csv"
                where = (
                    f"TO_CHAR({date_col}, 'YYYY-MM') = '{month_str}'"
                    if date_col == "incurred_date"
                    else f"{date_col} >= '{month_str}-01'::date "
                    f"AND {date_col} < ('{month_str}-01'::date + INTERVAL '1 month')"
                )
                _export_table_filtered(db_engine, table, out_path, where, mode)
                written.append(out_path)
        else:
            # ── dim / reference table: export once ───────────────────────
            ref_dir = out_dir / "_reference"
            ref_dir.mkdir(parents=True, exist_ok=True)
            out_path = ref_dir / f"{stem}.csv"
            _export_table(db_engine, table, out_path, mode)
            written.append(out_path)

    logger.info("ifrs17_monthly_export_completed", files=len(written))
    return written


def _get_distinct_months(
    db_engine: Engine, table: str, date_col: str
) -> list[str]:
    """Return sorted list of YYYY-MM strings from a fact table."""
    sql = text(
        f"SELECT DISTINCT TO_CHAR({date_col}, 'YYYY-MM') AS m "
        f"FROM {table} ORDER BY m"
    )
    with db_engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [r[0] for r in rows]


# ── low-level export helpers ─────────────────────────────────────────────────


def _export_table(
    db_engine: Engine,
    table: str,
    out_path: Path,
    mode: Literal["copy", "pandas"],
) -> None:
    """Export a full table to *out_path*, with copy→pandas fallback."""
    if mode == "copy":
        ok = _export_copy(db_engine, f"SELECT * FROM {table}", out_path)
        if not ok:
            logger.warning(
                "ifrs17_export_copy_failed_fallback_pandas", table=table,
            )
            _export_pandas(db_engine, f"SELECT * FROM {table}", out_path)
    else:
        _export_pandas(db_engine, f"SELECT * FROM {table}", out_path)


def _export_table_filtered(
    db_engine: Engine,
    table: str,
    out_path: Path,
    where: str,
    mode: Literal["copy", "pandas"],
) -> None:
    """Export a filtered subset of a table to *out_path*."""
    query = f"SELECT * FROM {table} WHERE {where}"
    if mode == "copy":
        ok = _export_copy(db_engine, query, out_path)
        if not ok:
            logger.warning(
                "ifrs17_export_copy_failed_fallback_pandas", table=table,
            )
            _export_pandas(db_engine, query, out_path)
    else:
        _export_pandas(db_engine, query, out_path)


def _export_copy(db_engine: Engine, query: str, out_path: Path) -> bool:
    """Streaming COPY export. Returns True on success."""
    sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
    try:
        with db_engine.connect() as conn:
            raw_conn = conn.connection.dbapi_connection
            with raw_conn.cursor() as cur, open(out_path, "w", newline="") as f:
                # psycopg2 has copy_expert; psycopg3 has copy()
                copy_expert = getattr(cur, "copy_expert", None)
                if copy_expert is not None:
                    copy_expert(sql, f)
                else:  # psycopg3 path
                    with cur.copy(sql) as cp:
                        for chunk in cp:
                            if chunk:
                                # chunk is bytes
                                f.write(chunk.decode("utf-8"))
        return True
    except Exception as e:
        logger.warning(
            "ifrs17_copy_export_error", query=query[:80], path=str(out_path), error=str(e)
        )
        return False


def _export_pandas(db_engine: Engine, query: str, out_path: Path) -> None:
    """Pandas fallback. Imports pandas lazily."""
    try:
        import pandas as pd  # noqa: PLC0415
    except Exception as e:  # pragma: no cover
        logger.error("ifrs17_pandas_unavailable", error=str(e))
        raise

    df = pd.read_sql(query, db_engine)
    df.to_csv(out_path, index=False)
