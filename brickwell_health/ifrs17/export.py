"""
CSV export for IFRS 17 fact/dim tables.

Primary path: ``cursor.copy_expert("COPY ... TO STDOUT WITH CSV HEADER", file)``
via the psycopg connection underlying the SQLAlchemy engine — streaming and
memory-safe.

Fallback for managed Postgres instances where COPY is not permitted:
``pandas.read_sql(...).to_csv(...)`` triggered by ``mode='pandas'``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import structlog
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
        if mode == "copy":
            ok = _export_copy(db_engine, table, out_path)
            if not ok:
                # Fall back once per-table if COPY fails (e.g. permission denied)
                logger.warning(
                    "ifrs17_export_copy_failed_fallback_pandas",
                    table=table,
                )
                _export_pandas(db_engine, table, out_path)
        else:
            _export_pandas(db_engine, table, out_path)
        written.append(out_path)
    return written


def _export_copy(db_engine: Engine, table: str, out_path: Path) -> bool:
    """Streaming COPY export. Returns True on success."""
    sql = f"COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER"
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
            "ifrs17_copy_export_error", table=table, path=str(out_path), error=str(e)
        )
        return False


def _export_pandas(db_engine: Engine, table: str, out_path: Path) -> None:
    """Pandas fallback. Imports pandas lazily."""
    try:
        import pandas as pd  # noqa: PLC0415
    except Exception as e:  # pragma: no cover
        logger.error("ifrs17_pandas_unavailable", error=str(e))
        raise

    df = pd.read_sql(f"SELECT * FROM {table}", db_engine)
    df.to_csv(out_path, index=False)
