"""
IFRS 17 / PAA configuration model.
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class IFRS17Config(BaseModel):
    """Configuration for the post-simulation IFRS 17 LRC engine."""

    enabled: bool = Field(
        default=False,
        description="Enable the IFRS 17 / PAA LRC engine post-sim step.",
    )
    assumptions_path: Path = Field(
        default=Path("data/reference/ifrs17_assumptions.yaml"),
        description="Path to the IFRS 17 numerical assumptions YAML.",
    )
    posting_rules_path: Path = Field(
        default=Path("data/reference/ifrs17_posting_rules.yaml"),
        description=(
            "Path to the IFRS 17 posting-rules YAML. Maps each movement bucket "
            "(insurance_revenue, premiums_received, claims_incurred, "
            "dac_amortised, loss_component_recognised, loss_component_reversed) "
            "to (debit_account_code, credit_account_code) in reference.gl_account."
        ),
    )
    csv_export_dir: Path = Field(
        default=Path("data/output/ifrs17"),
        description="Directory to write per-run CSV exports.",
    )
    csv_export_enabled: bool = Field(
        default=True,
        description="If True, export IFRS 17 tables as CSV after each run.",
    )
    discounting_enabled: bool = Field(
        default=False,
        description="If True, apply discounting to cashflows (off by default).",
    )
    csv_export_mode: Literal["copy", "pandas"] = Field(
        default="copy",
        description=(
            "Export mechanism: 'copy' uses psycopg2 COPY (fast, memory-safe); "
            "'pandas' falls back to pandas.read_sql/to_csv for managed Postgres "
            "instances where COPY is not permitted."
        ),
    )
    csv_export_granularity: Literal["full", "monthly"] = Field(
        default="full",
        description=(
            "Export granularity: 'full' writes one CSV per table (all months); "
            "'monthly' writes one CSV per table per reporting month, organised "
            "in YYYY-MM subdirectories — suitable for incremental ingestion."
        ),
    )
