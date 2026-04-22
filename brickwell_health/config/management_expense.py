"""
Management expense engine configuration model.
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class ManagementExpenseConfig(BaseModel):
    """Configuration for the post-simulation management expense engine."""

    enabled: bool = Field(
        default=False,
        description="Enable the management expense engine post-sim step.",
    )
    categories_path: Path = Field(
        default=Path("data/reference/management_expense_categories.yaml"),
        description="Path to the expense category definitions YAML.",
    )
    csv_export_dir: Path = Field(
        default=Path("data/output/management_expenses"),
        description="Directory to write per-run CSV exports.",
    )
    csv_export_enabled: bool = Field(
        default=True,
        description="If True, export management expense tables as CSV after each run.",
    )
    csv_export_mode: Literal["copy", "pandas"] = Field(
        default="copy",
        description="Export mechanism: 'copy' or 'pandas'.",
    )
    csv_export_granularity: Literal["full", "monthly"] = Field(
        default="full",
        description=(
            "Export granularity: 'full' writes one CSV (all months); "
            "'monthly' writes per-month CSVs in YYYY-MM subdirectories."
        ),
    )
