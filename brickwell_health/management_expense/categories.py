"""Management expense category loader and validator."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Mapping

import structlog

from brickwell_health.config.loader import load_yaml

logger = structlog.get_logger()

VALID_COST_DRIVERS = {"per_policy", "per_member", "pct_nep", "fixed"}


class CategoryError(ValueError):
    """Raised for invalid category definitions."""


@dataclass(frozen=True)
class ExpenseCategory:
    """One expense category loaded from YAML."""

    category_id: str
    description: str
    cost_driver: str
    annual_rate_per_unit: Decimal
    debit_account_code: str
    credit_account_code: str
    cost_centre_code: str


def load_categories(path: Path | str) -> list[ExpenseCategory]:
    """Load and validate expense categories from YAML."""
    raw = load_yaml(Path(path))
    cats_cfg = raw.get("categories") if isinstance(raw, dict) else None
    if not isinstance(cats_cfg, list) or not cats_cfg:
        raise CategoryError(
            f"YAML at {path} must contain a non-empty 'categories' list"
        )

    categories: list[ExpenseCategory] = []
    seen_ids: set[str] = set()

    for i, entry in enumerate(cats_cfg):
        required = ("category_id", "cost_driver", "annual_rate_per_unit",
                     "debit_account_code", "credit_account_code", "cost_centre_code")
        missing = [k for k in required if k not in entry]
        if missing:
            raise CategoryError(
                f"Category #{i} missing required keys: {missing}"
            )

        driver = str(entry["cost_driver"])
        if driver not in VALID_COST_DRIVERS:
            raise CategoryError(
                f"Category '{entry['category_id']}' has invalid cost_driver "
                f"'{driver}'; must be one of {VALID_COST_DRIVERS}"
            )

        cat = ExpenseCategory(
            category_id=str(entry["category_id"]),
            description=str(entry.get("description", "")),
            cost_driver=driver,
            annual_rate_per_unit=Decimal(str(entry["annual_rate_per_unit"])),
            debit_account_code=str(entry["debit_account_code"]),
            credit_account_code=str(entry["credit_account_code"]),
            cost_centre_code=str(entry["cost_centre_code"]),
        )
        if cat.category_id in seen_ids:
            raise CategoryError(f"Duplicate category_id: {cat.category_id}")
        seen_ids.add(cat.category_id)
        categories.append(cat)

    return categories


def validate_against_gl(
    categories: list[ExpenseCategory],
    gl_account_by_code: Mapping[str, int],
    cost_centre_by_code: Mapping[str, int],
) -> None:
    """Abort if any account or cost centre code is unresolvable."""
    errors: list[str] = []
    for cat in categories:
        if cat.debit_account_code not in gl_account_by_code:
            errors.append(
                f"{cat.category_id}: debit_account_code "
                f"'{cat.debit_account_code}' not found in gl_account"
            )
        if cat.credit_account_code not in gl_account_by_code:
            errors.append(
                f"{cat.category_id}: credit_account_code "
                f"'{cat.credit_account_code}' not found in gl_account"
            )
        if cat.cost_centre_code not in cost_centre_by_code:
            errors.append(
                f"{cat.category_id}: cost_centre_code "
                f"'{cat.cost_centre_code}' not found in cost_centre"
            )
    if errors:
        raise CategoryError(
            f"GL validation failed ({len(errors)} errors):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
