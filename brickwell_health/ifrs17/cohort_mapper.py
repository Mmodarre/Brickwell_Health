"""
Cohort mapper: assigns a cohort_id to each policy based on product type and
effective date.

Portfolios (4):
    HOSPITAL_ONLY   - only is_hospital
    EXTRAS_ONLY     - only is_extras
    COMBINED        - is_hospital AND is_extras (ambulance flag irrelevant)
    AMBULANCE_ONLY  - only is_ambulance

Cohorts are annual, aligned to the Australian Financial Year (July to June):
    AFY25  -> 2024-07-01 .. 2025-06-30
    AFY26  -> 2025-07-01 .. 2026-06-30
    ...

Cohort ID format: ``{PORTFOLIO}-AFY{YY}`` (e.g. ``HOSPITAL_ONLY-AFY25``).
"""

from __future__ import annotations

import json
from datetime import date
from functools import cache
from pathlib import Path
from typing import Optional


# -----------------------------------------------------------------------------
# Pure helpers (exposed for unit tests and engine pre-population)
# -----------------------------------------------------------------------------


def afy_start_year(d: date, afy_start_month: int = 7) -> int:
    """
    Return the calendar year in which the Australian Financial Year containing
    ``d`` *starts*. AFY25 starts 2024-07-01, so ``afy_start_year(2025-06-30)``
    returns 2024.
    """
    if d.month >= afy_start_month:
        return d.year
    return d.year - 1


def afy_label_for(d: date, afy_start_month: int = 7) -> str:
    """
    Return the AFY label (e.g. ``AFY25``) for a given date.

    The label uses the two-digit year of the AFY's *end* (so AFY25 = the year
    ending 2025-06-30). This matches ATO and industry conventions.
    """
    end_year = afy_start_year(d, afy_start_month) + 1
    return f"AFY{end_year % 100:02d}"


def afy_window(d: date, afy_start_month: int = 7) -> tuple[date, date]:
    """Return (start_date, end_date) for the AFY containing ``d`` (end inclusive)."""
    start_year = afy_start_year(d, afy_start_month)
    start = date(start_year, afy_start_month, 1)
    # end is last day of month before afy_start_month, next year
    end_month = afy_start_month - 1 if afy_start_month > 1 else 12
    end_year = start_year + 1 if afy_start_month > 1 else start_year
    # last day of end_month: first day of next month - 1 day
    from datetime import timedelta
    if end_month == 12:
        next_month_first = date(end_year + 1, 1, 1)
    else:
        next_month_first = date(end_year, end_month + 1, 1)
    end = next_month_first - timedelta(days=1)
    return start, end


def portfolio_from_flags(
    is_hospital: bool, is_extras: bool, is_ambulance: bool
) -> str:
    """
    Map product coverage flags to a portfolio string.

    Rules (per plan):
      - hospital AND extras      -> COMBINED (ambulance flag ignored)
      - hospital only            -> HOSPITAL_ONLY
      - extras only              -> EXTRAS_ONLY
      - ambulance only (sole)    -> AMBULANCE_ONLY
      - otherwise                -> COMBINED (defensive fallback)
    """
    if is_hospital and is_extras:
        return "COMBINED"
    if is_hospital and not is_extras:
        return "HOSPITAL_ONLY"
    if is_extras and not is_hospital:
        return "EXTRAS_ONLY"
    if is_ambulance and not is_hospital and not is_extras:
        return "AMBULANCE_ONLY"
    return "COMBINED"


def cohort_id(portfolio: str, afy_label: str) -> str:
    """Compose the cohort_id from portfolio and AFY label."""
    return f"{portfolio}-{afy_label}"


def all_afy_labels_in_range(
    start: date, end: date, afy_start_month: int = 7
) -> list[tuple[str, date, date]]:
    """
    Enumerate every AFY that overlaps the closed interval [start, end].

    Returns a list of (afy_label, afy_start_date, afy_end_date) tuples ordered
    by afy_start_date ascending.
    """
    out: list[tuple[str, date, date]] = []
    cur_start_year = afy_start_year(start, afy_start_month)
    while True:
        afy_start = date(cur_start_year, afy_start_month, 1)
        _, afy_end = afy_window(afy_start, afy_start_month)
        if afy_start > end:
            break
        out.append((afy_label_for(afy_start, afy_start_month), afy_start, afy_end))
        cur_start_year += 1
    return out


# -----------------------------------------------------------------------------
# CohortMapper: loads product.json once and caches (product_id -> portfolio)
# -----------------------------------------------------------------------------


class CohortMapper:
    """
    Resolves cohort_id for a policy at creation time.

    The mapper loads ``product.json`` once and builds a product_id -> portfolio
    lookup. Every subsequent ``cohort_id_for`` call is O(1).
    """

    def __init__(
        self,
        product_to_portfolio: dict[int, str],
        afy_start_month: int = 7,
    ):
        self._product_to_portfolio = product_to_portfolio
        self._afy_start_month = afy_start_month

    @classmethod
    def from_reference_path(
        cls,
        reference_path: Path | str,
        afy_start_month: int = 7,
    ) -> "CohortMapper":
        """Build a mapper by reading ``product.json`` from the reference dir."""
        path = Path(reference_path) / "product.json"
        return cls(
            product_to_portfolio=_load_product_portfolio_map(path),
            afy_start_month=afy_start_month,
        )

    def portfolio_for(self, product_id: int) -> str:
        """Return the portfolio for a given product_id."""
        if product_id not in self._product_to_portfolio:
            raise KeyError(f"Unknown product_id: {product_id}")
        return self._product_to_portfolio[product_id]

    def afy_label_for(self, d: date) -> str:
        return afy_label_for(d, self._afy_start_month)

    def cohort_id_for(self, effective_date: date, product_id: int) -> str:
        """Compose the cohort_id for a (date, product) pair."""
        portfolio = self.portfolio_for(product_id)
        return cohort_id(portfolio, self.afy_label_for(effective_date))

    def enumerate_cohorts(
        self, start: date, end: date
    ) -> list[tuple[str, str, str, date, date]]:
        """
        Return (cohort_id, portfolio, afy_label, afy_start, afy_end) for every
        (portfolio x AFY) overlapping [start, end].

        Used at DB init time to pre-populate ``ifrs17.cohort``.
        """
        portfolios = sorted(set(self._product_to_portfolio.values()))
        # Always include all 4 canonical portfolios even if no product uses one
        # yet (keeps grid completeness).
        canonical = ["HOSPITAL_ONLY", "EXTRAS_ONLY", "COMBINED", "AMBULANCE_ONLY"]
        for p in canonical:
            if p not in portfolios:
                portfolios.append(p)
        portfolios = sorted(portfolios)

        out: list[tuple[str, str, str, date, date]] = []
        for afy_label, afy_start, afy_end in all_afy_labels_in_range(
            start, end, self._afy_start_month
        ):
            for portfolio in portfolios:
                out.append(
                    (cohort_id(portfolio, afy_label), portfolio, afy_label, afy_start, afy_end)
                )
        return out


@cache
def _load_product_portfolio_map(path: Path) -> dict[int, str]:
    with open(path) as f:
        products = json.load(f)
    mapping: dict[int, str] = {}
    for p in products:
        mapping[int(p["product_id"])] = portfolio_from_flags(
            bool(p.get("is_hospital", False)),
            bool(p.get("is_extras", False)),
            bool(p.get("is_ambulance", False)),
        )
    return mapping
