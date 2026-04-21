"""
IFRS 17 numerical assumptions loader.

Wraps the YAML file produced alongside reference data, exposing typed accessors
so engine code does not scatter dictionary lookups.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from functools import cache
from pathlib import Path
from typing import Optional

from brickwell_health.config.loader import load_yaml


@dataclass(frozen=True)
class IFRS17Assumptions:
    """Typed view of ifrs17_assumptions.yaml."""

    # commission
    commission_rates_by_channel: dict[str, Decimal]
    commission_default_rate: Decimal
    commission_amortisation_months: int

    # risk adjustment
    ra_uplift_pct: Decimal
    ra_confidence_level: float

    # IBNR
    ibnr_lookback_months: int
    ibnr_lag_factor: Decimal

    # earning pattern
    earning_method: str
    earning_seasonal_weights: Optional[list[float]]

    # onerous detection
    onerous_threshold: Decimal
    onerous_hysteresis_band: Decimal
    onerous_min_policies: int
    onerous_min_months_history: int
    onerous_default_loss_ratio: Decimal
    onerous_per_policy_monthly_admin: Decimal

    # discounting
    discounting_enabled: bool
    discount_rate: Decimal

    # reporting
    portfolios: list[str] = field(default_factory=list)
    afy_start_month: int = 7

    def commission_rate(self, channel: str) -> Decimal:
        """Return the commission rate for a distribution channel (fallback to default)."""
        return self.commission_rates_by_channel.get(channel, self.commission_default_rate)


def _decimal(value) -> Decimal:
    return Decimal(str(value))


@cache
def load_assumptions(path: str) -> IFRS17Assumptions:
    """
    Load IFRS 17 assumptions from a YAML file.

    Cached on the file path — callers that need to reload after edits should
    call ``load_assumptions.cache_clear()``.
    """
    raw = load_yaml(Path(path))

    commission = raw.get("commission", {})
    rates = {
        channel: _decimal(rate)
        for channel, rate in (commission.get("rates_by_channel") or {}).items()
    }

    ra = raw.get("risk_adjustment", {})
    ibnr = raw.get("ibnr", {})
    earning = raw.get("earning_pattern", {})
    onerous = raw.get("onerous_detection", {})
    discount = raw.get("discounting", {})
    reporting = raw.get("reporting", {})

    return IFRS17Assumptions(
        commission_rates_by_channel=rates,
        commission_default_rate=_decimal(commission.get("default_rate", "0.02")),
        commission_amortisation_months=int(commission.get("amortisation_months", 12)),
        ra_uplift_pct=_decimal(ra.get("ra_uplift_pct", "0.06")),
        ra_confidence_level=float(ra.get("confidence_level", 0.75)),
        ibnr_lookback_months=int(ibnr.get("lookback_months", 3)),
        ibnr_lag_factor=_decimal(ibnr.get("lag_factor", "0.15")),
        earning_method=str(earning.get("method", "straight_line")),
        earning_seasonal_weights=earning.get("seasonal_weights"),
        onerous_threshold=_decimal(onerous.get("combined_ratio_threshold", "1.0")),
        onerous_hysteresis_band=_decimal(onerous.get("hysteresis_band", "0.05")),
        onerous_min_policies=int(onerous.get("min_policies_for_assessment", 10)),
        onerous_min_months_history=int(onerous.get("min_months_history", 3)),
        onerous_default_loss_ratio=_decimal(onerous.get("default_loss_ratio", "0.85")),
        onerous_per_policy_monthly_admin=_decimal(
            onerous.get("per_policy_monthly_admin", "5.00")
        ),
        discounting_enabled=bool(discount.get("enabled", False)),
        discount_rate=_decimal(discount.get("rate", "0.035")),
        portfolios=list(
            reporting.get(
                "portfolios",
                ["HOSPITAL_ONLY", "EXTRAS_ONLY", "COMBINED", "AMBULANCE_ONLY"],
            )
        ),
        afy_start_month=int(reporting.get("afy_start_month", 7)),
    )
