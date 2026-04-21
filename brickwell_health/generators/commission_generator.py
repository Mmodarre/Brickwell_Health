"""
Commission / acquisition cost generator for IFRS 17 DAC tracking.

Emits one ``billing.acquisition_cost`` row per policy at inception. The
commission rate is looked up from the assumptions YAML by distribution channel;
the amount is the annualised premium times the rate.

Deterministic: uses no RNG, so two runs with the same policy / assumptions
produce identical outputs (UUIDs come from the worker-scoped ID generator).

Trail commissions and clawbacks on early cancellation are out of scope for
Phase 1 — see the TODO below for Phase 2 integration points.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

import structlog

from brickwell_health.domain.commission import AcquisitionCostCreate

if TYPE_CHECKING:
    from brickwell_health.domain.policy import PolicyCreate
    from brickwell_health.generators.id_generator import IDGenerator
    from brickwell_health.ifrs17.assumptions import IFRS17Assumptions


logger = structlog.get_logger()


_FREQUENCY_TO_ANNUAL_MULTIPLIER: dict[str, int] = {
    "Monthly": 12,
    "Fortnightly": 26,
    "Weekly": 52,
    "Quarterly": 4,
    "Half-Yearly": 2,
    "HalfYearly": 2,
    "Annually": 1,
    "Annual": 1,
    "Yearly": 1,
}


def _annualise_premium(premium: Decimal, frequency: str) -> Decimal:
    """
    Convert a period premium to an annual figure using the known frequency
    mapping. Unknown frequencies fall back to Monthly with a warning.
    """
    multiplier = _FREQUENCY_TO_ANNUAL_MULTIPLIER.get(frequency)
    if multiplier is None:
        logger.warning(
            "commission_unknown_payment_frequency",
            frequency=frequency,
            fallback="Monthly",
        )
        multiplier = 12
    return (premium * Decimal(multiplier)).quantize(Decimal("0.01"))


def _add_months(d: date, months: int) -> date:
    """Add calendar months to a date, clamping the day to end-of-month if needed."""
    total = d.month - 1 + months
    new_year = d.year + total // 12
    new_month = total % 12 + 1
    # clamp day
    import calendar
    last_day = calendar.monthrange(new_year, new_month)[1]
    new_day = min(d.day, last_day)
    return date(new_year, new_month, new_day)


class CommissionGenerator:
    """Generates acquisition cost rows for a policy at inception.

    TODO (Phase 2): clawback on early cancellation. ``policy_lifecycle`` has no
    hook for this today; when it does, append a ``commission_type='Clawback'``
    row with a negative amount and set ``clawback_date`` on the original row.
    """

    def __init__(self, id_generator: "IDGenerator"):
        self.id_generator = id_generator

    def generate_for_policy(
        self,
        policy: "PolicyCreate",
        assumptions: "IFRS17Assumptions",
    ) -> list[AcquisitionCostCreate]:
        """
        Produce the commission row(s) for a single policy.

        Returns a list (rather than a scalar) so Phase 2 extensions can add
        trail rows without changing the call site.
        """
        # DistributionChannel may be an enum on the Policy model
        channel = policy.distribution_channel
        channel_str = channel.value if hasattr(channel, "value") else str(channel)

        annual_gwp = _annualise_premium(policy.premium_amount, policy.payment_frequency)
        rate = assumptions.commission_rate(channel_str)
        amount = (annual_gwp * rate).quantize(Decimal("0.01"))

        amort_months = assumptions.commission_amortisation_months
        amort_start = policy.effective_date
        amort_end = _add_months(amort_start, amort_months)

        row = AcquisitionCostCreate(
            acquisition_cost_id=self.id_generator.generate_uuid(),
            policy_id=policy.policy_id,
            commission_type="Upfront",
            distribution_channel=channel_str,
            gross_written_premium=annual_gwp,
            commission_rate=rate,
            commission_amount=amount,
            incurred_date=policy.effective_date,
            amortisation_start_date=amort_start,
            amortisation_end_date=amort_end,
            status="Active",
        )

        return [row]
