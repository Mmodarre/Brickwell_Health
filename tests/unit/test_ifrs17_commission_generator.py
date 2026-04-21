"""Unit tests for the commission (acquisition cost) generator."""

from datetime import date
from decimal import Decimal
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.domain.enums import DistributionChannel, PolicyType
from brickwell_health.domain.policy import PolicyCreate
from brickwell_health.generators.commission_generator import (
    CommissionGenerator,
    _annualise_premium,
    _add_months,
)
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.ifrs17.assumptions import IFRS17Assumptions


@pytest.fixture
def assumptions() -> IFRS17Assumptions:
    return IFRS17Assumptions(
        commission_rates_by_channel={
            "Online": Decimal("0.015"),
            "Broker": Decimal("0.12"),
            "Comparison": Decimal("0.18"),
        },
        commission_default_rate=Decimal("0.02"),
        commission_amortisation_months=12,
        ra_uplift_pct=Decimal("0.06"),
        ra_confidence_level=0.75,
        ibnr_lookback_months=3,
        ibnr_lag_factor=Decimal("0.15"),
        earning_method="straight_line",
        earning_seasonal_weights=None,
        onerous_threshold=Decimal("1.00"),
        onerous_hysteresis_band=Decimal("0.05"),
        onerous_min_policies=10,
        onerous_min_months_history=3,
        onerous_default_loss_ratio=Decimal("0.85"),
        onerous_per_policy_monthly_admin=Decimal("5.00"),
        discounting_enabled=False,
        discount_rate=Decimal("0.035"),
        portfolios=["HOSPITAL_ONLY", "EXTRAS_ONLY", "COMBINED", "AMBULANCE_ONLY"],
        afy_start_month=7,
    )


@pytest.fixture
def id_gen() -> IDGenerator:
    return IDGenerator(np.random.default_rng(42), prefix_year=2025, worker_id=0)


def _policy(**overrides) -> PolicyCreate:
    base = dict(
        policy_id=uuid4(),
        policy_number="POL-1",
        product_id=1,
        policy_type=PolicyType.SINGLE,
        effective_date=date(2025, 7, 1),
        premium_amount=Decimal("200"),
        payment_frequency="Monthly",
        distribution_channel=DistributionChannel.ONLINE,
        state_of_residence="NSW",
        original_join_date=date(2025, 7, 1),
    )
    base.update(overrides)
    return PolicyCreate(**base)


class TestAnnualisePremium:
    def test_monthly(self):
        assert _annualise_premium(Decimal("200"), "Monthly") == Decimal("2400.00")

    def test_quarterly(self):
        assert _annualise_premium(Decimal("600"), "Quarterly") == Decimal("2400.00")

    def test_annually(self):
        assert _annualise_premium(Decimal("2400"), "Annually") == Decimal("2400.00")

    def test_unknown_falls_back_to_monthly(self):
        # A warning is logged but no exception raised
        assert _annualise_premium(Decimal("200"), "gibberish") == Decimal("2400.00")


class TestAddMonths:
    def test_basic(self):
        assert _add_months(date(2025, 7, 1), 12) == date(2026, 7, 1)

    def test_clamp_end_of_month(self):
        assert _add_months(date(2025, 1, 31), 1) == date(2025, 2, 28)


class TestCommissionGenerator:
    def test_online_commission(self, id_gen, assumptions):
        gen = CommissionGenerator(id_gen)
        rows = gen.generate_for_policy(_policy(), assumptions)
        assert len(rows) == 1
        row = rows[0]
        assert row.distribution_channel == "Online"
        assert row.gross_written_premium == Decimal("2400.00")
        assert row.commission_rate == Decimal("0.015")
        assert row.commission_amount == Decimal("36.00")
        assert row.amortisation_start_date == date(2025, 7, 1)
        assert row.amortisation_end_date == date(2026, 7, 1)
        assert row.commission_type == "Upfront"

    def test_broker_commission(self, id_gen, assumptions):
        gen = CommissionGenerator(id_gen)
        rows = gen.generate_for_policy(
            _policy(distribution_channel=DistributionChannel.BROKER),
            assumptions,
        )
        # 2400 * 0.12 = 288
        assert rows[0].commission_rate == Decimal("0.12")
        assert rows[0].commission_amount == Decimal("288.00")

    def test_default_rate_for_unknown_channel(self, id_gen, assumptions):
        """Channels not in the YAML use the default rate."""
        gen = CommissionGenerator(id_gen)
        rows = gen.generate_for_policy(
            _policy(distribution_channel=DistributionChannel.CORPORATE),
            assumptions,
        )
        # CORPORATE not in assumptions rates — default 0.02
        assert rows[0].commission_rate == Decimal("0.02")
