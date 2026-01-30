"""
Unit tests for BillingGenerator lifecycle transitions.

Tests verify that payments are created with PENDING status for CDC/SCD support.
"""

from datetime import date
from decimal import Decimal
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.domain.enums import PaymentMethod, PaymentStatus
from brickwell_health.generators.billing_generator import BillingGenerator
from brickwell_health.generators.id_generator import IDGenerator


@pytest.fixture
def billing_generator(
    test_rng: np.random.Generator,
    test_reference,
    id_generator: IDGenerator,
    sim_env: SimulationEnvironment,
):
    """Create a billing generator for testing."""
    return BillingGenerator(
        rng=test_rng,
        reference=test_reference,
        id_generator=id_generator,
        sim_env=sim_env,
    )


@pytest.fixture
def sample_policy():
    """Create a sample policy for testing."""
    from brickwell_health.domain.policy import PolicyCreate

    return PolicyCreate(
        policy_id=uuid4(),
        policy_number="POL-2024-000001",
        product_id=1,
        policy_status="Active",
        policy_type="Single",
        effective_date=date(2024, 1, 1),
        payment_frequency="Monthly",
        premium_amount=Decimal("200.00"),
        excess_amount=Decimal("500.00"),
        distribution_channel="Online",
        state_of_residence="NSW",
        original_join_date=date(2024, 1, 1),
    )


@pytest.fixture
def sample_invoice(sample_policy):
    """Create a sample invoice for testing."""
    from brickwell_health.domain.billing import InvoiceCreate

    return InvoiceCreate(
        invoice_id=uuid4(),
        invoice_number="INV-2024-000001",
        policy_id=sample_policy.policy_id,
        invoice_date=date(2024, 6, 1),
        due_date=date(2024, 6, 15),
        period_start=date(2024, 6, 1),
        period_end=date(2024, 6, 30),
        gross_premium=Decimal("200.00"),
        net_amount=Decimal("180.00"),
        total_amount=Decimal("180.00"),
    )


class TestPaymentLifecycle:
    """Tests for payment lifecycle status."""

    def test_payment_created_as_pending(
        self,
        billing_generator: BillingGenerator,
        sample_policy,
        sample_invoice,
    ):
        """Payments should be created with PENDING status for lifecycle transitions."""
        payment = billing_generator.generate_payment(
            policy=sample_policy,
            invoice=sample_invoice,
            payment_date=date(2024, 6, 15),
            payment_method=PaymentMethod.DIRECT_DEBIT,
        )

        assert payment.payment_status == PaymentStatus.PENDING, (
            f"Expected PENDING, got {payment.payment_status}"
        )
