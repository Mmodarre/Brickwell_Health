"""
Unit tests for BillingProcess lifecycle transitions.

Tests verify that payment status is updated correctly after direct debit
and that invoice updates include audit fields.
"""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.domain.enums import PaymentStatus, InvoiceStatus


class MockBatchWriter:
    """Mock batch writer for testing."""

    def __init__(self):
        self.records = {}
        self.updates = []
        self.flush_count = 0
        self.cdc_flush_log = []  # Track CDC flushes for verification

    def add(self, table_name: str, record: dict) -> None:
        if table_name not in self.records:
            self.records[table_name] = []
        self.records[table_name].append(record)

    def update_record(self, table_name: str, key_field: str, key_value, updates: dict) -> bool:
        self.updates.append({
            "table_name": table_name,
            "key_field": key_field,
            "key_value": key_value,
            "updates": updates,
        })
        return True

    def is_in_buffer(self, table_name: str, key_field: str, key_value) -> bool:
        """Check if a record exists in the buffer."""
        if table_name not in self.records:
            return False
        key_str = str(key_value)
        return any(str(r.get(key_field)) == key_str for r in self.records[table_name])

    def flush_all(self) -> None:
        """Flush all buffers."""
        self.flush_count += 1
        # Clear buffers to simulate flush
        self.records = {}

    def flush_for_cdc(self, table_name: str, key_field: str, key_value) -> bool:
        """Flush if record is in buffer (for CDC visibility)."""
        if self.is_in_buffer(table_name, key_field, key_value):
            self.cdc_flush_log.append({
                "table": table_name,
                "key_field": key_field,
                "key_value": str(key_value),
            })
            self.flush_all()
            return True
        return False


@pytest.fixture
def mock_batch_writer():
    """Create a mock batch writer."""
    return MockBatchWriter()


@pytest.fixture
def mock_billing_process(test_rng, test_config, sim_env, mock_batch_writer):
    """Create a mock billing process for testing payment update logic."""
    from brickwell_health.core.processes.billing import BillingProcess

    # Create a minimal process for testing
    with patch.object(BillingProcess, '__init__', lambda self, *args, **kwargs: None):
        process = BillingProcess()
        process.rng = test_rng
        process.config = test_config
        process.sim_env = sim_env
        process.batch_writer = mock_batch_writer

        return process


class TestPaymentStatusUpdates:
    """Tests for payment status updates after direct debit."""

    def test_payment_updated_to_completed_on_success(
        self, mock_billing_process, mock_batch_writer
    ):
        """Payment status should be updated to COMPLETED after successful direct debit."""
        payment_id = uuid4()

        # Create a mock payment
        mock_payment = MagicMock()
        mock_payment.payment_id = payment_id

        mock_billing_process._update_payment_status(mock_payment, PaymentStatus.COMPLETED)

        # Should have called update_record
        assert len(mock_batch_writer.updates) == 1
        update = mock_batch_writer.updates[0]

        assert update["table_name"] == "payment"
        assert update["key_field"] == "payment_id"
        assert update["key_value"] == payment_id
        assert update["updates"]["payment_status"] == PaymentStatus.COMPLETED.value
        assert "modified_at" in update["updates"]
        assert update["updates"]["modified_by"] == "SIMULATION"

    def test_payment_updated_to_failed_on_failure(
        self, mock_billing_process, mock_batch_writer
    ):
        """Payment status should be updated to FAILED when all retries exhausted."""
        payment_id = uuid4()

        mock_payment = MagicMock()
        mock_payment.payment_id = payment_id

        mock_billing_process._update_payment_status(mock_payment, PaymentStatus.FAILED)

        assert len(mock_batch_writer.updates) == 1
        update = mock_batch_writer.updates[0]

        assert update["updates"]["payment_status"] == PaymentStatus.FAILED.value


class TestInvoiceAuditFields:
    """Tests for invoice update audit fields."""

    def test_invoice_update_includes_modified_at(
        self, mock_billing_process, mock_batch_writer
    ):
        """Invoice updates should include modified_at and modified_by fields."""
        invoice_id = uuid4()

        # Create a mock invoice
        mock_invoice = MagicMock()
        mock_invoice.invoice_id = invoice_id
        mock_invoice.invoice_status = InvoiceStatus.PAID
        mock_invoice.paid_amount = Decimal("180.00")
        mock_invoice.balance_due = Decimal("0.00")

        mock_billing_process._update_invoice_status(mock_invoice)

        assert len(mock_batch_writer.updates) == 1
        update = mock_batch_writer.updates[0]

        assert update["table_name"] == "invoice"
        assert update["key_field"] == "invoice_id"
        assert update["key_value"] == invoice_id
        assert "modified_at" in update["updates"]
        assert update["updates"]["modified_by"] == "SIMULATION"
        assert update["updates"]["invoice_status"] == InvoiceStatus.PAID.value


class TestCDCFlushBehavior:
    """Tests for CDC flush behavior during billing transitions."""

    def test_flush_for_cdc_called_on_invoice_update(
        self, mock_billing_process, mock_batch_writer
    ):
        """flush_for_cdc should be called before invoice status update."""
        invoice_id = uuid4()

        # Add invoice to buffer to simulate it being there
        mock_batch_writer.records["invoice"] = [{"invoice_id": str(invoice_id)}]

        mock_invoice = MagicMock()
        mock_invoice.invoice_id = invoice_id
        mock_invoice.invoice_status = InvoiceStatus.PAID
        mock_invoice.paid_amount = Decimal("180.00")
        mock_invoice.balance_due = Decimal("0.00")

        mock_billing_process._update_invoice_status(mock_invoice)

        # Should have recorded a CDC flush for the invoice
        assert len(mock_batch_writer.cdc_flush_log) == 1
        invoice_flush = mock_batch_writer.cdc_flush_log[0]
        assert invoice_flush["table"] == "invoice"
        assert invoice_flush["key_field"] == "invoice_id"
        assert invoice_flush["key_value"] == str(invoice_id)

    def test_no_flush_when_invoice_not_in_buffer(
        self, mock_billing_process, mock_batch_writer
    ):
        """flush_for_cdc should not trigger flush if invoice already in DB."""
        invoice_id = uuid4()

        # Buffer is empty - invoice already flushed to DB
        mock_batch_writer.records = {}

        mock_invoice = MagicMock()
        mock_invoice.invoice_id = invoice_id
        mock_invoice.invoice_status = InvoiceStatus.PAID
        mock_invoice.paid_amount = Decimal("180.00")
        mock_invoice.balance_due = Decimal("0.00")

        mock_billing_process._update_invoice_status(mock_invoice)

        # No CDC flush should have been triggered
        assert len(mock_batch_writer.cdc_flush_log) == 0
        assert mock_batch_writer.flush_count == 0

        # But update_record should still have been called
        assert len(mock_batch_writer.updates) == 1
