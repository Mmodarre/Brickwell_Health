"""
Tests for the event streaming subsystem.

Covers: publishers (noop, memory, json_file, log, zerobus),
        wrapper (StreamingBatchWriter), topic resolver, config, and
        claim lifecycle integration.
"""

import json
import time
import threading
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from uuid import UUID, uuid4

import pytest

from brickwell_health.config.models import StreamingConfig, ZeroBusConfig
from brickwell_health.streaming.publisher import PublishEvent, create_event, _serialize
from brickwell_health.streaming.topic_resolver import TopicResolver
from brickwell_health.streaming.implementations.noop import NoopPublisher
from brickwell_health.streaming.implementations.memory import InMemoryPublisher
from brickwell_health.streaming.implementations.json_file import JsonFilePublisher
from brickwell_health.streaming.implementations.log import LogPublisher
from brickwell_health.streaming.wrapper import StreamingBatchWriter
from brickwell_health.streaming.factory import create_publisher


# =============================================================================
# PublishEvent tests
# =============================================================================


class TestPublishEvent:
    def test_create_event_insert(self):
        event = create_event(
            event_type="insert",
            table="claim",
            timestamp=datetime(2025, 1, 15, 10, 0),
            worker_id=0,
            data={"claim_id": uuid4(), "amount": Decimal("1500.00")},
        )
        assert event.event_type == "insert"
        assert event.table == "claim"
        assert event.key == {}
        assert isinstance(event.event_id, UUID)

    def test_create_event_update(self):
        claim_id = uuid4()
        event = create_event(
            event_type="update",
            table="claim",
            timestamp=datetime(2025, 1, 16, 9, 0),
            worker_id=1,
            data={"status": "approved"},
            key={"claim_id": claim_id},
        )
        assert event.event_type == "update"
        assert event.key == {"claim_id": claim_id}

    def test_to_ingest_record_insert(self):
        claim_id = uuid4()
        event = create_event(
            event_type="insert",
            table="claim",
            timestamp=datetime(2025, 1, 15, 10, 0),
            worker_id=0,
            data={"claim_id": claim_id, "amount": Decimal("1500.00")},
        )
        record = event.to_ingest_record()
        assert record["_event_type"] == "insert"
        assert record["_event_id"] == str(event.event_id)
        assert record["_event_timestamp"] == "2025-01-15T10:00:00"
        assert record["_worker_id"] == 0
        assert record["claim_id"] == str(claim_id)
        assert record["amount"] == Decimal("1500.00")

    def test_to_ingest_record_update_includes_key(self):
        claim_id = uuid4()
        event = create_event(
            event_type="update",
            table="claim",
            timestamp=datetime(2025, 1, 16, 9, 0),
            worker_id=0,
            data={"status": "approved"},
            key={"claim_id": claim_id},
        )
        record = event.to_ingest_record()
        assert record["_event_type"] == "update"
        assert record["claim_id"] == str(claim_id)
        assert record["status"] == "approved"

    def test_to_dict(self):
        event = create_event(
            event_type="insert",
            table="claim",
            timestamp=datetime(2025, 1, 15, 10, 0),
            worker_id=0,
            data={"amount": 100},
        )
        d = event.to_dict()
        assert d["_table"] == "claim"
        assert "data" in d
        assert d["data"]["amount"] == 100

    def test_frozen(self):
        event = create_event(
            event_type="insert",
            table="claim",
            timestamp=datetime(2025, 1, 15),
            worker_id=0,
            data={},
        )
        with pytest.raises(AttributeError):
            event.table = "other"  # type: ignore


class TestSerialize:
    def test_uuid(self):
        u = uuid4()
        assert _serialize(u) == str(u)

    def test_datetime(self):
        dt = datetime(2025, 1, 15, 10, 0)
        assert _serialize(dt) == "2025-01-15T10:00:00"

    def test_date(self):
        d = date(2025, 1, 15)
        assert _serialize(d) == "2025-01-15"

    def test_passthrough(self):
        assert _serialize(42) == 42
        assert _serialize("hello") == "hello"


# =============================================================================
# Publisher implementation tests
# =============================================================================


class TestNoopPublisher:
    def test_operations_are_noop(self):
        pub = NoopPublisher()
        event = create_event("insert", "claim", datetime.now(), 0, {"a": 1})
        pub.publish("topic", event)
        pub.publish_batch("topic", [event])
        pub.flush()
        pub.close()
        assert pub.stats == {}


class TestInMemoryPublisher:
    def test_publish_captures_events(self):
        pub = InMemoryPublisher()
        e1 = create_event("insert", "claim", datetime.now(), 0, {"a": 1})
        e2 = create_event("update", "claim", datetime.now(), 0, {"b": 2})
        pub.publish("t1", e1)
        pub.publish("t2", e2)
        assert len(pub.events) == 2
        assert pub.stats["publish_count"] == 2

    def test_publish_batch(self):
        pub = InMemoryPublisher()
        events = [create_event("insert", "claim", datetime.now(), 0, {"i": i}) for i in range(5)]
        pub.publish_batch("topic", events)
        assert len(pub.events) == 5
        assert pub.stats["batch_count"] == 1

    def test_get_events_for_table(self):
        pub = InMemoryPublisher()
        pub.publish("t", create_event("insert", "claim", datetime.now(), 0, {}))
        pub.publish("t", create_event("insert", "invoice", datetime.now(), 0, {}))
        pub.publish("t", create_event("insert", "claim", datetime.now(), 0, {}))
        assert len(pub.get_events_for_table("claim")) == 2
        assert len(pub.get_events_for_table("invoice")) == 1

    def test_get_events_by_type(self):
        pub = InMemoryPublisher()
        pub.publish("t", create_event("insert", "claim", datetime.now(), 0, {}))
        pub.publish("t", create_event("update", "claim", datetime.now(), 0, {}))
        assert len(pub.get_events_by_type("insert")) == 1
        assert len(pub.get_events_by_type("update")) == 1

    def test_clear(self):
        pub = InMemoryPublisher()
        pub.publish("t", create_event("insert", "claim", datetime.now(), 0, {}))
        pub.clear()
        assert len(pub.events) == 0
        assert pub.stats["publish_count"] == 0


class TestJsonFilePublisher:
    def test_writes_ndjson(self, tmp_path):
        pub = JsonFilePublisher(output_dir=str(tmp_path), worker_id=0)
        event = create_event("insert", "claim", datetime(2025, 1, 15), 0, {"amount": 100})
        pub.publish("claim", event)
        pub.flush()
        pub.close()

        files = list(tmp_path.glob("*.ndjson"))
        assert len(files) == 1
        content = files[0].read_text().strip()
        data = json.loads(content)
        assert data["_event_type"] == "insert"
        assert data["data"]["amount"] == 100

    def test_batch_writes_multiple_lines(self, tmp_path):
        pub = JsonFilePublisher(output_dir=str(tmp_path), worker_id=1)
        events = [
            create_event("insert", "claim", datetime(2025, 1, 15), 1, {"i": i}) for i in range(3)
        ]
        pub.publish_batch("claim", events)
        pub.close()

        files = list(tmp_path.glob("*.ndjson"))
        lines = files[0].read_text().strip().split("\n")
        assert len(lines) == 3

    def test_handles_uuid_and_decimal(self, tmp_path):
        pub = JsonFilePublisher(output_dir=str(tmp_path), worker_id=0)
        uid = uuid4()
        event = create_event(
            "insert",
            "claim",
            datetime(2025, 1, 15),
            0,
            {"claim_id": uid, "amount": Decimal("99.95")},
        )
        pub.publish("claim", event)
        pub.close()

        content = list(tmp_path.glob("*.ndjson"))[0].read_text().strip()
        data = json.loads(content)
        assert data["data"]["claim_id"] == str(uid)
        assert data["data"]["amount"] == 99.95

    def test_stats(self, tmp_path):
        pub = JsonFilePublisher(output_dir=str(tmp_path), worker_id=0)
        event = create_event("insert", "claim", datetime.now(), 0, {})
        pub.publish("t", event)
        pub.publish_batch("t", [event, event])
        assert pub.stats["json_file_writes"] == 3


class TestLogPublisher:
    def test_publish_increments_count(self):
        pub = LogPublisher(level="debug")
        event = create_event("insert", "claim", datetime.now(), 0, {})
        pub.publish("topic", event)
        pub.publish_batch("topic", [event, event])
        assert pub.stats["log_events"] == 3

    def test_close_and_flush_are_noop(self):
        pub = LogPublisher()
        pub.flush()
        pub.close()


# =============================================================================
# ZeroBus publisher tests (mocked HTTP)
# =============================================================================


class TestZeroBusPublisher:
    @pytest.fixture
    def zerobus_pub(self):
        """OAuth2 mode publisher (client_id + client_secret, no PAT)."""
        from brickwell_health.streaming.implementations.zerobus import ZeroBusPublisher

        pub = ZeroBusPublisher(
            workspace_id="12345",
            workspace_url="https://test.cloud.databricks.com",
            region="us-east-1",
            catalog="brickwell_health",
            schema_name="ingest_schema_bwh",
            tables=["claim", "claim_line"],
            client_id="app-id",
            client_secret="secret",
        )
        return pub

    @pytest.fixture
    def zerobus_pat_pub(self):
        """PAT mode publisher (token set, no client_id/secret)."""
        from brickwell_health.streaming.implementations.zerobus import ZeroBusPublisher

        pub = ZeroBusPublisher(
            workspace_id="12345",
            workspace_url="https://test.cloud.databricks.com",
            region="us-east-1",
            catalog="brickwell_health",
            schema_name="ingest_schema_bwh",
            tables=["claim", "claim_line"],
            token="dapi-my-pat-token-123",
        )
        return pub

    def test_pat_skips_oauth(self, zerobus_pat_pub):
        """PAT auth should use the token directly, no OIDC call."""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        with patch.object(
            zerobus_pat_pub._session, "post", return_value=mock_response
        ) as mock_post:
            event = create_event("insert", "claim", datetime(2025, 1, 15), 0, {"amount": 100})
            zerobus_pat_pub.publish("brickwell_health.ingest_schema_bwh.claim", event)

        # Only one POST call (the ingest), no OIDC token fetch
        assert mock_post.call_count == 1
        call_args = mock_post.call_args
        assert "/ingest-record?" in call_args[0][0]
        # Bearer header uses the PAT directly
        assert call_args[1]["headers"]["Authorization"] == "Bearer dapi-my-pat-token-123"

    def test_pat_never_expires(self, zerobus_pat_pub):
        """PAT tokens don't expire, _ensure_token always returns the PAT."""
        assert zerobus_pat_pub._use_pat is True
        token = zerobus_pat_pub._ensure_token()
        assert token == "dapi-my-pat-token-123"

    def test_fetch_token(self, zerobus_pub):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "test-token-123",
            "expires_in": 3600,
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(zerobus_pub._session, "post", return_value=mock_response) as mock_post:
            token = zerobus_pub._fetch_token()

        assert token == "test-token-123"
        call_args = mock_post.call_args
        assert "/oidc/v1/token" in call_args[0][0]
        assert call_args[1]["data"]["grant_type"] == "client_credentials"

    def test_publish_single(self, zerobus_pub):
        # Pre-set token to avoid token fetch
        zerobus_pub._access_token = "test-token"
        zerobus_pub._token_expiry = time.time() + 3600

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        with patch.object(zerobus_pub._session, "post", return_value=mock_response) as mock_post:
            event = create_event("insert", "claim", datetime(2025, 1, 15), 0, {"amount": 100})
            zerobus_pub.publish("main.default.claim", event)

        call_args = mock_post.call_args
        assert "/ingest-record?" in call_args[0][0]
        assert "table_name=main.default.claim" in call_args[0][0]
        assert zerobus_pub.stats["zerobus_single_publishes"] == 1

    def test_publish_batch(self, zerobus_pub):
        zerobus_pub._access_token = "test-token"
        zerobus_pub._token_expiry = time.time() + 3600

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        with patch.object(zerobus_pub._session, "post", return_value=mock_response) as mock_post:
            events = [
                create_event("insert", "claim", datetime(2025, 1, 15), 0, {"i": i})
                for i in range(3)
            ]
            zerobus_pub.publish_batch("main.default.claim", events)

        call_args = mock_post.call_args
        assert "/api/1.0/ingest-batch?" in call_args[0][0]
        payload = json.loads(call_args[1]["data"])
        assert len(payload) == 3
        assert zerobus_pub.stats["zerobus_batch_publishes"] == 1

    def test_token_refresh_on_expiry(self, zerobus_pub):
        # Set expired token
        zerobus_pub._access_token = "expired-token"
        zerobus_pub._token_expiry = time.time() - 100

        token_response = MagicMock()
        token_response.json.return_value = {
            "access_token": "new-token",
            "expires_in": 3600,
        }
        token_response.raise_for_status = MagicMock()

        ingest_response = MagicMock()
        ingest_response.raise_for_status = MagicMock()

        with patch.object(
            zerobus_pub._session,
            "post",
            side_effect=[token_response, ingest_response],
        ):
            event = create_event("insert", "claim", datetime(2025, 1, 15), 0, {})
            zerobus_pub.publish("main.default.claim", event)

        assert zerobus_pub._access_token == "new-token"

    def test_publish_error_raises(self, zerobus_pub):
        import requests

        zerobus_pub._access_token = "test-token"
        zerobus_pub._token_expiry = time.time() + 3600

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        mock_response.status_code = 500

        with patch.object(zerobus_pub._session, "post", return_value=mock_response):
            event = create_event("insert", "claim", datetime.now(), 0, {})
            with pytest.raises(requests.HTTPError):
                zerobus_pub.publish("main.default.claim", event)

        assert zerobus_pub.stats["zerobus_errors"] == 1

    def test_close_closes_session(self, zerobus_pub):
        with patch.object(zerobus_pub._session, "close") as mock_close:
            zerobus_pub.close()
        mock_close.assert_called_once()


# =============================================================================
# ZeroBus stream reconnection tests
# =============================================================================


class TestZeroBusStreamReconnection:
    """Tests for stream health checking and automatic reconnection."""

    @pytest.fixture
    def zerobus_pub(self):
        """Create a ZeroBusPublisher with mocked SDK."""
        from brickwell_health.streaming.implementations.zerobus import ZeroBusPublisher

        pub = ZeroBusPublisher(
            workspace_id="12345",
            workspace_url="https://test.cloud.databricks.com",
            region="us-east-1",
            catalog="brickwell_health",
            schema_name="ingest_schema_bwh",
            tables=["claim"],
            client_id="app-id",
            client_secret="secret",
        )
        return pub

    def test_get_stream_reconnects_on_closed_state(self, zerobus_pub):
        from zerobus.sdk.shared.definitions import StreamState

        topic = "brickwell_health.ingest_schema_bwh.claim"

        # Inject a cached stream in CLOSED state
        closed_stream = MagicMock()
        closed_stream.get_state.return_value = StreamState.CLOSED
        zerobus_pub._streams[topic] = closed_stream

        # Mock SDK to return a fresh stream
        fresh_stream = MagicMock()
        zerobus_pub._sdk.create_stream = MagicMock(return_value=fresh_stream)

        result = zerobus_pub._get_stream(topic)

        closed_stream.close.assert_called_once()
        zerobus_pub._sdk.create_stream.assert_called_once()
        assert result is fresh_stream
        assert zerobus_pub._reconnect_count == 1

    def test_get_stream_reconnects_on_failed_state(self, zerobus_pub):
        from zerobus.sdk.shared.definitions import StreamState

        topic = "brickwell_health.ingest_schema_bwh.claim"

        failed_stream = MagicMock()
        failed_stream.get_state.return_value = StreamState.FAILED
        zerobus_pub._streams[topic] = failed_stream

        fresh_stream = MagicMock()
        zerobus_pub._sdk.create_stream = MagicMock(return_value=fresh_stream)

        result = zerobus_pub._get_stream(topic)

        assert result is fresh_stream
        assert zerobus_pub._reconnect_count == 1

    def test_get_stream_returns_cached_when_opened(self, zerobus_pub):
        from zerobus.sdk.shared.definitions import StreamState

        topic = "brickwell_health.ingest_schema_bwh.claim"

        opened_stream = MagicMock()
        opened_stream.get_state.return_value = StreamState.OPENED
        zerobus_pub._streams[topic] = opened_stream

        result = zerobus_pub._get_stream(topic)

        assert result is opened_stream
        assert zerobus_pub._reconnect_count == 0

    def test_get_stream_reconnects_when_get_state_fails(self, zerobus_pub):
        topic = "brickwell_health.ingest_schema_bwh.claim"

        broken_stream = MagicMock()
        broken_stream.get_state.side_effect = RuntimeError("connection lost")
        zerobus_pub._streams[topic] = broken_stream

        fresh_stream = MagicMock()
        zerobus_pub._sdk.create_stream = MagicMock(return_value=fresh_stream)

        result = zerobus_pub._get_stream(topic)

        assert result is fresh_stream
        assert zerobus_pub._reconnect_count == 1

    def test_reconnect_count_in_stats(self, zerobus_pub):
        assert zerobus_pub.stats["zerobus_reconnects"] == 0
        zerobus_pub._reconnect_count = 3
        assert zerobus_pub.stats["zerobus_reconnects"] == 3


# =============================================================================
# TopicResolver tests
# =============================================================================


class TestTopicResolver:
    def test_per_table_default(self):
        config = StreamingConfig(topic_strategy="per_table", topic_prefix="bw.")
        resolver = TopicResolver(config)
        assert resolver.resolve("claim") == "bw.claim"
        assert resolver.resolve("invoice") == "bw.invoice"

    def test_per_table_zerobus_auto_prefix(self):
        config = StreamingConfig(
            backend="zerobus",
            topic_strategy="per_table",
            zerobus=ZeroBusConfig(catalog="prod", schema_name="health"),
        )
        resolver = TopicResolver(config)
        assert resolver.resolve("claim") == "prod.health.claim"

    def test_single_strategy(self):
        config = StreamingConfig(
            topic_strategy="single",
            topic_prefix="all_events",
        )
        resolver = TopicResolver(config)
        assert resolver.resolve("claim") == "all_events"
        assert resolver.resolve("invoice") == "all_events"

    def test_custom_strategy(self):
        config = StreamingConfig(
            topic_strategy="custom",
            topic_mapping={"claim": "prod.claims.raw", "invoice": "prod.billing.raw"},
            topic_prefix="fallback.",
        )
        resolver = TopicResolver(config)
        assert resolver.resolve("claim") == "prod.claims.raw"
        assert resolver.resolve("invoice") == "prod.billing.raw"
        assert resolver.resolve("unknown") == "fallback.unknown"


# =============================================================================
# StreamingBatchWriter wrapper tests
# =============================================================================


class _MockBatchWriter:
    """Minimal mock satisfying BatchWriterProtocol for testing."""

    def __init__(self):
        self.adds: list[tuple[str, dict]] = []
        self.updates: list[tuple[str, str, object, dict]] = []
        self.flushed = 0
        self._counts: dict[str, int] = {}

    def add(self, table_name, record):
        self.adds.append((table_name, record))
        self._counts[table_name] = self._counts.get(table_name, 0) + 1

    def add_many(self, table_name, records):
        for r in records:
            self.add(table_name, r)

    def add_raw_sql(self, operation_type, sql):
        pass

    def update_record(self, table_name, key_field, key_value, updates):
        self.updates.append((table_name, key_field, key_value, updates))
        return True

    def is_in_buffer(self, table_name, key_field, key_value):
        return False

    def flush_for_cdc(self, table_name, key_field, key_value):
        return False

    def flush_all(self):
        self.flushed += 1

    def get_count(self, table_name):
        return self._counts.get(table_name, 0)

    def get_all_counts(self):
        return dict(self._counts)


class TestStreamingBatchWriter:
    @pytest.fixture
    def setup(self):
        inner = _MockBatchWriter()
        publisher = InMemoryPublisher()
        config = StreamingConfig(
            enabled=True,
            tables=["claim", "claim_line"],
            topic_strategy="per_table",
            topic_prefix="test.",
        )
        resolver = TopicResolver(config)
        ts = datetime(2025, 1, 15, 10, 0)

        wrapper = StreamingBatchWriter(
            inner=inner,
            publisher=publisher,
            topic_resolver=resolver,
            tables={"claim", "claim_line"},
            worker_id=0,
            fail_open=True,
            get_sim_datetime=lambda: ts,
            flush_interval=0.05,  # Fast for tests
            batch_size=50,
        )
        return wrapper, inner, publisher

    def test_add_delegates_to_inner(self, setup):
        wrapper, inner, publisher = setup
        wrapper.add("claim", {"claim_id": "abc", "amount": 100})
        assert len(inner.adds) == 1
        assert inner.adds[0] == ("claim", {"claim_id": "abc", "amount": 100})
        wrapper.close()

    def test_add_queues_insert_event_for_configured_table(self, setup):
        wrapper, inner, publisher = setup
        wrapper.add("claim", {"claim_id": "abc"})
        wrapper.close()  # Drains the queue
        # Give background thread time to process
        time.sleep(0.2)
        events = publisher.get_events_for_table("claim")
        assert len(events) == 1
        assert events[0].event_type == "insert"
        assert events[0].data["claim_id"] == "abc"

    def test_add_ignores_non_configured_table(self, setup):
        wrapper, inner, publisher = setup
        wrapper.add("invoice", {"invoice_id": "xyz"})
        wrapper.close()
        time.sleep(0.2)
        assert len(publisher.get_events_for_table("invoice")) == 0
        # But inner writer still received it
        assert len(inner.adds) == 1

    def test_update_record_queues_update_event(self, setup):
        wrapper, inner, publisher = setup
        wrapper.update_record("claim", "claim_id", "abc", {"status": "approved"})
        wrapper.close()
        time.sleep(0.2)
        updates = publisher.get_events_by_type("update")
        assert len(updates) == 1
        assert updates[0].data["status"] == "approved"
        assert updates[0].key == {"claim_id": "abc"}

    def test_update_ignores_non_configured_table(self, setup):
        wrapper, inner, publisher = setup
        wrapper.update_record("invoice", "invoice_id", "xyz", {"paid": True})
        wrapper.close()
        time.sleep(0.2)
        assert len(publisher.get_events_by_type("update")) == 0

    def test_flush_all_delegates(self, setup):
        wrapper, inner, publisher = setup
        wrapper.flush_all()
        assert inner.flushed == 1
        wrapper.close()

    def test_get_count_delegates(self, setup):
        wrapper, inner, publisher = setup
        wrapper.add("claim", {"a": 1})
        assert wrapper.get_count("claim") == 1
        wrapper.close()

    def test_close_drains_queue(self, setup):
        wrapper, inner, publisher = setup
        for i in range(10):
            wrapper.add("claim", {"i": i})
        wrapper.close()
        time.sleep(0.3)
        assert len(publisher.get_events_for_table("claim")) == 10

    def test_fail_open_logs_on_error(self):
        inner = _MockBatchWriter()
        publisher = MagicMock()
        publisher.publish.side_effect = RuntimeError("network error")
        publisher.publish_batch.side_effect = RuntimeError("network error")
        type(publisher).stats = PropertyMock(return_value={})

        config = StreamingConfig(
            enabled=True,
            tables=["claim"],
            topic_strategy="per_table",
            fail_open=True,
        )
        resolver = TopicResolver(config)

        wrapper = StreamingBatchWriter(
            inner=inner,
            publisher=publisher,
            topic_resolver=resolver,
            tables={"claim"},
            worker_id=0,
            fail_open=True,
            get_sim_datetime=lambda: datetime.now(),
            flush_interval=0.05,
            batch_size=50,
        )
        wrapper.add("claim", {"a": 1})
        time.sleep(0.3)
        # Should not have raised
        stats = wrapper.get_streaming_stats()
        assert stats["publish_errors"] >= 1
        wrapper.close()

    def test_add_many_streams_all_records(self, setup):
        wrapper, inner, publisher = setup
        records = [{"claim_id": f"c{i}"} for i in range(3)]
        wrapper.add_many("claim", records)
        wrapper.close()
        time.sleep(0.2)
        assert len(publisher.get_events_for_table("claim")) == 3

    def test_close_sets_closed_flag(self, setup):
        wrapper, inner, publisher = setup
        assert wrapper._closed is False
        wrapper.close()
        assert wrapper._closed is True

    def test_add_after_close_does_not_queue_event(self, setup):
        wrapper, inner, publisher = setup
        wrapper.close()
        time.sleep(0.1)
        wrapper.add("claim", {"claim_id": "after_close"})
        time.sleep(0.2)
        assert len(inner.adds) == 1  # Inner writer still receives the record
        assert len(publisher.get_events_for_table("claim")) == 0

    def test_update_after_close_does_not_queue_event(self, setup):
        wrapper, inner, publisher = setup
        wrapper.close()
        time.sleep(0.1)
        wrapper.update_record("claim", "claim_id", "abc", {"status": "paid"})
        time.sleep(0.2)
        assert len(inner.updates) == 1  # Inner writer still receives the update
        assert len(publisher.get_events_by_type("update")) == 0

    def test_add_many_after_close_does_not_queue_events(self, setup):
        wrapper, inner, publisher = setup
        wrapper.close()
        time.sleep(0.1)
        records = [{"claim_id": f"c{i}"} for i in range(3)]
        wrapper.add_many("claim", records)
        time.sleep(0.2)
        assert len(inner.adds) == 3  # Inner writer still receives all records
        assert len(publisher.get_events_for_table("claim")) == 0

    def test_events_dropped_after_close_stat(self, setup):
        wrapper, inner, publisher = setup
        wrapper.close()
        time.sleep(0.1)
        wrapper.add("claim", {"claim_id": "drop1"})
        wrapper.add("claim", {"claim_id": "drop2"})
        wrapper.update_record("claim", "claim_id", "x", {"status": "y"})
        assert wrapper._stats["events_dropped_after_close"] == 3


# =============================================================================
# Config tests
# =============================================================================


class TestStreamingConfig:
    def test_defaults(self):
        config = StreamingConfig()
        assert config.enabled is False
        assert config.backend == "json_file"
        assert "claim" in config.tables
        assert config.fail_open is True

    def test_enable_zerobus(self):
        config = StreamingConfig(
            enabled=True,
            backend="zerobus",
            zerobus=ZeroBusConfig(
                workspace_id="123",
                workspace_url="https://test.cloud.databricks.com",
                region="us-east-1",
                client_id="app",
                client_secret="secret",
            ),
        )
        assert config.enabled is True
        assert config.backend == "zerobus"
        assert config.zerobus.workspace_id == "123"

    def test_custom_tables(self):
        config = StreamingConfig(
            enabled=True,
            tables=["policy", "member"],
        )
        assert config.tables == ["policy", "member"]


# =============================================================================
# Factory tests
# =============================================================================


class TestFactory:
    def test_creates_noop(self):
        config = StreamingConfig(backend="noop")
        pub = create_publisher(config, worker_id=0)
        assert isinstance(pub, NoopPublisher)

    def test_creates_json_file(self, tmp_path):
        config = StreamingConfig(backend="json_file", json_file_output_dir=str(tmp_path))
        pub = create_publisher(config, worker_id=0)
        assert isinstance(pub, JsonFilePublisher)
        pub.close()

    def test_creates_log(self):
        config = StreamingConfig(backend="log")
        pub = create_publisher(config, worker_id=0)
        assert isinstance(pub, LogPublisher)

    def test_creates_zerobus_with_pat(self):
        config = StreamingConfig(
            backend="zerobus",
            zerobus=ZeroBusConfig(
                workspace_id="123",
                workspace_url="https://test.cloud.databricks.com",
                region="us-east-1",
                token="dapi-test-pat",
            ),
        )
        pub = create_publisher(config, worker_id=0)
        from brickwell_health.streaming.implementations.zerobus import ZeroBusPublisher

        assert isinstance(pub, ZeroBusPublisher)
        assert pub._use_pat is True
        pub.close()

    def test_creates_zerobus_with_oauth(self):
        config = StreamingConfig(
            backend="zerobus",
            zerobus=ZeroBusConfig(
                workspace_id="123",
                workspace_url="https://test.cloud.databricks.com",
                region="us-east-1",
                client_id="app",
                client_secret="secret",
            ),
        )
        pub = create_publisher(config, worker_id=0)
        from brickwell_health.streaming.implementations.zerobus import ZeroBusPublisher

        assert isinstance(pub, ZeroBusPublisher)
        assert pub._use_pat is False
        pub.close()

    def test_unknown_backend_falls_back_to_noop(self):
        config = StreamingConfig(backend="unknown_backend")
        pub = create_publisher(config, worker_id=0)
        assert isinstance(pub, NoopPublisher)


# =============================================================================
# Claim lifecycle integration test
# =============================================================================


class TestClaimLifecycleStreaming:
    """
    Simulate a claim going through SUBMITTED → ASSESSED → APPROVED → PAID
    and verify the streaming event sequence.
    """

    def test_claim_lifecycle_events(self):
        inner = _MockBatchWriter()
        publisher = InMemoryPublisher()
        config = StreamingConfig(
            enabled=True,
            tables=["claim"],
            topic_strategy="per_table",
            topic_prefix="test.",
        )
        resolver = TopicResolver(config)
        sim_time = datetime(2025, 1, 15, 10, 0)

        wrapper = StreamingBatchWriter(
            inner=inner,
            publisher=publisher,
            topic_resolver=resolver,
            tables={"claim"},
            worker_id=0,
            fail_open=True,
            get_sim_datetime=lambda: sim_time,
            flush_interval=0.05,
            batch_size=50,
        )

        claim_id = str(uuid4())

        # Step 1: Claim created (INSERT)
        wrapper.add(
            "claim",
            {
                "claim_id": claim_id,
                "status": "submitted",
                "amount": 1500,
            },
        )

        # Step 2: Claim assessed (UPDATE)
        wrapper.update_record(
            "claim",
            "claim_id",
            claim_id,
            {
                "status": "assessed",
                "assessment_date": "2025-01-16",
            },
        )

        # Step 3: Claim approved (UPDATE)
        wrapper.update_record(
            "claim",
            "claim_id",
            claim_id,
            {
                "status": "approved",
                "approved_amount": 1200,
            },
        )

        # Step 4: Claim paid (UPDATE)
        wrapper.update_record(
            "claim",
            "claim_id",
            claim_id,
            {
                "status": "paid",
                "payment_date": "2025-01-18",
            },
        )

        # Close and drain
        wrapper.close()
        time.sleep(0.3)

        # Verify event sequence
        events = publisher.get_events_for_table("claim")
        assert len(events) == 4

        assert events[0].event_type == "insert"
        assert events[0].data["status"] == "submitted"

        assert events[1].event_type == "update"
        assert events[1].data["status"] == "assessed"
        assert events[1].key["claim_id"] == claim_id

        assert events[2].event_type == "update"
        assert events[2].data["status"] == "approved"

        assert events[3].event_type == "update"
        assert events[3].data["status"] == "paid"

        # Verify all events have metadata
        for event in events:
            assert event.worker_id == 0
            assert event.table == "claim"
            assert isinstance(event.event_id, UUID)

        # Verify topics
        for topic, _ in publisher.events:
            assert topic == "test.claim"
