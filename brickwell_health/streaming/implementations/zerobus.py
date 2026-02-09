"""
ZeroBus Ingest publisher: streams events to Databricks using the official SDK.

Uses OAuth2 service principal authentication (client_id + client_secret).
"""

import time
from datetime import date, datetime
from typing import Any

import structlog
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.shared.definitions import StreamState
from zerobus.sdk.sync import ZerobusSdk

from brickwell_health.streaming.publisher import PublishEvent

logger = structlog.get_logger()

# Epoch for date/timestamp conversions
EPOCH = datetime(1970, 1, 1)

# Stream states that cannot accept ingest_record() calls
_UNHEALTHY_STATES = frozenset(
    {
        StreamState.CLOSED,
        StreamState.FAILED,
        StreamState.UNINITIALIZED,
    }
)


class ZeroBusPublisher:
    """
    Publishes events to Databricks via ZeroBus Ingest SDK.

    The SDK handles:
    - OAuth2 token management (automatic refresh)
    - Connection pooling and retries
    - Batching and flushing

    Auth: OAuth2 client_credentials (service principal) only.
    """

    def __init__(
        self,
        workspace_id: str,
        workspace_url: str,
        region: str,
        catalog: str,
        schema_name: str,
        tables: list[str],
        token: str = "",
        client_id: str = "",
        client_secret: str = "",
    ) -> None:
        self._workspace_id = workspace_id
        self._workspace_url = workspace_url.rstrip("/")
        self._region = region
        self._catalog = catalog
        self._schema_name = schema_name
        self._tables = tables
        self._client_id = client_id
        self._client_secret = client_secret

        # Build ZeroBus server endpoint (no https:// prefix)
        # Azure: workspace_id.zerobus.region.azuredatabricks.net
        # AWS: workspace_id.zerobus.region.cloud.databricks.com
        if "azuredatabricks.net" in workspace_url:
            self._server_endpoint = f"{workspace_id}.zerobus.{region}.azuredatabricks.net"
        else:
            self._server_endpoint = f"{workspace_id}.zerobus.{region}.cloud.databricks.com"

        # Initialize SDK
        self._sdk = ZerobusSdk(
            self._server_endpoint,
            unity_catalog_url=self._workspace_url,
        )

        # Stream cache: one stream per table
        self._streams: dict[str, Any] = {}

        # Stats
        self._record_count = 0
        self._batch_count = 0
        self._error_count = 0
        self._reconnect_count = 0

        logger.info(
            "zerobus_publisher_initialized",
            server_endpoint=self._server_endpoint,
            workspace_url=self._workspace_url,
            catalog=catalog,
            schema=schema_name,
        )

    def _get_stream(self, topic: str) -> Any:
        """
        Get or create a stream for the given topic (table name).

        ZeroBus SDK requires one stream per table.
        If the cached stream is in an unhealthy state (CLOSED, FAILED,
        UNINITIALIZED), it is discarded and a new stream is created.
        """
        existing = self._streams.get(topic)
        if existing is not None:
            try:
                state = existing.get_state()
                if state not in _UNHEALTHY_STATES:
                    return existing
                # Stream is unhealthy -- close defensively and recreate
                logger.warning(
                    "zerobus_stream_unhealthy",
                    topic=topic,
                    state=state.name if hasattr(state, "name") else str(state),
                )
            except Exception as e:
                logger.warning(
                    "zerobus_stream_state_check_failed",
                    topic=topic,
                    error=str(e),
                )
            # Clean up dead stream
            try:
                existing.close()
            except Exception:
                pass
            del self._streams[topic]
            self._reconnect_count += 1

        # Create new stream
        import random

        time.sleep(random.uniform(0.1, 0.5))

        table_properties = TableProperties(topic)
        options = StreamConfigurationOptions(record_type=RecordType.JSON)

        stream = self._sdk.create_stream(
            self._client_id,
            self._client_secret,
            table_properties,
            options,
        )
        self._streams[topic] = stream
        logger.debug("zerobus_stream_created", topic=topic)

        return stream

    def _convert_for_zerobus(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Convert record values to ZeroBus-compatible types.

        ZeroBus requirements:
        - DATE: int32 (days since epoch 1970-01-01)
        - TIMESTAMP: int64 (microseconds since epoch)
        - DECIMAL: float
        """
        converted = {}
        for key, value in record.items():
            if value is None:
                converted[key] = None
            elif isinstance(value, date) and not isinstance(value, datetime):
                # DATE -> int32 (days since epoch)
                days = (value - date(1970, 1, 1)).days
                converted[key] = days
            elif isinstance(value, datetime):
                # TIMESTAMP -> int64 (microseconds since epoch)
                microseconds = int(value.timestamp() * 1_000_000)
                converted[key] = microseconds
            elif isinstance(value, str):
                # Check if it's an ISO date string and convert
                if len(value) == 10 and value[4] == "-" and value[7] == "-":
                    # Looks like "YYYY-MM-DD" - convert to days since epoch
                    try:
                        dt = datetime.strptime(value, "%Y-%m-%d").date()
                        days = (dt - date(1970, 1, 1)).days
                        converted[key] = days
                    except ValueError:
                        converted[key] = value
                elif "T" in value and len(value) >= 19:
                    # Looks like ISO timestamp - convert to microseconds
                    try:
                        # Handle with/without timezone
                        if "+" in value or value.endswith("Z"):
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                        else:
                            dt = datetime.fromisoformat(value)
                        microseconds = int(dt.timestamp() * 1_000_000)
                        converted[key] = microseconds
                    except ValueError:
                        converted[key] = value
                else:
                    converted[key] = value
            elif hasattr(value, "__class__") and value.__class__.__name__ == "Decimal":
                # DECIMAL -> float
                converted[key] = float(value)
            else:
                converted[key] = value

        return converted

    def publish(self, topic: str, event: PublishEvent) -> None:
        """Publish a single record via SDK."""
        try:
            stream = self._get_stream(topic)
            record = event.to_ingest_record()
            record = self._convert_for_zerobus(record)
            stream.ingest_record(record)
            self._record_count += 1
        except Exception as e:
            self._error_count += 1
            logger.warning(
                "zerobus_publish_error",
                topic=topic,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def publish_batch(self, topic: str, events: list[PublishEvent]) -> None:
        """
        Publish multiple records via SDK.

        The SDK handles batching internally, so we just ingest each record.
        """
        try:
            stream = self._get_stream(topic)
            for event in events:
                record = event.to_ingest_record()
                record = self._convert_for_zerobus(record)
                stream.ingest_record(record)
            self._record_count += len(events)
            self._batch_count += 1
        except Exception as e:
            self._error_count += 1
            logger.warning(
                "zerobus_batch_error",
                topic=topic,
                event_count=len(events),
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def flush(self) -> None:
        """Flush all active streams."""
        for topic, stream in self._streams.items():
            try:
                stream.flush()
                logger.debug("zerobus_stream_flushed", topic=topic)
            except Exception as e:
                logger.warning(
                    "zerobus_flush_error",
                    topic=topic,
                    error=str(e),
                )

    def close(self) -> None:
        """Close all active streams."""
        for topic, stream in self._streams.items():
            try:
                stream.close()
                logger.debug("zerobus_stream_closed", topic=topic)
            except Exception as e:
                logger.warning(
                    "zerobus_close_error",
                    topic=topic,
                    error=str(e),
                )
        self._streams.clear()

    @property
    def stats(self) -> dict[str, int]:
        return {
            "zerobus_records_published": self._record_count,
            "zerobus_batches_published": self._batch_count,
            "zerobus_errors": self._error_count,
            "zerobus_reconnects": self._reconnect_count,
        }
