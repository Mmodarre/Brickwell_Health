"""
Core publisher abstractions: PublishEvent dataclass and EventPublisher protocol.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID, uuid4


@dataclass(frozen=True)
class PublishEvent:
    """
    An event to be published to a streaming backend.

    Represents either an INSERT (new record) or UPDATE (changed fields)
    for append-only streaming targets like ZeroBus/Databricks.
    """

    event_id: UUID
    event_type: str  # "insert" or "update"
    table: str
    timestamp: datetime
    worker_id: int
    data: dict[str, Any]  # Full record for insert, changed fields for update
    key: dict[str, Any] = field(default_factory=dict)  # Primary key for updates

    def to_ingest_record(self) -> dict[str, Any]:
        """
        Merge data with event metadata into a flat dict for ZeroBus ingest.

        Returns a single dict with _event_* columns prepended to the data.
        """
        record: dict[str, Any] = {
            "_event_type": self.event_type,
            "_event_id": str(self.event_id),
            "_event_timestamp": self.timestamp.isoformat(),
            "_event_worker_id": self.worker_id,
        }
        # For updates, include the primary key fields so the row can be associated
        if self.key:
            record.update({k: _serialize(v) for k, v in self.key.items()})
        record.update({k: _serialize(v) for k, v in self.data.items()})
        return record

    def to_dict(self) -> dict[str, Any]:
        """
        Plain dict representation for JSON serialization (json_file/log backends).
        """
        return {
            "_event_type": self.event_type,
            "_event_id": str(self.event_id),
            "_event_timestamp": self.timestamp.isoformat(),
            "_worker_id": self.worker_id,
            "_table": self.table,
            "_key": {k: _serialize(v) for k, v in self.key.items()} if self.key else {},
            "data": {k: _serialize(v) for k, v in self.data.items()},
        }


def create_event(
    event_type: str,
    table: str,
    timestamp: datetime,
    worker_id: int,
    data: dict[str, Any],
    key: dict[str, Any] | None = None,
) -> PublishEvent:
    """Factory to create a PublishEvent with auto-generated event_id."""
    return PublishEvent(
        event_id=uuid4(),
        event_type=event_type,
        table=table,
        timestamp=timestamp,
        worker_id=worker_id,
        data=data,
        key=key or {},
    )


def _serialize(value: Any) -> Any:
    """Serialize a value for JSON output."""
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    # Convert Decimal to float (ZeroBus doesn't support Decimal)
    if hasattr(value, "__class__") and value.__class__.__name__ == "Decimal":
        return float(value)
    return value


@runtime_checkable
class EventPublisher(Protocol):
    """Protocol for event publisher backends."""

    def publish(self, topic: str, event: PublishEvent) -> None:
        """Publish a single event."""
        ...

    def publish_batch(self, topic: str, events: list[PublishEvent]) -> None:
        """Publish a batch of events."""
        ...

    def flush(self) -> None:
        """Flush any internal buffers."""
        ...

    def close(self) -> None:
        """Close the publisher and release resources."""
        ...

    @property
    def stats(self) -> dict[str, int]:
        """Return publishing statistics."""
        ...
