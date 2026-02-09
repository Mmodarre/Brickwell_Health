"""
In-memory publisher for unit testing.
"""

from brickwell_health.streaming.publisher import PublishEvent


class InMemoryPublisher:
    """
    Captures all events in memory for testing and inspection.

    NOT thread-safe by design - intended for single-threaded test use.
    """

    def __init__(self) -> None:
        self._events: list[tuple[str, PublishEvent]] = []
        self._publish_count = 0
        self._batch_count = 0

    def publish(self, topic: str, event: PublishEvent) -> None:
        self._events.append((topic, event))
        self._publish_count += 1

    def publish_batch(self, topic: str, events: list[PublishEvent]) -> None:
        for event in events:
            self._events.append((topic, event))
        self._batch_count += 1
        self._publish_count += len(events)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass

    @property
    def stats(self) -> dict[str, int]:
        return {
            "publish_count": self._publish_count,
            "batch_count": self._batch_count,
            "total_events": len(self._events),
        }

    # ---- Test helpers ----

    @property
    def events(self) -> list[tuple[str, PublishEvent]]:
        """All captured (topic, event) pairs."""
        return list(self._events)

    def get_events_for_table(self, table: str) -> list[PublishEvent]:
        """Get all events for a specific table."""
        return [e for _, e in self._events if e.table == table]

    def get_events_by_type(self, event_type: str) -> list[PublishEvent]:
        """Get all events of a specific type (insert/update)."""
        return [e for _, e in self._events if e.event_type == event_type]

    def clear(self) -> None:
        """Clear all captured events."""
        self._events.clear()
        self._publish_count = 0
        self._batch_count = 0
