"""
Log publisher: emits events through structlog.
"""

import structlog

from brickwell_health.streaming.publisher import PublishEvent

logger = structlog.get_logger()


class LogPublisher:
    """Publishes events by logging them via structlog."""

    def __init__(self, level: str = "debug") -> None:
        self._level = level.lower()
        self._count = 0

    def _log(self, **kwargs: object) -> None:
        log_fn = getattr(logger, self._level, logger.debug)
        log_fn(**kwargs)

    def publish(self, topic: str, event: PublishEvent) -> None:
        self._log(
            event="streaming_event",
            topic=topic,
            event_type=event.event_type,
            table=event.table,
            event_id=str(event.event_id),
        )
        self._count += 1

    def publish_batch(self, topic: str, events: list[PublishEvent]) -> None:
        self._log(
            event="streaming_batch",
            topic=topic,
            event_count=len(events),
            tables=list({e.table for e in events}),
        )
        self._count += len(events)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass

    @property
    def stats(self) -> dict[str, int]:
        return {"log_events": self._count}
