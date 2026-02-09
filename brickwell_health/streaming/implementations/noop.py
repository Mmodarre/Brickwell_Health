"""
No-op publisher: discards all events silently.
"""

from brickwell_health.streaming.publisher import PublishEvent


class NoopPublisher:
    """Publisher that discards all events. Used when streaming is disabled."""

    def publish(self, topic: str, event: PublishEvent) -> None:
        pass

    def publish_batch(self, topic: str, events: list[PublishEvent]) -> None:
        pass

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass

    @property
    def stats(self) -> dict[str, int]:
        return {}
