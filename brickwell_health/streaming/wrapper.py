"""
StreamingBatchWriter: wrapper that delegates to BatchWriter and publishes events.
"""

import queue
import threading
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable

import structlog

from brickwell_health.db.protocol import BatchWriterProtocol
from brickwell_health.streaming.publisher import (
    EventPublisher,
    PublishEvent,
    create_event,
)
from brickwell_health.streaming.topic_resolver import TopicResolver

logger = structlog.get_logger()

_SENTINEL = object()


class StreamingBatchWriter:
    """
    Wrapper around BatchWriter that publishes events to a streaming backend.

    Delegates all operations to the inner writer, then queues events
    for configured tables. A background thread drains the queue and
    publishes events in batches.
    """

    def __init__(
        self,
        inner: BatchWriterProtocol,
        publisher: EventPublisher,
        topic_resolver: TopicResolver,
        tables: set[str],
        worker_id: int,
        fail_open: bool,
        get_sim_datetime: Callable[[], datetime],
        flush_interval: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        self._inner = inner
        self._publisher = publisher
        self._topic_resolver = topic_resolver
        self._tables = tables
        self._worker_id = worker_id
        self._fail_open = fail_open
        self._get_sim_datetime = get_sim_datetime
        self._flush_interval = flush_interval
        self._batch_size = batch_size

        self._queue: queue.Queue[PublishEvent | object] = queue.Queue()
        self._thread = threading.Thread(
            target=self._background_loop,
            name=f"streaming-worker-{worker_id}",
            daemon=True,
        )
        self._thread.start()
        self._closed = False

        self._stats = {
            "events_queued": 0,
            "events_published": 0,
            "publish_errors": 0,
            "events_dropped_after_close": 0,
        }

    # ---- Pass-through attributes (e.g. engine used by survey process) ----

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the inner writer for attributes not on this wrapper."""
        return getattr(self._inner, name)

    # ---- Table name resolution ----

    def _is_streaming_table(self, table_name: str) -> bool:
        """Check if a table is configured for streaming.

        Handles both qualified ("claims.ambulance_claim") and unqualified
        ("ambulance_claim") names against the configured unqualified set.
        """
        if table_name in self._tables:
            return True
        # Strip schema prefix: "claims.ambulance_claim" → "ambulance_claim"
        return table_name.split(".")[-1] in self._tables

    # ---- BatchWriterProtocol methods ----

    def add(self, table_name: str, record: dict[str, Any]) -> None:
        self._inner.add(table_name, record)
        if self._is_streaming_table(table_name) and not self._closed:
            event = create_event(
                event_type="insert",
                table=table_name,
                timestamp=self._get_sim_datetime(),
                worker_id=self._worker_id,
                data=record,
            )
            self._queue.put(event)
            self._stats["events_queued"] += 1
        elif self._is_streaming_table(table_name) and self._closed:
            self._stats["events_dropped_after_close"] += 1

    def add_many(self, table_name: str, records: list[dict[str, Any]]) -> None:
        self._inner.add_many(table_name, records)
        if self._is_streaming_table(table_name) and not self._closed:
            ts = self._get_sim_datetime()
            for record in records:
                event = create_event(
                    event_type="insert",
                    table=table_name,
                    timestamp=ts,
                    worker_id=self._worker_id,
                    data=record,
                )
                self._queue.put(event)
                self._stats["events_queued"] += 1
        elif self._is_streaming_table(table_name) and self._closed:
            self._stats["events_dropped_after_close"] += len(records)

    def add_raw_sql(self, operation_type: str, sql: str) -> None:
        self._inner.add_raw_sql(operation_type, sql)

    def update_record(
        self,
        table_name: str,
        key_field: str,
        key_value: Any,
        updates: dict[str, Any],
    ) -> bool:
        result = self._inner.update_record(table_name, key_field, key_value, updates)
        if result and self._is_streaming_table(table_name) and not self._closed:
            event = create_event(
                event_type="update",
                table=table_name,
                timestamp=self._get_sim_datetime(),
                worker_id=self._worker_id,
                data=updates,
                key={key_field: key_value},
            )
            self._queue.put(event)
            self._stats["events_queued"] += 1
        elif result and self._is_streaming_table(table_name) and self._closed:
            self._stats["events_dropped_after_close"] += 1
        return result

    def is_in_buffer(self, table_name: str, key_field: str, key_value: Any) -> bool:
        return self._inner.is_in_buffer(table_name, key_field, key_value)

    def flush_for_cdc(self, table_name: str, key_field: str, key_value: Any) -> bool:
        return self._inner.flush_for_cdc(table_name, key_field, key_value)

    def flush_all(self) -> None:
        self._inner.flush_all()

    def get_count(self, table_name: str) -> int:
        return self._inner.get_count(table_name)

    def get_all_counts(self) -> dict[str, int]:
        return self._inner.get_all_counts()

    # ---- Streaming lifecycle ----

    def close(self) -> None:
        """Signal background thread to stop, drain remaining events, and close publisher."""
        # Set closed flag FIRST to prevent new events from being queued
        self._closed = True

        self._queue.put(_SENTINEL)
        self._thread.join(timeout=30)

        if self._thread.is_alive():
            logger.warning(
                "streaming_thread_join_timeout",
                worker_id=self._worker_id,
                queue_size=self._queue.qsize(),
            )

        try:
            self._publisher.close()
        except Exception as e:
            logger.warning("publisher_close_error", error=str(e))

        if self._stats["events_dropped_after_close"] > 0:
            logger.info(
                "streaming_events_dropped_after_close",
                worker_id=self._worker_id,
                count=self._stats["events_dropped_after_close"],
            )

    def get_streaming_stats(self) -> dict[str, int]:
        """Return streaming statistics."""
        combined = dict(self._stats)
        combined.update(self._publisher.stats)
        return combined

    # ---- Background thread ----

    def _background_loop(self) -> None:
        """Background thread: drain queue and publish events in batches."""
        buffer: list[PublishEvent] = []

        while True:
            # Block until first event arrives or timeout
            try:
                item = self._queue.get(timeout=self._flush_interval)
                if item is _SENTINEL:
                    # Drain remaining events before stopping
                    self._drain_remaining(buffer)
                    self._publish_buffer(buffer)
                    return
                buffer.append(item)
            except queue.Empty:
                pass

            # Drain additional events without blocking
            while len(buffer) < self._batch_size:
                try:
                    item = self._queue.get_nowait()
                    if item is _SENTINEL:
                        self._publish_buffer(buffer)
                        return
                    buffer.append(item)
                except queue.Empty:
                    break

            # Publish accumulated buffer
            if buffer:
                self._publish_buffer(buffer)
                buffer = []

    def _drain_remaining(self, buffer: list[PublishEvent]) -> None:
        """Drain any remaining events from the queue into the buffer."""
        while True:
            try:
                item = self._queue.get_nowait()
                if item is _SENTINEL:
                    return
                buffer.append(item)
            except queue.Empty:
                return

    def _publish_buffer(self, buffer: list[PublishEvent]) -> None:
        """Group events by topic and publish."""
        if not buffer:
            return

        # Group by resolved topic
        by_topic: dict[str, list[PublishEvent]] = defaultdict(list)
        for event in buffer:
            topic = self._topic_resolver.resolve(event.table)
            by_topic[topic].append(event)

        for topic, events in by_topic.items():
            try:
                if len(events) == 1:
                    self._publisher.publish(topic, events[0])
                else:
                    self._publisher.publish_batch(topic, events)
                self._stats["events_published"] += len(events)
            except Exception as e:
                self._stats["publish_errors"] += len(events)
                if self._fail_open:
                    logger.warning(
                        "streaming_publish_error",
                        topic=topic,
                        event_count=len(events),
                        error=str(e),
                        worker_id=self._worker_id,
                    )
                else:
                    raise
