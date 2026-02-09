"""
NDJSON file publisher: writes events as newline-delimited JSON files per topic.
"""

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import structlog

from brickwell_health.streaming.publisher import PublishEvent

logger = structlog.get_logger()


class _JsonEncoder(json.JSONEncoder):
    """Custom JSON encoder for simulator types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)


class JsonFilePublisher:
    """
    Writes events as NDJSON (one JSON object per line) to files organized by topic.

    File naming: {output_dir}/{topic}_worker{worker_id}.ndjson
    """

    def __init__(self, output_dir: str, worker_id: int) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._worker_id = worker_id
        self._handles: dict[str, Any] = {}
        self._write_count = 0

    def _get_handle(self, topic: str) -> Any:
        if topic not in self._handles:
            # Sanitize topic for filename (replace dots with underscores)
            safe_topic = topic.replace(".", "_").replace("/", "_")
            path = self._output_dir / f"{safe_topic}_worker{self._worker_id}.ndjson"
            self._handles[topic] = open(path, "a", encoding="utf-8")  # noqa: SIM115
        return self._handles[topic]

    def publish(self, topic: str, event: PublishEvent) -> None:
        handle = self._get_handle(topic)
        line = json.dumps(event.to_dict(), cls=_JsonEncoder, separators=(",", ":"))
        handle.write(line + "\n")
        self._write_count += 1

    def publish_batch(self, topic: str, events: list[PublishEvent]) -> None:
        handle = self._get_handle(topic)
        for event in events:
            line = json.dumps(event.to_dict(), cls=_JsonEncoder, separators=(",", ":"))
            handle.write(line + "\n")
        self._write_count += len(events)

    def flush(self) -> None:
        for handle in self._handles.values():
            handle.flush()

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()

    @property
    def stats(self) -> dict[str, int]:
        return {
            "json_file_writes": self._write_count,
            "open_files": len(self._handles),
        }
