"""
Protocol definition for batch writer interface.

Defines the contract that both BatchWriter and StreamingBatchWriter satisfy.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BatchWriterProtocol(Protocol):
    """
    Protocol for batch writer implementations.

    Both BatchWriter (PostgreSQL COPY) and StreamingBatchWriter (wrapper)
    satisfy this protocol.
    """

    def add(self, table_name: str, record: dict[str, Any]) -> None:
        """Add a record to the batch buffer."""
        ...

    def add_many(self, table_name: str, records: list[dict[str, Any]]) -> None:
        """Add multiple records to the batch buffer."""
        ...

    def add_raw_sql(self, operation_type: str, sql: str) -> None:
        """Add a raw SQL statement to be executed during flush."""
        ...

    def update_record(
        self,
        table_name: str,
        key_field: str,
        key_value: Any,
        updates: dict[str, Any],
    ) -> bool:
        """Update a record in buffer or database."""
        ...

    def is_in_buffer(self, table_name: str, key_field: str, key_value: Any) -> bool:
        """Check if a record exists in the buffer."""
        ...

    def flush_for_cdc(self, table_name: str, key_field: str, key_value: Any) -> bool:
        """Flush all buffers if a specific record is still in buffer."""
        ...

    def flush_all(self) -> None:
        """Flush all table buffers."""
        ...

    def get_count(self, table_name: str) -> int:
        """Get total records written for a table."""
        ...

    def get_all_counts(self) -> dict[str, int]:
        """Get counts for all tables."""
        ...
