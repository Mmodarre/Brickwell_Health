"""
Topic resolution strategies for mapping table names to streaming topics.
"""

from brickwell_health.config.models import StreamingConfig


class TopicResolver:
    """
    Resolves table names to streaming topic names.

    Supports three strategies:
    - per_table: "{prefix}{table_name}" or "{catalog}.{schema}.{table_name}" for ZeroBus
    - single: All events go to one topic
    - custom: Explicit table-to-topic mapping
    """

    def __init__(self, config: StreamingConfig) -> None:
        self._strategy = config.topic_strategy
        self._prefix = config.topic_prefix
        self._mapping = config.topic_mapping

        # For ZeroBus, build prefix from catalog.schema
        if config.backend == "zerobus" and not self._prefix:
            zb = config.zerobus
            if zb.catalog and zb.schema_name:
                self._prefix = f"{zb.catalog}.{zb.schema_name}."

    def resolve(self, table_name: str) -> str:
        """
        Resolve a table name to a topic name.

        Args:
            table_name: Database table name (e.g., "claim")

        Returns:
            Topic name for the streaming backend
        """
        if self._strategy == "custom":
            return self._mapping.get(table_name, f"{self._prefix}{table_name}")
        elif self._strategy == "single":
            # All events go to the first value in mapping, or prefix as topic
            if self._mapping:
                return next(iter(self._mapping.values()))
            return self._prefix.rstrip(".") if self._prefix else "events"
        else:
            # per_table (default)
            return f"{self._prefix}{table_name}"
