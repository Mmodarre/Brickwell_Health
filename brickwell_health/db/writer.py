"""
High-performance batch writer using PostgreSQL COPY.

Provides 10-100x throughput improvement over individual INSERTs.
"""

from io import StringIO
from typing import Any
from uuid import UUID
from datetime import date, datetime
from decimal import Decimal

import structlog
from sqlalchemy.engine import Engine

logger = structlog.get_logger()


class BatchWriter:
    """
    High-performance batch writer using PostgreSQL COPY.

    Accumulates records in memory buffers and writes in batches
    for optimal throughput.

    Usage:
        writer = BatchWriter(engine, batch_size=10000)
        writer.add("member", {"member_id": uuid, "first_name": "John", ...})
        writer.add("member", {"member_id": uuid2, ...})
        writer.flush_all()  # Write remaining records
    """

    # Tables in dependency order - parent tables first
    TABLE_FLUSH_ORDER = [
        # Core entities (no dependencies)
        "member",
        # Member children (depend on member)
        "member_update",
        "application",
        # Application children
        "application_member",
        "health_declaration",
        # Policy (depends on application)
        "policy",
        # Policy children
        "policy_member",
        "coverage",
        "bank_account",
        # Second-level children (depend on policy_member, coverage, bank_account)
        "waiting_period",
        "lhc_loading",
        "age_based_discount",
        "phi_rebate_entitlement",
        "suspension",
        "upgrade_request",
        "direct_debit_mandate",
        # Claims (depend on policy, member, coverage)
        "claim",
        "claim_line",
        "hospital_admission",
        "extras_claim",
        "ambulance_claim",
        "prosthesis_claim",
        "medical_service",
        "claim_assessment",
        "benefit_usage",
        # Billing (depend on policy, invoice)
        "invoice",
        "payment",
        "direct_debit_result",
        "arrears",
        "refund",
        "premium_discount",
        # CRM Domain (depend on policy, member, claim, invoice)
        "interaction",
        "service_case",
        "complaint",
        # Communication Domain (depend on policy, member, campaign, interaction)
        "communication_preference",
        "campaign",
        "communication",
        "campaign_response",
        # Digital Behavior Domain (depend on member, policy)
        "web_session",
        "digital_event",
        # Survey Domain (depend on member, policy, interaction)
        "nps_survey_pending",
        "nps_survey",
        "csat_survey_pending",
        "csat_survey",
        # NBA Domain (depend on member, policy, communication, interaction)
        "nba_action_catalog",
        "nba_action_recommendation",
        "nba_action_execution",
    ]

    def __init__(self, engine: Engine, batch_size: int = 10000):
        """
        Initialize BatchWriter.

        Args:
            engine: SQLAlchemy engine
            batch_size: Number of records per batch before auto-flush
        """
        self.engine = engine
        self.batch_size = batch_size
        self._buffers: dict[str, list[dict[str, Any]]] = {}
        self._counts: dict[str, int] = {}
        self._column_order: dict[str, list[str]] = {}
        # Buffer for raw SQL statements (executed after COPY operations)
        self._raw_sql_buffer: list[tuple[str, str]] = []

    def add(self, table_name: str, record: dict[str, Any]) -> None:
        """
        Add a record to the batch buffer.

        Automatically flushes when batch_size is reached.

        Args:
            table_name: Name of the database table
            record: Dictionary of column names to values
        """
        if table_name not in self._buffers:
            self._buffers[table_name] = []
            self._counts[table_name] = 0
            # Store column order from first record
            self._column_order[table_name] = list(record.keys())

        self._buffers[table_name].append(record)

        # Flush all tables in dependency order if any table hits batch size
        if len(self._buffers[table_name]) >= self.batch_size:
            self._flush_all_in_order()

    def add_many(self, table_name: str, records: list[dict[str, Any]]) -> None:
        """
        Add multiple records to the batch buffer.

        Args:
            table_name: Name of the database table
            records: List of record dictionaries
        """
        for record in records:
            self.add(table_name, record)

    def add_raw_sql(self, operation_type: str, sql: str) -> None:
        """
        Add a raw SQL statement to be executed during flush.

        Raw SQL statements are executed AFTER all COPY operations complete,
        ensuring that referenced records exist in the database.

        Args:
            operation_type: Type of operation (for logging, e.g., "policy_update")
            sql: SQL statement to execute
        """
        self._raw_sql_buffer.append((operation_type, sql))

    def update_record(
        self,
        table_name: str,
        key_field: str,
        key_value: Any,
        updates: dict[str, Any],
    ) -> bool:
        """
        Update a record in buffer OR database.

        This method handles the race condition between buffered inserts and updates:
        1. If the record is still in the buffer, update it there (so COPY inserts correct data)
        2. If the record was already flushed to DB, execute an UPDATE statement

        Args:
            table_name: Name of the database table
            key_field: Primary key field name (e.g., "invoice_id")
            key_value: Value of the primary key to match
            updates: Dictionary of field names to new values

        Returns:
            True if record was found and updated, False otherwise
        """
        # Normalize key_value for comparison (handle UUID, etc.)
        key_str = str(key_value)

        # First, check if record is still in the buffer
        if table_name in self._buffers:
            for record in self._buffers[table_name]:
                record_key = record.get(key_field)
                if str(record_key) == key_str:
                    # Found in buffer - update it there
                    record.update(updates)
                    logger.debug(
                        "record_updated_in_buffer",
                        table=table_name,
                        key_field=key_field,
                        key_value=key_str,
                    )
                    return True

        # Not in buffer - try to update in database
        return self._update_in_database(table_name, key_field, key_value, updates)

    def is_in_buffer(self, table_name: str, key_field: str, key_value: Any) -> bool:
        """
        Check if a record exists in the buffer.

        Used by CDC-aware code to determine if a flush is needed before
        updating a record to ensure the INSERT is captured separately.

        Args:
            table_name: Name of the database table
            key_field: Primary key field name
            key_value: Value of the primary key to match

        Returns:
            True if record is in buffer, False otherwise
        """
        key_str = str(key_value)
        if table_name in self._buffers:
            for record in self._buffers[table_name]:
                if str(record.get(key_field)) == key_str:
                    return True
        return False

    def flush_for_cdc(self, table_name: str, key_field: str, key_value: Any) -> bool:
        """
        Flush all buffers if a specific record is still in buffer.

        This ensures INSERT is committed to DB before UPDATE, making
        both events visible to CDC consumers.

        Args:
            table_name: Name of the database table
            key_field: Primary key field name
            key_value: Value of the primary key to match

        Returns:
            True if flush was triggered, False if record was already in DB
        """
        if self.is_in_buffer(table_name, key_field, key_value):
            logger.debug(
                "cdc_flush_triggered",
                table=table_name,
                key_field=key_field,
                key_value=str(key_value),
                reason="ensure_insert_before_update",
            )
            self.flush_all()
            return True
        return False

    def _update_in_database(
        self,
        table_name: str,
        key_field: str,
        key_value: Any,
        updates: dict[str, Any],
    ) -> bool:
        """
        Execute UPDATE statement on the database.

        Args:
            table_name: Name of the database table
            key_field: Primary key field name
            key_value: Value of the primary key
            updates: Dictionary of field names to new values

        Returns:
            True if a row was updated, False otherwise
        """
        if not updates:
            return False

        # Build SET clause
        set_parts = []
        params = []
        for field, value in updates.items():
            set_parts.append(f"{field} = %s")
            params.append(value)

        set_clause = ", ".join(set_parts)
        params.append(str(key_value))  # For WHERE clause

        sql = f"UPDATE {table_name} SET {set_clause} WHERE {key_field} = %s::uuid"

        try:
            with self.engine.connect() as conn:
                raw_conn = conn.connection.dbapi_connection
                with raw_conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    rows_affected = cursor.rowcount
                raw_conn.commit()

            if rows_affected > 0:
                logger.debug(
                    "record_updated_in_db",
                    table=table_name,
                    key_field=key_field,
                    key_value=str(key_value),
                    rows_affected=rows_affected,
                )
                return True
            else:
                logger.debug(
                    "record_not_found_for_update",
                    table=table_name,
                    key_field=key_field,
                    key_value=str(key_value),
                )
                return False

        except Exception as e:
            logger.error(
                "record_update_failed",
                table=table_name,
                key_field=key_field,
                key_value=str(key_value),
                error=str(e),
            )
            raise

    def _flush_table(self, table_name: str) -> None:
        """
        Flush a single table's buffer using COPY.

        Args:
            table_name: Name of the table to flush
        """
        records = self._buffers.get(table_name, [])
        if not records:
            return

        # Get column order
        columns = self._column_order[table_name]
        columns_str = ", ".join(columns)

        # Build tab-separated data in memory
        buffer = StringIO()
        for record in records:
            values = [self._format_value(record.get(col)) for col in columns]
            buffer.write("\t".join(values) + "\n")

        data = buffer.getvalue()

        # Execute COPY using psycopg3 API
        with self.engine.connect() as conn:
            # Get raw psycopg connection for COPY
            raw_conn = conn.connection.dbapi_connection
            
            # psycopg3 uses cursor.copy() with COPY command
            with raw_conn.cursor() as cursor:
                # Use COPY FROM STDIN with text format
                with cursor.copy(
                    f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
                ) as copy:
                    copy.write(data.encode("utf-8"))
            raw_conn.commit()

        # Update counts and clear buffer
        self._counts[table_name] = self._counts.get(table_name, 0) + len(records)
        self._buffers[table_name] = []

        logger.debug(
            "batch_flushed",
            table=table_name,
            records=len(records),
            total=self._counts[table_name],
        )

    def _flush_all_in_order(self) -> None:
        """Flush all tables in dependency order (parent tables first)."""
        # First flush tables in the predefined order
        for table_name in self.TABLE_FLUSH_ORDER:
            if table_name in self._buffers:
                self._flush_table(table_name)

        # Then flush any remaining tables not in the order list
        for table_name in list(self._buffers.keys()):
            self._flush_table(table_name)

        # Execute any pending raw SQL statements after COPY operations
        self._flush_raw_sql()

    def _flush_raw_sql(self) -> None:
        """Execute all pending raw SQL statements."""
        if not self._raw_sql_buffer:
            return

        try:
            with self.engine.connect() as conn:
                raw_conn = conn.connection.dbapi_connection
                with raw_conn.cursor() as cursor:
                    for operation_type, sql in self._raw_sql_buffer:
                        try:
                            cursor.execute(sql)
                            logger.debug(
                                "raw_sql_executed",
                                operation_type=operation_type,
                            )
                        except Exception as e:
                            logger.error(
                                "raw_sql_failed",
                                operation_type=operation_type,
                                error=str(e),
                                sql=sql[:200],  # Truncate for logging
                            )
                            raise
                raw_conn.commit()

            logger.debug(
                "raw_sql_batch_flushed",
                count=len(self._raw_sql_buffer),
            )
        finally:
            # Clear buffer even on error to prevent re-execution
            self._raw_sql_buffer = []

    def flush_all(self) -> None:
        """Flush all table buffers in dependency order."""
        self._flush_all_in_order()

    def get_count(self, table_name: str) -> int:
        """
        Get total records written for a table.

        Args:
            table_name: Name of the table

        Returns:
            Total number of records written
        """
        # Include pending records in buffer
        pending = len(self._buffers.get(table_name, []))
        return self._counts.get(table_name, 0) + pending

    def get_all_counts(self) -> dict[str, int]:
        """
        Get counts for all tables.

        Returns:
            Dictionary of table names to record counts
        """
        all_tables = set(self._counts.keys()) | set(self._buffers.keys())
        return {table: self.get_count(table) for table in all_tables}

    @staticmethod
    def _format_value(value: Any) -> str:
        """
        Format a value for PostgreSQL COPY.

        Handles NULL, boolean, numeric, UUID, date/datetime, and string values.

        Args:
            value: Value to format

        Returns:
            Formatted string for COPY
        """
        if value is None:
            return "\\N"
        if isinstance(value, bool):
            return "t" if value else "f"
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (int, float)):
            return str(value)

        # String value - escape special characters
        s = str(value)
        s = s.replace("\\", "\\\\")
        s = s.replace("\t", "\\t")
        s = s.replace("\n", "\\n")
        s = s.replace("\r", "\\r")
        return s
