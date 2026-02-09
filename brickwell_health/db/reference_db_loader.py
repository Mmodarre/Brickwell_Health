"""
Reference data loader for bulk loading JSON files into database tables.

Uses PostgreSQL COPY for high-performance bulk inserts.
"""

import json
from io import StringIO
from pathlib import Path
from typing import Any
from decimal import Decimal
from datetime import date, datetime
from uuid import UUID

import structlog
from sqlalchemy.engine import Engine

logger = structlog.get_logger()


# Table loading order - respects FK dependencies
TABLE_LOAD_ORDER = [
    "product_tier",              # No dependencies - must load before product
    "state_territory",           # No dependencies
    "product",                   # FK to product_tier (optional)
    "excess_option",             # FK to product - must load after product
    "provider",                  # No dependencies
    "hospital",                  # No dependencies
    "clinical_category",         # No dependencies
    "benefit_category",          # Self-referential FK (parent_category_id) - load parents first
    "claim_rejection_reason",    # No dependencies
    "extras_item_code",          # No dependencies
    "prosthesis_list_item",      # No dependencies
    "mbs_item",                  # No dependencies
    "interaction_type",          # No dependencies
    "interaction_outcome",       # No dependencies
    "case_type",                 # No dependencies
    "complaint_category",        # Self-referential FK (parent_category_id) - load parents first
    "communication_template",    # No dependencies
    "campaign_type",             # No dependencies
    "survey_type",               # No dependencies
    "provider_location",         # FK to provider - must load after provider
]


# Mapping from table name to JSON filename
TABLE_JSON_MAPPING = {
    "product_tier": "product_tier.json",
    "state_territory": "state_territory.json",
    "product": "product.json",
    "excess_option": "excess_option.json",
    "provider": "provider.json",
    "hospital": "hospital.json",
    "clinical_category": "clinical_category.json",
    "benefit_category": "benefit_category.json",
    "claim_rejection_reason": "claim_rejection_reason.json",
    "extras_item_code": "extras_item_code.json",
    "prosthesis_list_item": "prosthesis_list_item.json",
    "mbs_item": "mbs_item.json",
    "interaction_type": "interaction_type.json",
    "interaction_outcome": "interaction_outcome.json",
    "case_type": "case_type.json",
    "complaint_category": "complaint_category.json",
    "communication_template": "communication_template.json",
    "campaign_type": "campaign_type.json",
    "survey_type": "survey_type.json",
    "provider_location": "provider_location.json",
}


class ReferenceDataDBLoader:
    """
    Loads reference data from JSON files into PostgreSQL tables.

    Uses PostgreSQL COPY for high-performance bulk inserts.

    Usage:
        loader = ReferenceDataDBLoader(engine, reference_path)
        loader.load_all()
    """

    def __init__(self, engine: Engine, reference_path: Path | str):
        """
        Initialize the reference data loader.

        Args:
            engine: SQLAlchemy engine for database connection
            reference_path: Path to reference data directory containing JSON files
        """
        self.engine = engine
        self.reference_path = Path(reference_path)
        self.stats: dict[str, int] = {}

    def load_all(self) -> dict[str, int]:
        """
        Load all reference data tables from JSON files.

        Returns:
            Dictionary of table names to record counts loaded
        """
        logger.info("loading_reference_data_start", path=str(self.reference_path))

        for table_name in TABLE_LOAD_ORDER:
            json_filename = TABLE_JSON_MAPPING[table_name]
            json_path = self.reference_path / json_filename

            if not json_path.exists():
                logger.warning(
                    "reference_json_not_found",
                    table=table_name,
                    file=json_filename,
                    path=str(json_path)
                )
                self.stats[table_name] = 0
                continue

            try:
                records = self._load_json(json_path)

                if not records:
                    logger.info("no_records_to_load", table=table_name)
                    self.stats[table_name] = 0
                    continue

                # Transform records based on table-specific logic
                transformed = self._transform_records(table_name, records)

                # Load into database
                count = self._load_table(table_name, transformed)
                self.stats[table_name] = count

                logger.info(
                    "reference_table_loaded",
                    table=table_name,
                    records=count
                )

            except Exception as e:
                logger.error(
                    "reference_table_load_failed",
                    table=table_name,
                    file=json_filename,
                    error=str(e)
                )
                raise

        total_records = sum(self.stats.values())
        logger.info(
            "loading_reference_data_complete",
            tables=len(self.stats),
            total_records=total_records,
            stats=self.stats
        )

        return self.stats

    def _load_json(self, json_path: Path) -> list[dict[str, Any]]:
        """
        Load JSON file and return list of records.

        Args:
            json_path: Path to JSON file

        Returns:
            List of record dictionaries
        """
        with open(json_path) as f:
            data = json.load(f)

        # Handle both list and dict formats
        if isinstance(data, dict):
            # Some files might have a wrapper object
            data = data.get("records", [data])

        return data if isinstance(data, list) else [data]

    def _transform_records(
        self,
        table_name: str,
        records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Transform records for specific table requirements.

        Args:
            table_name: Name of the table
            records: List of records from JSON

        Returns:
            Transformed list of records
        """
        # Table-specific transformations
        if table_name == "benefit_category":
            return self._transform_benefit_category(records)
        elif table_name == "complaint_category":
            return self._transform_complaint_category(records)
        elif table_name == "interaction_type":
            return self._transform_interaction_type(records)

        # No transformation needed
        return records

    def _transform_benefit_category(
        self,
        records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Transform benefit_category records.

        Handles self-referential parent_category_id FK by ensuring
        parent categories are ordered before children.

        Args:
            records: List of benefit category records

        Returns:
            Sorted list with parents before children
        """
        # Build parent-child relationships
        by_id = {r["benefit_category_id"]: r for r in records}
        sorted_records = []
        visited = set()

        def add_with_parents(record):
            record_id = record["benefit_category_id"]
            if record_id in visited:
                return

            # Add parent first if exists
            parent_id = record.get("parent_category_id")
            if parent_id and parent_id in by_id:
                add_with_parents(by_id[parent_id])

            sorted_records.append(record)
            visited.add(record_id)

        # Process all records
        for record in records:
            add_with_parents(record)

        logger.debug(
            "benefit_category_sorted",
            total=len(records),
            sorted=len(sorted_records)
        )

        return sorted_records

    def _transform_complaint_category(
        self,
        records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Transform complaint_category records.

        Handles self-referential parent_category_id FK by ensuring
        parent categories are ordered before children.

        Args:
            records: List of complaint category records

        Returns:
            Sorted list with parents before children
        """
        # Build parent-child relationships
        by_id = {r["complaint_category_id"]: r for r in records}
        sorted_records = []
        visited = set()

        def add_with_parents(record):
            record_id = record["complaint_category_id"]
            if record_id in visited:
                return

            # Add parent first if exists
            parent_id = record.get("parent_category_id")
            if parent_id and parent_id in by_id:
                add_with_parents(by_id[parent_id])

            sorted_records.append(record)
            visited.add(record_id)

        # Process all records
        for record in records:
            add_with_parents(record)

        logger.debug(
            "complaint_category_sorted",
            total=len(records),
            sorted=len(sorted_records)
        )

        return sorted_records

    def _transform_interaction_type(
        self,
        records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Transform interaction_type records.

        Note: Originally filtered to "Inquiry" category but JSON doesn't have
        category values, so we load all interaction types.

        Args:
            records: List of interaction type records

        Returns:
            All interaction type records
        """
        # Load all interaction types (JSON doesn't have category field)
        return records

    def _load_table(
        self,
        table_name: str,
        records: list[dict[str, Any]]
    ) -> int:
        """
        Load records into a table using PostgreSQL COPY.

        Args:
            table_name: Name of the table
            records: List of record dictionaries

        Returns:
            Number of records loaded
        """
        if not records:
            return 0

        # Get column names from first record
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)

        # Build tab-separated data in memory
        buffer = StringIO()
        for record in records:
            values = [self._format_value(record.get(col)) for col in columns]
            buffer.write("\t".join(values) + "\n")

        data = buffer.getvalue()

        # Execute COPY using psycopg3 API
        with self.engine.connect() as conn:
            raw_conn = conn.connection.dbapi_connection

            with raw_conn.cursor() as cursor:
                # Use COPY FROM STDIN with text format
                with cursor.copy(
                    f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
                ) as copy:
                    copy.write(data.encode("utf-8"))

            raw_conn.commit()

        return len(records)

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


def load_reference_data(
    engine: Engine,
    reference_path: Path | str
) -> dict[str, int]:
    """
    Convenience function to load all reference data.

    Args:
        engine: SQLAlchemy engine
        reference_path: Path to reference data directory

    Returns:
        Dictionary of table names to record counts
    """
    loader = ReferenceDataDBLoader(engine, reference_path)
    return loader.load_all()
