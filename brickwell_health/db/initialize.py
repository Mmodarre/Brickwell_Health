"""
Database initialization for Brickwell Health Simulator.

Creates all required tables for simulation.
"""

from pathlib import Path

from sqlalchemy import text
import structlog

from brickwell_health.config import load_config
from brickwell_health.db.connection import create_engine_from_config

logger = structlog.get_logger()


# Schema files in dependency order (core tables must be created first)
SCHEMA_FILES = [
    # Core domains (dependency order matters)
    "schema_policy.sql",        # member, application, policy, coverage, waiting_period
    "schema_regulatory.sql",    # lhc_loading, suspension, bank_account (depends on policy)
    "schema_claims.sql",        # claim, hospital_admission, etc. (depends on policy, member, coverage)
    "schema_billing.sql",       # invoice, payment, direct_debit (depends on policy, bank_account)
    # Extended domains
    "schema_member_lifecycle.sql",
    "schema_crm.sql",
    "schema_communication.sql",
    "schema_digital.sql",
    "schema_survey.sql",
    "schema_nba.sql",           # NBA action catalog, recommendations, executions (depends on member, policy, communication, crm)
    # System records (must be last - inserts placeholder data)
    "schema_system.sql",
]


def init_database(
    config_path: str | None = None,
    drop_existing: bool = False,
    enable_cdc: bool = False,
) -> None:
    """
    Initialize the database with all required tables.

    Args:
        config_path: Path to configuration file
        drop_existing: If True, drop all tables before creating
        enable_cdc: If True, create a CDC replication slot for change data capture
    """
    logger.info("loading_configuration")
    config = load_config(config_path)

    logger.info(
        "connecting_to_database",
        host=config.database.host,
        database=config.database.database,
    )
    engine = create_engine_from_config(config.database)

    if drop_existing:
        logger.warning("dropping_existing_tables")
        _drop_all_tables(engine)
        _clear_checkpoints(config.reference_data_path)

    logger.info("creating_tables")
    _execute_schema_files(engine)

    if enable_cdc:
        _setup_cdc_slot(engine)

    logger.info("database_initialized_successfully")


def _execute_schema_files(engine) -> None:
    """Execute all schema SQL files in dependency order."""
    schema_dir = Path(__file__).parent
    
    for schema_file in SCHEMA_FILES:
        schema_path = schema_dir / schema_file
        if schema_path.exists():
            logger.info("executing_schema_file", file=schema_file)
            with open(schema_path) as f:
                schema_sql = f.read()
            
            with engine.connect() as conn:
                try:
                    conn.execute(text(schema_sql))
                    conn.commit()
                except Exception as e:
                    logger.error("schema_file_error", file=schema_file, error=str(e))
                    raise
        else:
            logger.warning("schema_file_not_found", file=schema_file)


def _drop_all_tables(engine) -> None:
    """Drop all tables in reverse dependency order."""
    tables = [
        # NBA Domain (depends on member, policy, communication, crm)
        "nba_action_execution",
        "nba_action_recommendation",
        "nba_action_catalog",
        # Survey Domain
        "csat_survey",
        "csat_survey_pending",
        "nps_survey",
        "nps_survey_pending",
        # Digital Domain
        "digital_event",
        "web_session",
        # Communication Domain
        "campaign_response",
        "communication",
        "campaign",
        "communication_preference",
        # CRM Domain
        "complaint",
        "service_case",
        "interaction",
        # Billing Domain
        "premium_discount",
        "refund",
        "arrears",
        "direct_debit_result",
        "direct_debit_mandate",
        "payment",
        "invoice",
        # Claims Domain
        "benefit_usage",
        "claim_assessment",
        "medical_service",
        "prosthesis_claim",
        "ambulance_claim",
        "extras_claim",
        "hospital_admission",
        "claim_line",
        "claim",
        # Regulatory Domain
        "bank_account",
        "upgrade_request",
        "suspension",
        "phi_rebate_entitlement",
        "age_based_discount",
        "lhc_loading",
        # Policy/Member Domain
        "health_declaration",
        "waiting_period",
        "coverage",
        "policy_member",
        "policy",
        "application_member",
        "application",
        "member_update",
        "member",
    ]

    with engine.connect() as conn:
        for table in tables:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
            except Exception as e:
                logger.warning("drop_table_error", table=table, error=str(e))
        conn.commit()


def _setup_cdc_slot(engine) -> None:
    """
    Set up CDC replication slot and publication for LakeFlow Connect.

    Creates a logical replication slot using pgoutput plugin (required by
    Databricks LakeFlow Connect) and a publication covering all public tables.
    Also sets REPLICA IDENTITY FULL on all tables (required for tables without
    primary keys or with TOAST-able columns like TEXT, JSONB).

    The slot and publication are idempotent - existing ones are dropped first.
    """
    SLOT_NAME = "lakeflow_slot"
    PUB_NAME = "lakeflow_pub"

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection

        try:
            with raw_conn.cursor() as cursor:
                # Drop existing slot if it exists
                cursor.execute(
                    """
                    SELECT pg_drop_replication_slot(slot_name)
                    FROM pg_replication_slots
                    WHERE slot_name = %s
                    """,
                    (SLOT_NAME,)
                )

                # Create replication slot with pgoutput plugin (LakeFlow requirement)
                cursor.execute(
                    "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                    (SLOT_NAME,)
                )
                logger.info("cdc_slot_created", slot_name=SLOT_NAME, plugin="pgoutput")

                # Drop existing publication if it exists
                cursor.execute(f"DROP PUBLICATION IF EXISTS {PUB_NAME}")

                # Get all public tables
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """)
                table_rows = cursor.fetchall()
                table_names = [row[0] for row in table_rows]

                if table_names:
                    # Set REPLICA IDENTITY FULL on all tables
                    # Required for LakeFlow to track changes on tables without PKs
                    # or with TOAST-able columns (TEXT, JSONB, etc.)
                    for table_name in table_names:
                        cursor.execute(
                            f'ALTER TABLE "{table_name}" REPLICA IDENTITY FULL'
                        )
                    logger.info(
                        "replica_identity_set",
                        mode="FULL",
                        table_count=len(table_names),
                    )

                    # Create publication for all tables (quote table names)
                    quoted_tables = [f'"{t}"' for t in table_names]
                    tables_csv = ", ".join(quoted_tables)
                    cursor.execute(f"CREATE PUBLICATION {PUB_NAME} FOR TABLE {tables_csv}")
                    logger.info("publication_created", name=PUB_NAME, table_count=len(table_names))

                    # Verify publication was created with tables
                    cursor.execute("""
                        SELECT COUNT(*) FROM pg_publication_tables
                        WHERE pubname = %s
                    """, (PUB_NAME,))
                    pub_table_count = cursor.fetchone()[0]
                    logger.info("publication_verified", name=PUB_NAME, table_count=pub_table_count)
                else:
                    logger.warning("no_tables_found_for_publication")

                raw_conn.commit()

        except Exception as e:
            logger.warning(
                "cdc_setup_failed",
                slot_name=SLOT_NAME,
                error=str(e),
                hint="Ensure wal_level=logical in PostgreSQL config",
            )


def _clear_checkpoints(reference_data_path: str) -> None:
    """
    Clear all checkpoint files.

    Checkpoints are stored in a 'checkpoints' directory sibling to reference_data_path.
    Stale checkpoints can cause errors when schema or counters change.

    Args:
        reference_data_path: Path to reference data directory (used to locate checkpoints)
    """
    checkpoint_dir = Path(reference_data_path).parent / "checkpoints"

    if not checkpoint_dir.exists():
        logger.debug("checkpoint_dir_not_found", path=str(checkpoint_dir))
        return

    # Count files before clearing
    checkpoint_files = list(checkpoint_dir.glob("checkpoint_*.json"))
    if not checkpoint_files:
        logger.debug("no_checkpoints_to_clear")
        return

    # Clear all checkpoint files
    for checkpoint_file in checkpoint_files:
        try:
            checkpoint_file.unlink()
        except Exception as e:
            logger.warning("checkpoint_delete_error", file=str(checkpoint_file), error=str(e))

    logger.info("checkpoints_cleared", count=len(checkpoint_files))
