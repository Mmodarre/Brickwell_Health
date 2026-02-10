"""
Database initialization for Brickwell Health Simulator.

Creates all required tables for simulation.
"""

from pathlib import Path

from sqlalchemy import text
import structlog

from brickwell_health.config import load_config
from brickwell_health.db.connection import create_engine_from_config
from brickwell_health.db.reference_db_loader import load_reference_data

logger = structlog.get_logger()


# Schema files in dependency order (core tables must be created first)
SCHEMA_FILES = [
    # Create all schemas first (must be executed before any table creation)
    "schema_init.sql",          # Creates 11 schemas: reference, policy, regulatory, claims, billing, member_lifecycle, crm, communication, digital, survey, nba
    # Reference data tables (no dependencies - must be first)
    "schema_reference.sql",     # product, provider, hospital, benefit_category, etc. (reference schema)
    # Core domains (dependency order matters)
    "schema_policy.sql",        # member, application, policy, coverage, waiting_period (policy schema)
    "schema_regulatory.sql",    # lhc_loading, suspension, bank_account (regulatory schema - depends on policy)
    "schema_claims.sql",        # claim, hospital_admission, etc. (claims schema - depends on policy, member, coverage)
    "schema_billing.sql",       # invoice, payment, direct_debit (billing schema - depends on policy, bank_account)
    # Extended domains
    "schema_member_lifecycle.sql",  # member demographic changes (member_lifecycle schema)
    "schema_crm.sql",           # interactions, cases, complaints (crm schema)
    "schema_communication.sql", # campaigns, communications (communication schema)
    "schema_digital.sql",       # web sessions, digital events (digital schema)
    "schema_survey.sql",        # NPS/CSAT surveys (survey schema)
    "schema_nba.sql",           # NBA action catalog, recommendations, executions (nba schema - depends on member, policy, communication, crm)
    # System records (must be last - inserts placeholder data)
    "schema_system.sql",        # System metadata (stays in public schema)
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

    # Load reference data from JSON files into reference tables
    logger.info("loading_reference_data_from_json")
    reference_path = Path(config.reference_data_path)
    stats = load_reference_data(engine, reference_path)
    logger.info("reference_data_loaded", stats=stats)

    # Add foreign key constraints from transactional tables to reference tables
    logger.info("adding_reference_foreign_key_constraints")
    _execute_reference_fk_constraints(engine)

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


def _execute_reference_fk_constraints(engine) -> None:
    """
    Execute reference data foreign key constraints.

    Adds FK constraints from transactional tables to reference tables.
    Must be called AFTER reference data has been loaded.
    """
    schema_dir = Path(__file__).parent
    fk_file = schema_dir / "schema_reference_fk.sql"

    if not fk_file.exists():
        logger.warning("reference_fk_file_not_found", file="schema_reference_fk.sql")
        return

    logger.info("executing_reference_fk_constraints")
    with open(fk_file) as f:
        fk_sql = f.read()

    with engine.connect() as conn:
        try:
            conn.execute(text(fk_sql))
            conn.commit()
            logger.info("reference_fk_constraints_added")
        except Exception as e:
            logger.error("reference_fk_constraint_error", error=str(e))
            raise


def _drop_all_tables(engine) -> None:
    """Drop all tables in reverse dependency order with schema qualification."""
    # First, drop ALL tables in public schema (CASCADE handles dependencies)
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename NOT LIKE 'pg_%'
        """))
        public_tables = [row[0] for row in result]

        for table in public_tables:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS public.{table} CASCADE"))
                logger.debug("dropped_public_table", table=table)
            except Exception as e:
                logger.warning("drop_public_table_error", table=table, error=str(e))

        if public_tables:
            logger.info("dropped_public_tables", count=len(public_tables))
        conn.commit()

    # Then drop schema-qualified tables
    tables = [
        # NBA Domain (depends on member, policy, communication, crm)
        "nba.nba_action_execution",
        "nba.nba_action_recommendation",
        "nba.nba_action_catalog",
        # Survey Domain
        "survey.csat_survey",
        "survey.csat_survey_pending",
        "survey.nps_survey",
        "survey.nps_survey_pending",
        # Digital Domain
        "digital.digital_event",
        "digital.web_session",
        # Communication Domain
        "communication.campaign_response",
        "communication.communication",
        "communication.campaign",
        "communication.communication_preference",
        # CRM Domain
        "crm.complaint",
        "crm.service_case",
        "crm.interaction",
        # Billing Domain
        "billing.premium_discount",
        "billing.refund",
        "billing.arrears",
        "billing.direct_debit_result",
        "billing.payment",
        "billing.invoice",
        # Claims Domain
        "claims.benefit_usage",
        "claims.claim_assessment",
        "claims.medical_service",
        "claims.prosthesis_claim",
        "claims.ambulance_claim",
        "claims.extras_claim",
        "claims.hospital_admission",
        "claims.claim_line",
        "claims.claim",
        # Member Lifecycle Domain
        "member_lifecycle.member_update",
        # Regulatory Domain
        "billing.direct_debit_mandate",
        "regulatory.bank_account",
        "regulatory.upgrade_request",
        "regulatory.suspension",
        "regulatory.phi_rebate_entitlement",
        "regulatory.age_based_discount",
        "regulatory.lhc_loading",
        # Policy/Member Domain
        "policy.health_declaration",
        "policy.waiting_period",
        "policy.coverage",
        "policy.policy_member",
        "policy.policy",
        "policy.application_member",
        "policy.application",
        "policy.member",
        # Reference Tables (drop last - they are referenced by FK constraints)
        "reference.provider_location",
        "reference.communication_template",
        "reference.complaint_category",
        "reference.case_type",
        "reference.interaction_outcome",
        "reference.interaction_type",
        "reference.mbs_item",
        "reference.prosthesis_list_item",
        "reference.extras_item_code",
        "reference.claim_rejection_reason",
        "reference.benefit_category",
        "reference.clinical_category",
        "reference.hospital",
        "reference.provider",
        "reference.excess_option",
        "reference.product",
        "reference.product_tier",
        "reference.campaign_type",
        "reference.survey_type",
        "reference.state_territory",
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
    Databricks LakeFlow Connect) and a publication covering all tables across
    all schemas (reference, policy, regulatory, claims, billing, member_lifecycle,
    crm, communication, digital, survey, nba).

    Also sets REPLICA IDENTITY FULL on all tables (required for tables without
    primary keys or with TOAST-able columns like TEXT, JSONB).

    The slot and publication are idempotent - existing ones are dropped first.
    """
    SLOT_NAME = "lakeflow_slot"
    PUB_NAME = "lakeflow_pub"

    # All schemas to include in CDC
    SCHEMAS = [
        "reference", "policy", "regulatory", "claims", "billing",
        "member_lifecycle", "crm", "communication", "digital", "survey", "nba"
    ]

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

                # Get all tables from all schemas (not just public)
                schema_conditions = " OR ".join([f"table_schema = '{s}'" for s in SCHEMAS])
                cursor.execute(f"""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE ({schema_conditions}) AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name
                """)
                table_rows = cursor.fetchall()

                logger.info(f"Found {len(table_rows)} tables across {len(SCHEMAS)} schemas for CDC")

                if table_rows:
                    # Set REPLICA IDENTITY FULL on all tables
                    # Required for LakeFlow to track changes on tables without PKs
                    # or with TOAST-able columns (TEXT, JSONB, etc.)
                    for schema, table_name in table_rows:
                        cursor.execute(
                            f'ALTER TABLE "{schema}"."{table_name}" REPLICA IDENTITY FULL'
                        )
                    logger.info(
                        "replica_identity_set",
                        mode="FULL",
                        table_count=len(table_rows),
                        schema_count=len(SCHEMAS),
                    )

                    # Create publication for all schema.table combinations
                    quoted_tables = [f'"{s}"."{t}"' for s, t in table_rows]
                    tables_csv = ", ".join(quoted_tables)
                    cursor.execute(f"CREATE PUBLICATION {PUB_NAME} FOR TABLE {tables_csv}")
                    logger.info("publication_created", name=PUB_NAME, table_count=len(table_rows), schema_count=len(SCHEMAS))

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
