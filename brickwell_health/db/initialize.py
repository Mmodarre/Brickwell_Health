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
    # IFRS 17 / PAA LRC accounting (depends on policy, billing)
    "schema_ifrs17.sql",        # cohort, monthly_balance, monthly_movement, onerous_assessment + billing.acquisition_cost
    # Finance dimensions + IFRS 17 journal lines (Phase 2)
    # Runs AFTER schema_ifrs17.sql because ifrs17.journal_line FKs ifrs17.cohort,
    # and attaches gl_period_id FK constraints to the 3 existing IFRS 17 facts.
    "schema_finance.sql",
    # Management expense journal lines (depends on reference.gl_period, gl_account, cost_centre)
    "schema_management_expense.sql",
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

    # Extend reference.gl_period to cover the whole simulation window. The
    # JSON seed only has 24 months; monthly IFRS 17 journal lines need an FK
    # target for every reporting month, so we top up missing months here.
    _extend_gl_periods(engine, config)

    # Pre-populate IFRS 17 cohort dimension (portfolio x AFY) so that
    # policy.policy.ifrs17_cohort_id FK targets exist before any policy row
    # is inserted by the simulation.
    _populate_ifrs17_cohorts(engine, config)

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



def _extend_gl_periods(engine, config) -> None:
    """
    Top up ``reference.gl_period`` with monthly rows that cover the simulation
    window. The JSON seed is treated as the authoritative PeopleSoft snapshot;
    this helper only inserts months that are not already present and tags them
    with ``created_by='SIMULATION'`` so they can be identified later.

    AU fiscal year convention: July = period 1, fiscal_year labels the FY in
    which June falls (e.g. Jul 2024 - Jun 2025 is fiscal_year 2025).

    Idempotent via ``INSERT ... ON CONFLICT (period_code) DO NOTHING``.
    """
    from calendar import monthrange
    from datetime import date, timedelta

    # Buffer the end date to align with the cohort pre-population so that any
    # reporting month the engine emits has a matching gl_period row.
    buffered_end = config.simulation.end_date + timedelta(days=400)

    # Enumerate the first-of-month date for every month in the range.
    cursor_date = date(config.simulation.start_date.year, config.simulation.start_date.month, 1)
    months: list[tuple[date, date]] = []
    while cursor_date <= buffered_end:
        last_day = date(
            cursor_date.year,
            cursor_date.month,
            monthrange(cursor_date.year, cursor_date.month)[1],
        )
        months.append((cursor_date, last_day))
        # Advance to the first of the next month
        if cursor_date.month == 12:
            cursor_date = date(cursor_date.year + 1, 1, 1)
        else:
            cursor_date = date(cursor_date.year, cursor_date.month + 1, 1)

    if not months:
        logger.info("gl_period_extension_no_months")
        return

    # Resolve the next id to use for synthesised rows so we never collide with
    # the seed data (which uses sequential ids 1..24).
    with engine.connect() as conn:
        max_id_row = conn.execute(
            text("SELECT COALESCE(MAX(period_id), 0) FROM reference.gl_period")
        ).fetchone()
        next_id = int(max_id_row[0]) + 1

        inserted = 0
        for start_d, end_d in months:
            period_code = f"{start_d.year:04d}-{start_d.month:02d}"
            period_name = f"{start_d.strftime('%B')} {start_d.year}"
            # AU fiscal year: Jul-Jun; FY label = year in which June falls.
            if start_d.month >= 7:
                fiscal_year = start_d.year + 1
                period_number = start_d.month - 6  # Jul=1, Aug=2, ... Dec=6
            else:
                fiscal_year = start_d.year
                period_number = start_d.month + 6  # Jan=7, Feb=8, ... Jun=12

            result = conn.execute(
                text(
                    """
                    INSERT INTO reference.gl_period
                        (period_id, period_code, period_name, fiscal_year,
                         period_number, start_date, end_date, status,
                         closed_date, closed_by, created_by)
                    VALUES
                        (:period_id, :period_code, :period_name, :fiscal_year,
                         :period_number, :start_date, :end_date, 'Open',
                         NULL, NULL, 'SIMULATION')
                    ON CONFLICT (period_code) DO NOTHING
                    """
                ),
                {
                    "period_id": next_id,
                    "period_code": period_code,
                    "period_name": period_name,
                    "fiscal_year": fiscal_year,
                    "period_number": period_number,
                    "start_date": start_d,
                    "end_date": end_d,
                },
            )
            if result.rowcount:
                inserted += 1
                next_id += 1
        conn.commit()

    logger.info(
        "gl_period_extended",
        sim_start=str(config.simulation.start_date),
        sim_end=str(config.simulation.end_date),
        months_examined=len(months),
        inserted=inserted,
    )


def _populate_ifrs17_cohorts(engine, config) -> None:
    """
    Pre-populate ``ifrs17.cohort`` with the full (portfolio x AFY) grid that
    overlaps the simulation window.

    We extend the enumerated range by one AFY past ``simulation.end_date`` so
    that policies whose ``effective_date`` spills past the nominal sim end
    (application + decision-time pipeline can push a handful of policies into
    the next AFY) still resolve their FK to ``ifrs17.cohort``.

    Idempotent via ``INSERT ... ON CONFLICT DO NOTHING`` — safe to re-run.
    """
    try:
        from brickwell_health.ifrs17.cohort_mapper import CohortMapper
    except Exception as e:  # pragma: no cover - import guard
        logger.warning("ifrs17_cohort_mapper_unavailable", error=str(e))
        return

    reference_path = Path(config.reference_data_path)
    mapper = CohortMapper.from_reference_path(reference_path)

    # Buffer one extra AFY beyond the nominal end date.
    from datetime import timedelta
    buffered_end = config.simulation.end_date + timedelta(days=400)

    rows = mapper.enumerate_cohorts(
        config.simulation.start_date,
        buffered_end,
    )

    if not rows:
        logger.info("ifrs17_cohorts_no_rows")
        return

    with engine.connect() as conn:
        try:
            for cohort_id, portfolio, afy_label, afy_start, afy_end in rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO ifrs17.cohort
                            (cohort_id, portfolio, afy_label, afy_start_date, afy_end_date)
                        VALUES
                            (:cohort_id, :portfolio, :afy_label, :afy_start, :afy_end)
                        ON CONFLICT (cohort_id) DO NOTHING
                        """
                    ),
                    {
                        "cohort_id": cohort_id,
                        "portfolio": portfolio,
                        "afy_label": afy_label,
                        "afy_start": afy_start,
                        "afy_end": afy_end,
                    },
                )
            conn.commit()
            logger.info("ifrs17_cohorts_populated", count=len(rows))
        except Exception as e:
            logger.warning("ifrs17_cohort_populate_failed", error=str(e))
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
        # Finance Domain (management expense journal lines)
        "finance.journal_line",
        # IFRS 17 Domain (fact tables first, dimension + acquisition cost last)
        "ifrs17.journal_line",
        "ifrs17.onerous_assessment",
        "ifrs17.monthly_movement",
        "ifrs17.monthly_balance",
        "billing.acquisition_cost",
        "ifrs17.cohort",
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
        # Finance dimensions (Phase 2)
        "reference.gl_account_hierarchy",
        "reference.gl_account",
        "reference.gl_period",
        "reference.cost_centre",
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
