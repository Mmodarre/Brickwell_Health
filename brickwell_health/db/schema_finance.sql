-- =============================================================================
-- FINANCE DIMENSIONS + IFRS 17 JOURNAL LINES
-- =============================================================================
-- Adds conventional ERP / PeopleSoft-side finance dimensions (chart of accounts,
-- GL account hierarchy, GL periods, cost centres) and the IFRS 17 journal-line
-- fact that posts double-entry debits/credits keyed to the GL dims.
--
-- Runs AFTER schema_reference.sql (creates `reference` schema) and AFTER
-- schema_ifrs17.sql (creates `ifrs17.cohort`).

-- -----------------------------------------------------------------------------
-- GL_ACCOUNT: Chart of accounts (150 rows loaded from gl_account.json)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.gl_account (
    account_id              INTEGER PRIMARY KEY,
    -- account_code is NOT unique: reference data carries legacy + current
    -- codes for some numeric ranges (e.g. 5400-*, 5500-*). We look up by code
    -- only for posting-rule codes, which are verified distinct.
    account_code            VARCHAR(20) NOT NULL,
    account_name            VARCHAR(200) NOT NULL,
    account_type            VARCHAR(20) NOT NULL
        CHECK (account_type IN ('Asset', 'Liability', 'Revenue', 'Expense', 'Equity')),
    account_subtype         VARCHAR(40),
    parent_account_id       INTEGER,
    hierarchy_level         INTEGER,
    is_posting_account      BOOLEAN NOT NULL DEFAULT FALSE,
    is_control_account      BOOLEAN NOT NULL DEFAULT FALSE,
    normal_balance          VARCHAR(10),
    currency_code           VARCHAR(3),
    cost_centre_required    BOOLEAN NOT NULL DEFAULT FALSE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_gl_account_code ON reference.gl_account(account_code);
CREATE INDEX IF NOT EXISTS idx_gl_account_type ON reference.gl_account(account_type);
CREATE INDEX IF NOT EXISTS idx_gl_account_parent ON reference.gl_account(parent_account_id);

-- -----------------------------------------------------------------------------
-- GL_ACCOUNT_HIERARCHY: Denormalised hierarchy paths for reporting roll-ups
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.gl_account_hierarchy (
    hierarchy_id            INTEGER PRIMARY KEY,
    account_id              INTEGER NOT NULL,
    parent_account_id       INTEGER,
    hierarchy_name          VARCHAR(40),
    hierarchy_path          VARCHAR(200),
    level_1_name            VARCHAR(200),
    level_2_name            VARCHAR(200),
    level_3_name            VARCHAR(200),
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_gl_hierarchy_account ON reference.gl_account_hierarchy(account_id);

-- -----------------------------------------------------------------------------
-- GL_PERIOD: Accounting periods (24 rows from JSON + dynamic extension in
-- initialize.py::_extend_gl_periods for the simulation window)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.gl_period (
    period_id               INTEGER PRIMARY KEY,
    period_code             VARCHAR(10) NOT NULL UNIQUE,
    period_name             VARCHAR(40) NOT NULL,
    fiscal_year             INTEGER NOT NULL,
    period_number           INTEGER NOT NULL,
    start_date              DATE NOT NULL,
    end_date                DATE NOT NULL,
    status                  VARCHAR(10) NOT NULL DEFAULT 'Open',
    closed_date             TIMESTAMP,
    closed_by               VARCHAR(50),
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_gl_period_dates ON reference.gl_period(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_gl_period_fy_num ON reference.gl_period(fiscal_year, period_number);

-- -----------------------------------------------------------------------------
-- COST_CENTRE: Cost centre dimension (25 rows from cost_centre.json)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.cost_centre (
    cost_centre_id          INTEGER PRIMARY KEY,
    cost_centre_code        VARCHAR(20) NOT NULL UNIQUE,
    cost_centre_name        VARCHAR(100) NOT NULL,
    department              VARCHAR(60),
    manager                 VARCHAR(100),
    parent_cost_centre_id   INTEGER,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    budget_owner            VARCHAR(60),
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_cost_centre_code ON reference.cost_centre(cost_centre_code);

-- -----------------------------------------------------------------------------
-- JOURNAL_LINE: Double-entry IFRS 17 postings keyed to GL dims
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ifrs17.journal_line (
    journal_line_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cohort_id               VARCHAR(30) NOT NULL REFERENCES ifrs17.cohort(cohort_id),
    reporting_month         DATE NOT NULL,
    gl_period_id            INTEGER NOT NULL REFERENCES reference.gl_period(period_id),
    gl_account_id           INTEGER NOT NULL REFERENCES reference.gl_account(account_id),
    cost_centre_id          INTEGER REFERENCES reference.cost_centre(cost_centre_id),
    movement_bucket         VARCHAR(40) NOT NULL,
    debit_amount            DECIMAL(14,2) NOT NULL DEFAULT 0,
    credit_amount           DECIMAL(14,2) NOT NULL DEFAULT 0,
    journal_source          VARCHAR(20) NOT NULL DEFAULT 'IFRS17_ENGINE',
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    CHECK ((debit_amount = 0) <> (credit_amount = 0))
);

CREATE INDEX IF NOT EXISTS idx_jl_cohort_month ON ifrs17.journal_line(cohort_id, reporting_month);
CREATE INDEX IF NOT EXISTS idx_jl_account ON ifrs17.journal_line(gl_account_id);
CREATE INDEX IF NOT EXISTS idx_jl_period ON ifrs17.journal_line(gl_period_id);
CREATE INDEX IF NOT EXISTS idx_jl_bucket ON ifrs17.journal_line(movement_bucket);

-- -----------------------------------------------------------------------------
-- Attach gl_period_id FK to existing IFRS 17 fact tables. The column itself is
-- added by schema_ifrs17.sql (runs earlier, when reference.gl_period does not
-- yet exist); the FK constraint is therefore added here.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_mb_gl_period'
    ) THEN
        ALTER TABLE ifrs17.monthly_balance
            ADD CONSTRAINT fk_mb_gl_period
            FOREIGN KEY (gl_period_id) REFERENCES reference.gl_period(period_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_mm_gl_period'
    ) THEN
        ALTER TABLE ifrs17.monthly_movement
            ADD CONSTRAINT fk_mm_gl_period
            FOREIGN KEY (gl_period_id) REFERENCES reference.gl_period(period_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_oa_gl_period'
    ) THEN
        ALTER TABLE ifrs17.onerous_assessment
            ADD CONSTRAINT fk_oa_gl_period
            FOREIGN KEY (gl_period_id) REFERENCES reference.gl_period(period_id);
    END IF;
END $$;
