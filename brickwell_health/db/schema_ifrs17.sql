-- =============================================================================
-- IFRS 17 / PAA LRC ACCOUNTING DOMAIN
-- =============================================================================
-- Supports the Premium Allocation Approach (PAA) measurement model for
-- short-duration PHI contracts (AASB 17 / IFRS 17).
--
-- Tables:
--   billing.acquisition_cost    - Policy-level commission / DAC rows
--   ifrs17.cohort               - Portfolio + AFY dimension
--   ifrs17.monthly_balance      - Point-in-time LRC / LIC state per (cohort, month)
--   ifrs17.monthly_movement     - P&L / roll-forward movements per (cohort, month)
--   ifrs17.onerous_assessment   - Onerous-contract evaluation per (cohort, month)
--
-- Also adds policy.policy.ifrs17_cohort_id for cohort assignment at inception.

CREATE SCHEMA IF NOT EXISTS ifrs17;

-- -----------------------------------------------------------------------------
-- ACQUISITION_COST: Commission / deferred acquisition cost rows
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS billing.acquisition_cost (
    acquisition_cost_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy.policy(policy_id),

    commission_type         VARCHAR(30) NOT NULL,   -- Upfront/Trail/Clawback
    distribution_channel    VARCHAR(20) NOT NULL,

    gross_written_premium   DECIMAL(12,2) NOT NULL,
    commission_rate         DECIMAL(6,4) NOT NULL,
    commission_amount       DECIMAL(12,2) NOT NULL,

    incurred_date           DATE NOT NULL,
    amortisation_start_date DATE NOT NULL,
    amortisation_end_date   DATE NOT NULL,

    status                  VARCHAR(20) NOT NULL DEFAULT 'Active',
    clawback_date           DATE,
    clawback_amount         DECIMAL(12,2),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_acq_cost_policy ON billing.acquisition_cost(policy_id);
CREATE INDEX IF NOT EXISTS idx_acq_cost_incurred ON billing.acquisition_cost(incurred_date);
CREATE INDEX IF NOT EXISTS idx_acq_cost_status_date ON billing.acquisition_cost(status, incurred_date);

-- -----------------------------------------------------------------------------
-- COHORT: Portfolio x Australian Financial Year dimension
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ifrs17.cohort (
    cohort_id                   VARCHAR(30) PRIMARY KEY,
    portfolio                   VARCHAR(20) NOT NULL
        CHECK (portfolio IN ('HOSPITAL_ONLY', 'EXTRAS_ONLY', 'COMBINED', 'AMBULANCE_ONLY')),
    afy_label                   VARCHAR(10) NOT NULL,
    afy_start_date              DATE NOT NULL,
    afy_end_date                DATE NOT NULL,
    is_onerous_at_inception     BOOLEAN NOT NULL DEFAULT FALSE,
    onerous_first_detected_month DATE,
    created_at                  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cohort_portfolio ON ifrs17.cohort(portfolio);
CREATE INDEX IF NOT EXISTS idx_cohort_afy ON ifrs17.cohort(afy_start_date);

-- -----------------------------------------------------------------------------
-- MONTHLY_BALANCE: Point-in-time LRC / LIC / DAC state
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ifrs17.monthly_balance (
    monthly_balance_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cohort_id                   VARCHAR(30) NOT NULL REFERENCES ifrs17.cohort(cohort_id),
    reporting_month             DATE NOT NULL,

    policy_count                INTEGER NOT NULL DEFAULT 0,
    in_force_premium            DECIMAL(14,2) NOT NULL DEFAULT 0,

    lrc_excl_loss_component     DECIMAL(14,2) NOT NULL DEFAULT 0,
    loss_component              DECIMAL(14,2) NOT NULL DEFAULT 0,
    lrc_total                   DECIMAL(14,2) NOT NULL DEFAULT 0,

    lic_best_estimate           DECIMAL(14,2) NOT NULL DEFAULT 0,
    lic_risk_adjustment         DECIMAL(14,2) NOT NULL DEFAULT 0,
    lic_ibnr                    DECIMAL(14,2) NOT NULL DEFAULT 0,
    lic_total                   DECIMAL(14,2) NOT NULL DEFAULT 0,

    deferred_acquisition_cost   DECIMAL(14,2) NOT NULL DEFAULT 0,

    is_onerous                  BOOLEAN NOT NULL DEFAULT FALSE,

    created_at                  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (cohort_id, reporting_month)
);

CREATE INDEX IF NOT EXISTS idx_mb_cohort ON ifrs17.monthly_balance(cohort_id);
CREATE INDEX IF NOT EXISTS idx_mb_month ON ifrs17.monthly_balance(reporting_month);

-- -----------------------------------------------------------------------------
-- MONTHLY_MOVEMENT: P&L / LRC roll-forward per (cohort, month)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ifrs17.monthly_movement (
    monthly_movement_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cohort_id                   VARCHAR(30) NOT NULL REFERENCES ifrs17.cohort(cohort_id),
    reporting_month             DATE NOT NULL,

    opening_lrc                 DECIMAL(14,2) NOT NULL DEFAULT 0,
    premiums_received           DECIMAL(14,2) NOT NULL DEFAULT 0,
    insurance_revenue           DECIMAL(14,2) NOT NULL DEFAULT 0,
    insurance_service_expense   DECIMAL(14,2) NOT NULL DEFAULT 0,
    claims_incurred             DECIMAL(14,2) NOT NULL DEFAULT 0,
    acquisition_cost_amortised  DECIMAL(14,2) NOT NULL DEFAULT 0,
    loss_component_recognised   DECIMAL(14,2) NOT NULL DEFAULT 0,
    loss_component_reversed     DECIMAL(14,2) NOT NULL DEFAULT 0,
    closing_lrc                 DECIMAL(14,2) NOT NULL DEFAULT 0,
    insurance_service_result    DECIMAL(14,2) NOT NULL DEFAULT 0,

    created_at                  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (cohort_id, reporting_month)
);

CREATE INDEX IF NOT EXISTS idx_mm_cohort ON ifrs17.monthly_movement(cohort_id);
CREATE INDEX IF NOT EXISTS idx_mm_month ON ifrs17.monthly_movement(reporting_month);

-- -----------------------------------------------------------------------------
-- ONEROUS_ASSESSMENT: Combined-ratio evaluation + loss component changes
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ifrs17.onerous_assessment (
    assessment_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cohort_id                   VARCHAR(30) NOT NULL REFERENCES ifrs17.cohort(cohort_id),
    reporting_month             DATE NOT NULL,

    expected_remaining_premium  DECIMAL(14,2) NOT NULL DEFAULT 0,
    expected_remaining_claims   DECIMAL(14,2) NOT NULL DEFAULT 0,
    expected_remaining_expenses DECIMAL(14,2) NOT NULL DEFAULT 0,
    expected_combined_ratio     DECIMAL(6,4),

    onerous_threshold_crossed   BOOLEAN NOT NULL DEFAULT FALSE,
    loss_component_change       DECIMAL(14,2) NOT NULL DEFAULT 0,
    notes                       TEXT,

    created_at                  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (cohort_id, reporting_month)
);

CREATE INDEX IF NOT EXISTS idx_oa_cohort ON ifrs17.onerous_assessment(cohort_id);
CREATE INDEX IF NOT EXISTS idx_oa_month ON ifrs17.onerous_assessment(reporting_month);

-- -----------------------------------------------------------------------------
-- Migration: link policies to cohorts
-- -----------------------------------------------------------------------------
ALTER TABLE policy.policy
    ADD COLUMN IF NOT EXISTS ifrs17_cohort_id VARCHAR(30)
    REFERENCES ifrs17.cohort(cohort_id);

CREATE INDEX IF NOT EXISTS idx_policy_ifrs17_cohort
    ON policy.policy(ifrs17_cohort_id);

-- -----------------------------------------------------------------------------
-- Phase 2: GL period FK on each fact row (nullable for Phase 1 backward compat;
-- engine always populates). FK to reference.gl_period added in schema_finance.sql.
-- -----------------------------------------------------------------------------
ALTER TABLE ifrs17.monthly_balance
    ADD COLUMN IF NOT EXISTS gl_period_id INTEGER;

ALTER TABLE ifrs17.monthly_movement
    ADD COLUMN IF NOT EXISTS gl_period_id INTEGER;

ALTER TABLE ifrs17.onerous_assessment
    ADD COLUMN IF NOT EXISTS gl_period_id INTEGER;
