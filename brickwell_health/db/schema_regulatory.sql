-- =============================================================================
-- REGULATORY DOMAIN
-- =============================================================================

-- LHC_LOADING: Lifetime Health Cover loading records
CREATE TABLE IF NOT EXISTS regulatory.lhc_loading (
    lhc_loading_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    member_id               UUID NOT NULL REFERENCES policy.member(member_id),
    policy_id               UUID NOT NULL REFERENCES policy.policy(policy_id),

    certified_age_of_entry  INTEGER NOT NULL,
    base_day                DATE NOT NULL,  -- 1 July after 31st birthday

    loading_percentage      DECIMAL(5,2) NOT NULL,  -- 0-70%
    loading_start_date      DATE NOT NULL,
    loading_removal_date    DATE,  -- After 10 continuous years

    continuous_cover_start  DATE,
    years_without_cover     INTEGER DEFAULT 0,

    is_loading_active       BOOLEAN NOT NULL DEFAULT TRUE,

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_lhc_member ON regulatory.lhc_loading(member_id);
CREATE INDEX IF NOT EXISTS idx_lhc_policy ON regulatory.lhc_loading(policy_id);

-- AGE_BASED_DISCOUNT: Youth discount entitlement (18-29 years)
CREATE TABLE IF NOT EXISTS regulatory.age_based_discount (
    age_discount_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    member_id               UUID NOT NULL REFERENCES policy.member(member_id),
    policy_id               UUID NOT NULL REFERENCES policy.policy(policy_id),

    age_at_eligibility      INTEGER NOT NULL,
    discount_percentage     DECIMAL(5,2) NOT NULL,  -- Max 10%
    eligibility_date        DATE NOT NULL,
    phase_out_start_date    DATE NOT NULL,  -- When phase-out begins (age 41)
    phase_out_end_date      DATE NOT NULL,  -- When discount ends (age 51)

    current_discount_pct    DECIMAL(5,2) NOT NULL,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_abd_member ON regulatory.age_based_discount(member_id);
CREATE INDEX IF NOT EXISTS idx_abd_policy ON regulatory.age_based_discount(policy_id);

-- PHI_REBATE_ENTITLEMENT: Government rebate tier per policy
CREATE TABLE IF NOT EXISTS regulatory.phi_rebate_entitlement (
    rebate_entitlement_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy.policy(policy_id),
    financial_year          VARCHAR(9) NOT NULL,  -- e.g., "2024-2025"
    income_tier             VARCHAR(10) NOT NULL,  -- Tier 0/1/2/3
    oldest_member_age_bracket VARCHAR(20) NOT NULL,  -- Under 65/65-69/70+
    rebate_percentage       DECIMAL(6,4) NOT NULL,
    income_declaration_date DATE,
    declared_income_range   VARCHAR(50),
    single_or_family        VARCHAR(10) NOT NULL,  -- Single/Family
    mls_liable              BOOLEAN NOT NULL DEFAULT FALSE,
    effective_date          DATE NOT NULL,
    end_date                DATE,

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_rebate_policy ON regulatory.phi_rebate_entitlement(policy_id);
CREATE INDEX IF NOT EXISTS idx_rebate_fy ON regulatory.phi_rebate_entitlement(financial_year);

-- SUSPENSION: Policy suspension periods
CREATE TABLE IF NOT EXISTS regulatory.suspension (
    suspension_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy.policy(policy_id),
    suspension_type         VARCHAR(30) NOT NULL,  -- Financial Hardship/Overseas Travel/Other
    start_date              DATE NOT NULL,
    expected_end_date       DATE,
    actual_end_date         DATE,
    reason                  VARCHAR(500),
    status                  VARCHAR(20) NOT NULL,  -- Active/Ended/Extended
    max_suspension_days     INTEGER NOT NULL DEFAULT 730,
    days_used               INTEGER NOT NULL DEFAULT 0,
    waiting_period_impact   BOOLEAN NOT NULL DEFAULT FALSE,

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_suspension_policy ON regulatory.suspension(policy_id);
CREATE INDEX IF NOT EXISTS idx_suspension_status ON regulatory.suspension(status);

-- UPGRADE_REQUEST: Product upgrade/downgrade requests
CREATE TABLE IF NOT EXISTS regulatory.upgrade_request (
    upgrade_request_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy.policy(policy_id),
    request_type            VARCHAR(20) NOT NULL,  -- Upgrade/Downgrade/ChangeExcess
    current_product_id      INTEGER NOT NULL,
    requested_product_id    INTEGER NOT NULL,
    current_excess          DECIMAL(10,2),
    requested_excess        DECIMAL(10,2),
    requested_effective_date DATE NOT NULL,
    request_reason          VARCHAR(500),
    request_status          VARCHAR(20) NOT NULL,  -- Pending/Approved/Declined
    submission_date         TIMESTAMP NOT NULL,
    decision_date           TIMESTAMP,
    decision_by             VARCHAR(50),
    requires_waiting_period BOOLEAN,
    waiting_period_details  VARCHAR(500),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_upgrade_policy ON regulatory.upgrade_request(policy_id);
CREATE INDEX IF NOT EXISTS idx_upgrade_status ON regulatory.upgrade_request(request_status);

-- BANK_ACCOUNT: Member bank accounts for payments/refunds
CREATE TABLE IF NOT EXISTS regulatory.bank_account (
    bank_account_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    member_id               UUID NOT NULL REFERENCES policy.member(member_id),
    policy_id               UUID REFERENCES policy.policy(policy_id),
    account_name            VARCHAR(100) NOT NULL,
    bsb                     VARCHAR(7) NOT NULL,
    account_number_masked   VARCHAR(20) NOT NULL,  -- Masked for security
    bank_name               VARCHAR(100),
    account_type            VARCHAR(20) NOT NULL,  -- Savings/Cheque
    purpose                 VARCHAR(30) NOT NULL,  -- PremiumDebit/ClaimRefund/Both
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    is_verified             BOOLEAN NOT NULL DEFAULT FALSE,
    verification_date       DATE,

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_bank_member ON regulatory.bank_account(member_id);
CREATE INDEX IF NOT EXISTS idx_bank_policy ON regulatory.bank_account(policy_id);
