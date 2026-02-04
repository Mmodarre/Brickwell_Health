-- =============================================================================
-- POLICY ADMINISTRATION DOMAIN
-- =============================================================================

-- MEMBER: Individual persons (merged ADDRESS/CONTACT)
CREATE TABLE IF NOT EXISTS member (
    member_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    member_number           VARCHAR(25) NOT NULL UNIQUE,
    title                   VARCHAR(10),
    first_name              VARCHAR(100) NOT NULL,
    middle_name             VARCHAR(100),
    last_name               VARCHAR(100) NOT NULL,
    preferred_name          VARCHAR(100),
    date_of_birth           DATE NOT NULL,
    gender                  VARCHAR(10) NOT NULL,
    medicare_number         VARCHAR(12),
    medicare_irn            VARCHAR(1),
    medicare_expiry_date    DATE,
    
    -- Merged from ADDRESS
    address_line_1          VARCHAR(200),
    address_line_2          VARCHAR(200),
    suburb                  VARCHAR(100),
    state                   VARCHAR(3) NOT NULL,
    postcode                VARCHAR(10),
    country                 VARCHAR(3) DEFAULT 'AUS',
    
    -- Merged from CONTACT
    email                   VARCHAR(200),
    mobile_phone            VARCHAR(20),
    home_phone              VARCHAR(20),
    
    -- Regulatory
    australian_resident     BOOLEAN DEFAULT TRUE,
    tax_file_number_provided BOOLEAN DEFAULT FALSE,
    lhc_applicable          BOOLEAN DEFAULT FALSE,
    
    -- Demographics
    marital_status          VARCHAR(20) DEFAULT 'Single',
    
    -- Status
    deceased_flag           BOOLEAN DEFAULT FALSE,
    deceased_date           DATE,
    
    -- Audit
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_member_number ON member(member_number);
CREATE INDEX IF NOT EXISTS idx_member_state ON member(state);
CREATE INDEX IF NOT EXISTS idx_member_dob ON member(date_of_birth);
CREATE INDEX IF NOT EXISTS idx_member_marital_status ON member(marital_status);

-- MEMBER_UPDATE: Tracks all member demographic changes
CREATE TABLE IF NOT EXISTS member_update (
    member_update_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    member_id               UUID NOT NULL REFERENCES member(member_id),
    
    change_type             VARCHAR(50) NOT NULL,  -- AddressChange/PhoneChange/EmailChange/NameChange/MaritalStatusChange/MedicareRenewal/PreferredNameUpdate/Death
    change_date             DATE NOT NULL,
    
    previous_values         JSONB,  -- Previous field values
    new_values              JSONB,  -- New field values
    
    reason                  VARCHAR(200),
    triggered_by            VARCHAR(50),  -- SIMULATION/POLICY_EVENT/etc.
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_member_update_member ON member_update(member_id);
CREATE INDEX IF NOT EXISTS idx_member_update_date ON member_update(change_date);
CREATE INDEX IF NOT EXISTS idx_member_update_type ON member_update(change_type);

-- APPLICATION: New policy applications
CREATE TABLE IF NOT EXISTS application (
    application_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_number      VARCHAR(25) NOT NULL UNIQUE,
    application_type        VARCHAR(20) NOT NULL,  -- New/Upgrade/Downgrade/Transfer
    application_status      VARCHAR(20) NOT NULL,  -- Pending/Approved/Declined/Withdrawn
    product_id              INTEGER NOT NULL,
    requested_policy_type   VARCHAR(20) NOT NULL,  -- Single/Couple/Family/SingleParent
    requested_excess        DECIMAL(10,2),
    requested_start_date    DATE NOT NULL,
    channel                 VARCHAR(50) NOT NULL,  -- Online/Phone/Broker/Corporate
    previous_fund_code      VARCHAR(10),
    transfer_certificate_received BOOLEAN DEFAULT FALSE,
    submission_date         TIMESTAMP NOT NULL,
    decision_date           TIMESTAMP,
    decision_by             VARCHAR(50),
    decline_reason          VARCHAR(500),
    state                   VARCHAR(3) NOT NULL,
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_application_number ON application(application_number);
CREATE INDEX IF NOT EXISTS idx_application_status ON application(application_status);
CREATE INDEX IF NOT EXISTS idx_application_submission ON application(submission_date);

-- APPLICATION_MEMBER: Members on application
CREATE TABLE IF NOT EXISTS application_member (
    application_member_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id          UUID NOT NULL REFERENCES application(application_id),
    member_role             VARCHAR(20) NOT NULL,  -- Primary/Partner/Dependent
    title                   VARCHAR(10),
    first_name              VARCHAR(100) NOT NULL,
    middle_name             VARCHAR(100),
    last_name               VARCHAR(100) NOT NULL,
    date_of_birth           DATE NOT NULL,
    gender                  VARCHAR(10) NOT NULL,
    relationship_to_primary VARCHAR(30),  -- Self/Spouse/Child/Other
    medicare_number         VARCHAR(12),
    medicare_irn            VARCHAR(1),
    email                   VARCHAR(200),
    mobile_phone            VARCHAR(20),
    existing_member_id      UUID REFERENCES member(member_id),
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_appmember_application ON application_member(application_id);

-- POLICY: Insurance policies
CREATE TABLE IF NOT EXISTS policy (
    policy_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_number           VARCHAR(25) NOT NULL UNIQUE,
    application_id          UUID REFERENCES application(application_id),
    product_id              INTEGER NOT NULL,
    
    policy_status           VARCHAR(20) NOT NULL,  -- Active/Suspended/Cancelled/Lapsed
    policy_type             VARCHAR(20) NOT NULL,  -- Single/Couple/Family/SingleParent
    
    effective_date          DATE NOT NULL,
    end_date                DATE,
    cancellation_reason     VARCHAR(200),
    
    payment_frequency       VARCHAR(20) NOT NULL DEFAULT 'Monthly',
    premium_amount          DECIMAL(10,2) NOT NULL,
    excess_amount           DECIMAL(10,2),
    
    government_rebate_tier  VARCHAR(10),
    rebate_claimed_as       VARCHAR(20),  -- ReducedPremium/TaxReturn
    
    distribution_channel    VARCHAR(50) NOT NULL,
    state_of_residence      VARCHAR(3) NOT NULL,
    
    original_join_date      DATE NOT NULL,
    previous_fund_code      VARCHAR(10),
    transfer_certificate_date DATE,
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_policy_number ON policy(policy_number);
CREATE INDEX IF NOT EXISTS idx_policy_status ON policy(policy_status);
CREATE INDEX IF NOT EXISTS idx_policy_product ON policy(product_id);
CREATE INDEX IF NOT EXISTS idx_policy_effective ON policy(effective_date);

-- POLICY_MEMBER: Links members to policies with roles
CREATE TABLE IF NOT EXISTS policy_member (
    policy_member_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy(policy_id),
    member_id               UUID NOT NULL REFERENCES member(member_id),
    
    member_role             VARCHAR(20) NOT NULL,  -- Primary/Partner/Dependent
    relationship_to_primary VARCHAR(30) NOT NULL,  -- Self/Spouse/Child/Other
    
    effective_date          DATE NOT NULL,
    end_date                DATE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_pm_policy ON policy_member(policy_id);
CREATE INDEX IF NOT EXISTS idx_pm_member ON policy_member(member_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pm_active ON policy_member(policy_id, member_id) 
    WHERE is_active = TRUE;

-- COVERAGE: Active coverages on a policy
CREATE TABLE IF NOT EXISTS coverage (
    coverage_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy(policy_id),
    
    coverage_type           VARCHAR(20) NOT NULL,  -- Hospital/Extras/Ambulance
    product_id              INTEGER NOT NULL,
    
    effective_date          DATE NOT NULL,
    end_date                DATE,
    status                  VARCHAR(20) NOT NULL DEFAULT 'Active',
    
    tier                    VARCHAR(20),  -- Gold/Silver/Bronze/Basic (hospital only)
    excess_amount           DECIMAL(10,2),
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_coverage_policy ON coverage(policy_id);
CREATE INDEX IF NOT EXISTS idx_coverage_type ON coverage(coverage_type);

-- WAITING_PERIOD: Waiting periods by coverage type per member
CREATE TABLE IF NOT EXISTS waiting_period (
    waiting_period_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_member_id        UUID NOT NULL REFERENCES policy_member(policy_member_id),
    coverage_id             UUID NOT NULL REFERENCES coverage(coverage_id),
    
    waiting_period_type     VARCHAR(50) NOT NULL,  -- General/Pre-existing/Obstetric/Psychiatric
    benefit_category_id     INTEGER,  -- FK to reference.benefit_category (NULL = all)
    clinical_category_id    INTEGER,  -- FK to reference.clinical_category
    
    start_date              DATE NOT NULL,
    end_date                DATE NOT NULL,
    duration_months         INTEGER NOT NULL,
    
    status                  VARCHAR(20) NOT NULL DEFAULT 'InProgress',  -- InProgress/Completed/Waived
    waiver_reason           VARCHAR(200),
    
    -- Merged from WAITING_PERIOD_EXEMPTION
    exemption_granted       BOOLEAN DEFAULT FALSE,
    exemption_type          VARCHAR(50),
    exemption_reason        VARCHAR(200),
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_wp_policy_member ON waiting_period(policy_member_id);
CREATE INDEX IF NOT EXISTS idx_wp_coverage ON waiting_period(coverage_id);
CREATE INDEX IF NOT EXISTS idx_wp_status ON waiting_period(status);
CREATE INDEX IF NOT EXISTS idx_wp_end_date ON waiting_period(end_date);

-- HEALTH_DECLARATION: Health questions and declarations
CREATE TABLE IF NOT EXISTS health_declaration (
    health_declaration_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_member_id   UUID NOT NULL REFERENCES application_member(application_member_id),
    application_id          UUID NOT NULL REFERENCES application(application_id),
    question_code           VARCHAR(20) NOT NULL,
    question_text           VARCHAR(500) NOT NULL,
    response                VARCHAR(10) NOT NULL,  -- Yes/No
    response_details        VARCHAR(2000),
    declaration_date        TIMESTAMP NOT NULL,
    declaration_acknowledged BOOLEAN NOT NULL DEFAULT TRUE,
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_hd_application ON health_declaration(application_id);
