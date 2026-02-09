-- =============================================================================
-- REFERENCE DATA TABLES
-- Loaded from JSON during init-db, read-only during simulation
-- =============================================================================

-- PRODUCT_TIER: Hospital cover tier classifications (Gold, Silver, Bronze, Basic)
CREATE TABLE IF NOT EXISTS product_tier (
    product_tier_id         INTEGER PRIMARY KEY,
    tier_code               VARCHAR(50) NOT NULL UNIQUE,
    tier_name               VARCHAR(100) NOT NULL,
    tier_level              INTEGER NOT NULL,
    description             TEXT,
    min_clinical_categories INTEGER,
    effective_date          DATE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    display_color           VARCHAR(20),
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_product_tier_code ON product_tier(tier_code);
CREATE INDEX IF NOT EXISTS idx_product_tier_level ON product_tier(tier_level);

-- EXCESS_OPTION: Hospital excess configurations per product
CREATE TABLE IF NOT EXISTS excess_option (
    excess_option_id        INTEGER PRIMARY KEY,
    product_id              INTEGER NOT NULL,
    excess_amount           DECIMAL(10,2) NOT NULL,
    excess_type             VARCHAR(50) NOT NULL,
    annual_max_excess       DECIMAL(10,2),
    premium_discount_pct    DECIMAL(5,2),
    applies_to_singles      BOOLEAN NOT NULL DEFAULT TRUE,
    applies_to_families     BOOLEAN NOT NULL DEFAULT TRUE,
    children_excess_waived  BOOLEAN NOT NULL DEFAULT TRUE,
    effective_date          DATE,
    end_date                DATE,
    is_default              BOOLEAN NOT NULL DEFAULT FALSE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_excess_option_product ON excess_option(product_id);
CREATE INDEX IF NOT EXISTS idx_excess_option_type ON excess_option(excess_type);
CREATE INDEX IF NOT EXISTS idx_excess_option_active ON excess_option(is_active);

-- CAMPAIGN_TYPE: Marketing campaign type classifications
CREATE TABLE IF NOT EXISTS campaign_type (
    type_id                 INTEGER PRIMARY KEY,
    type_code               VARCHAR(50) NOT NULL UNIQUE,
    type_name               VARCHAR(255) NOT NULL,
    description             TEXT,
    target_response_rate    DECIMAL(5,4),
    typical_duration_weeks  INTEGER,
    distribution_weight     DECIMAL(5,2),
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_campaign_type_code ON campaign_type(type_code);

-- SURVEY_TYPE: Survey type classifications (NPS, CSAT)
CREATE TABLE IF NOT EXISTS survey_type (
    type_id                 INTEGER PRIMARY KEY,
    type_code               VARCHAR(50) NOT NULL UNIQUE,
    type_name               VARCHAR(255) NOT NULL,
    survey_class            VARCHAR(50) NOT NULL,
    trigger_event           VARCHAR(100),
    delay_hours             INTEGER,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_survey_type_code ON survey_type(type_code);
CREATE INDEX IF NOT EXISTS idx_survey_type_class ON survey_type(survey_class);
CREATE INDEX IF NOT EXISTS idx_survey_type_trigger ON survey_type(trigger_event);

-- STATE_TERRITORY: Australian states and territories
-- Must be created first as it may be referenced by other tables
CREATE TABLE IF NOT EXISTS state_territory (
    state_territory_id      INTEGER PRIMARY KEY,
    state_code              VARCHAR(10) NOT NULL UNIQUE,
    state_name              VARCHAR(100) NOT NULL,
    ambulance_scheme        VARCHAR(100),
    ambulance_provider      VARCHAR(255),
    has_reciprocal_ambulance BOOLEAN,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_state_code ON state_territory(state_code);

-- PRODUCT: Insurance product catalog
CREATE TABLE IF NOT EXISTS product (
    product_id              INTEGER PRIMARY KEY,
    product_code            VARCHAR(50) NOT NULL UNIQUE,
    product_name            VARCHAR(255) NOT NULL,
    product_type_id         INTEGER NOT NULL,
    product_tier_id         INTEGER,
    description             TEXT,
    is_hospital             BOOLEAN NOT NULL DEFAULT FALSE,
    is_extras               BOOLEAN NOT NULL DEFAULT FALSE,
    is_ambulance            BOOLEAN NOT NULL DEFAULT FALSE,
    default_excess          DECIMAL(10,2),
    status                  VARCHAR(50) NOT NULL DEFAULT 'Active',
    effective_date          DATE,
    end_date                DATE,
    min_age                 INTEGER,
    max_age                 INTEGER,
    available_policy_types  VARCHAR(255),
    is_community_rated      BOOLEAN,
    government_rebate_eligible BOOLEAN,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100),
    modified_date           TIMESTAMP,
    modified_by             VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_product_code ON product(product_code);
CREATE INDEX IF NOT EXISTS idx_product_status ON product(status);
CREATE INDEX IF NOT EXISTS idx_product_tier ON product(product_tier_id);

-- PROVIDER: Healthcare providers (doctors, specialists, allied health)
CREATE TABLE IF NOT EXISTS provider (
    provider_id             INTEGER PRIMARY KEY,
    provider_number         VARCHAR(50) NOT NULL UNIQUE,
    provider_type_id        INTEGER NOT NULL,
    provider_category       VARCHAR(50),
    title                   VARCHAR(20),
    first_name              VARCHAR(100),
    last_name               VARCHAR(100),
    practice_name           VARCHAR(255),
    abn                     VARCHAR(11),
    ahpra_number            VARCHAR(50),
    specialty_id            INTEGER,
    email                   VARCHAR(255),
    phone                   VARCHAR(20),
    fax                     VARCHAR(20),
    preferred_provider      BOOLEAN DEFAULT FALSE,
    network_id              INTEGER,
    status                  VARCHAR(50) NOT NULL DEFAULT 'Active',
    registration_date       DATE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100),
    modified_date           TIMESTAMP,
    modified_by             VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_provider_number ON provider(provider_number);
CREATE INDEX IF NOT EXISTS idx_provider_type ON provider(provider_type_id);
CREATE INDEX IF NOT EXISTS idx_provider_status ON provider(status);
CREATE INDEX IF NOT EXISTS idx_provider_specialty ON provider(specialty_id);

-- HOSPITAL: Hospital facilities
CREATE TABLE IF NOT EXISTS hospital (
    hospital_id             INTEGER PRIMARY KEY,
    hospital_code           VARCHAR(50) NOT NULL UNIQUE,
    hospital_name           VARCHAR(255) NOT NULL,
    hospital_type_id        INTEGER NOT NULL,
    abn                     VARCHAR(11),
    address_line_1          VARCHAR(255),
    address_line_2          VARCHAR(255),
    suburb                  VARCHAR(100),
    state                   VARCHAR(10),
    postcode                VARCHAR(10),
    phone                   VARCHAR(20),
    fax                     VARCHAR(20),
    email                   VARCHAR(255),
    latitude                DECIMAL(10,8),
    longitude               DECIMAL(11,8),
    total_beds              INTEGER,
    emergency_department    BOOLEAN DEFAULT FALSE,
    icu_available           BOOLEAN DEFAULT FALSE,
    maternity_available     BOOLEAN DEFAULT FALSE,
    is_contracted           BOOLEAN DEFAULT TRUE,
    status                  VARCHAR(50) NOT NULL DEFAULT 'Active',
    created_date            TIMESTAMP,
    created_by              VARCHAR(100),
    modified_date           TIMESTAMP,
    modified_by             VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_hospital_code ON hospital(hospital_code);
CREATE INDEX IF NOT EXISTS idx_hospital_state ON hospital(state);
CREATE INDEX IF NOT EXISTS idx_hospital_status ON hospital(status);

-- CLINICAL_CATEGORY: Hospital clinical categories (38 categories)
CREATE TABLE IF NOT EXISTS clinical_category (
    clinical_category_id    INTEGER PRIMARY KEY,
    category_code           VARCHAR(50) NOT NULL UNIQUE,
    category_name           VARCHAR(255) NOT NULL,
    category_number         INTEGER,
    description             TEXT,
    is_minimum_for_gold     BOOLEAN DEFAULT FALSE,
    is_minimum_for_silver   BOOLEAN DEFAULT FALSE,
    is_minimum_for_bronze   BOOLEAN DEFAULT FALSE,
    is_minimum_for_basic    BOOLEAN DEFAULT FALSE,
    can_be_restricted       BOOLEAN DEFAULT FALSE,
    effective_date          DATE,
    end_date                DATE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_clinical_category_code ON clinical_category(category_code);
CREATE INDEX IF NOT EXISTS idx_clinical_category_active ON clinical_category(is_active);

-- BENEFIT_CATEGORY: Extras and hospital benefit categories
CREATE TABLE IF NOT EXISTS benefit_category (
    benefit_category_id     INTEGER PRIMARY KEY,
    category_code           VARCHAR(50) NOT NULL UNIQUE,
    category_name           VARCHAR(255) NOT NULL,
    parent_category_id      INTEGER REFERENCES benefit_category(benefit_category_id),
    category_type           VARCHAR(50),
    description             TEXT,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    display_order           INTEGER,
    icon_name               VARCHAR(100),
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_benefit_category_code ON benefit_category(category_code);
CREATE INDEX IF NOT EXISTS idx_benefit_category_parent ON benefit_category(parent_category_id);

-- CLAIM_REJECTION_REASON: Reasons for claim rejection
CREATE TABLE IF NOT EXISTS claim_rejection_reason (
    rejection_reason_id     INTEGER PRIMARY KEY,
    reason_code             VARCHAR(50) NOT NULL UNIQUE,
    reason_description      VARCHAR(255) NOT NULL,
    category                VARCHAR(50) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rejection_reason_code ON claim_rejection_reason(reason_code);

-- EXTRAS_ITEM_CODE: Extras/ancillary service codes (dental, optical, etc.)
CREATE TABLE IF NOT EXISTS extras_item_code (
    extras_item_id          INTEGER PRIMARY KEY,
    item_code               VARCHAR(50) NOT NULL UNIQUE,
    item_description        VARCHAR(255) NOT NULL,
    service_type_id         INTEGER NOT NULL,
    standard_code_system    VARCHAR(50),
    typical_fee             DECIMAL(10,2),
    effective_date          DATE,
    end_date                DATE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_extras_item_code ON extras_item_code(item_code);
CREATE INDEX IF NOT EXISTS idx_extras_service_type ON extras_item_code(service_type_id);

-- PROSTHESIS_LIST_ITEM: Prosthesis items (implants, devices)
CREATE TABLE IF NOT EXISTS prosthesis_list_item (
    prosthesis_item_id      INTEGER PRIMARY KEY,
    prosthesis_category_id  INTEGER NOT NULL,
    billing_code            VARCHAR(50) NOT NULL UNIQUE,
    item_name               VARCHAR(255) NOT NULL,
    manufacturer            VARCHAR(255),
    brand_name              VARCHAR(255),
    minimum_benefit         DECIMAL(12,2),
    maximum_benefit         DECIMAL(12,2),
    no_gap_benefit          DECIMAL(12,2),
    effective_date          DATE,
    end_date                DATE,
    is_current              BOOLEAN DEFAULT TRUE,
    requires_clinical_justification BOOLEAN DEFAULT FALSE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_prosthesis_billing_code ON prosthesis_list_item(billing_code);
CREATE INDEX IF NOT EXISTS idx_prosthesis_category ON prosthesis_list_item(prosthesis_category_id);

-- MBS_ITEM: Medicare Benefits Schedule items
CREATE TABLE IF NOT EXISTS mbs_item (
    mbs_item_id             INTEGER PRIMARY KEY,
    item_number             VARCHAR(10) NOT NULL UNIQUE,
    item_description        TEXT NOT NULL,
    category_id             INTEGER NOT NULL,
    schedule_fee            DECIMAL(10,2),
    benefit_75              DECIMAL(10,2),
    benefit_85              DECIMAL(10,2),
    is_anaes_applicable     BOOLEAN DEFAULT FALSE,
    is_assist_applicable    BOOLEAN DEFAULT FALSE,
    multiple_procedure_rule VARCHAR(255),
    is_derived_fee          BOOLEAN DEFAULT FALSE,
    derived_fee_rule        VARCHAR(255),
    effective_date          DATE,
    end_date                DATE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_mbs_item_number ON mbs_item(item_number);
CREATE INDEX IF NOT EXISTS idx_mbs_category ON mbs_item(category_id);

-- INTERACTION_TYPE: CRM interaction types
CREATE TABLE IF NOT EXISTS interaction_type (
    interaction_type_id     INTEGER PRIMARY KEY,
    type_code               VARCHAR(50) NOT NULL UNIQUE,
    type_name               VARCHAR(255) NOT NULL,
    type_category           VARCHAR(50),
    requires_case           BOOLEAN DEFAULT FALSE,
    target_resolution_hours INTEGER,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_interaction_type_code ON interaction_type(type_code);
CREATE INDEX IF NOT EXISTS idx_interaction_type_category ON interaction_type(type_category);

-- INTERACTION_OUTCOME: CRM interaction outcomes
CREATE TABLE IF NOT EXISTS interaction_outcome (
    outcome_id              INTEGER PRIMARY KEY,
    outcome_code            VARCHAR(50) NOT NULL UNIQUE,
    outcome_name            VARCHAR(255) NOT NULL,
    is_resolution           BOOLEAN DEFAULT FALSE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_outcome_code ON interaction_outcome(outcome_code);

-- CASE_TYPE: Service case types
CREATE TABLE IF NOT EXISTS case_type (
    case_type_id            INTEGER PRIMARY KEY,
    type_code               VARCHAR(50) NOT NULL UNIQUE,
    type_name               VARCHAR(255) NOT NULL,
    type_category           VARCHAR(50),
    default_priority        VARCHAR(50),
    sla_hours               INTEGER,
    requires_approval       BOOLEAN DEFAULT FALSE,
    workflow_id             INTEGER,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_case_type_code ON case_type(type_code);

-- COMPLAINT_CATEGORY: Complaint categories
CREATE TABLE IF NOT EXISTS complaint_category (
    complaint_category_id   INTEGER PRIMARY KEY,
    category_code           VARCHAR(50) NOT NULL UNIQUE,
    category_name           VARCHAR(255) NOT NULL,
    parent_category_id      INTEGER REFERENCES complaint_category(complaint_category_id),
    sla_days                INTEGER,
    phio_reportable         BOOLEAN DEFAULT FALSE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_complaint_category_code ON complaint_category(category_code);

-- COMMUNICATION_TEMPLATE: Communication templates
CREATE TABLE IF NOT EXISTS communication_template (
    template_id             INTEGER PRIMARY KEY,
    template_code           VARCHAR(50) NOT NULL UNIQUE,
    template_name           VARCHAR(255) NOT NULL,
    template_category       VARCHAR(50),
    default_channel         VARCHAR(50),
    subject_template        TEXT,
    trigger_event           VARCHAR(100),
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_date            TIMESTAMP,
    created_by              VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_template_code ON communication_template(template_code);
CREATE INDEX IF NOT EXISTS idx_template_trigger ON communication_template(trigger_event);

-- PROVIDER_LOCATION: Provider office locations
-- Must be created after provider table
CREATE TABLE IF NOT EXISTS provider_location (
    provider_location_id    INTEGER PRIMARY KEY,
    provider_id             INTEGER NOT NULL REFERENCES provider(provider_id),
    location_name           VARCHAR(255),
    address_line_1          VARCHAR(255),
    address_line_2          VARCHAR(255),
    suburb                  VARCHAR(100),
    state                   VARCHAR(10),
    postcode                VARCHAR(10),
    phone                   VARCHAR(20),
    fax                     VARCHAR(20),
    email                   VARCHAR(255),
    latitude                DECIMAL(10,8),
    longitude               DECIMAL(11,8),
    is_primary              BOOLEAN DEFAULT FALSE,
    accepts_hicaps          BOOLEAN DEFAULT TRUE,
    wheelchair_accessible   BOOLEAN DEFAULT FALSE,
    parking_available       BOOLEAN DEFAULT FALSE,
    status                  VARCHAR(50) NOT NULL DEFAULT 'Active',
    created_date            TIMESTAMP,
    created_by              VARCHAR(100),
    modified_date           TIMESTAMP,
    modified_by             VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_provider_location_provider ON provider_location(provider_id);
CREATE INDEX IF NOT EXISTS idx_provider_location_state ON provider_location(state);
