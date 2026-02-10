-- =============================================================================
-- CLAIMS DOMAIN
-- =============================================================================

-- CLAIM: Claim header record
CREATE TABLE IF NOT EXISTS claims.claim (
    claim_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_number            VARCHAR(25) NOT NULL UNIQUE,

    policy_id               UUID NOT NULL REFERENCES policy.policy(policy_id),
    member_id               UUID NOT NULL REFERENCES policy.member(member_id),
    coverage_id             UUID NOT NULL REFERENCES policy.coverage(coverage_id),

    claim_type              VARCHAR(20) NOT NULL,  -- Hospital/Extras/Ambulance
    claim_status            VARCHAR(20) NOT NULL DEFAULT 'Submitted',

    service_date            DATE NOT NULL,
    lodgement_date          DATE NOT NULL,
    assessment_date         DATE,
    payment_date            DATE,

    provider_id             INTEGER,  -- FK to reference.provider
    hospital_id             INTEGER,  -- FK to reference.hospital

    total_charge            DECIMAL(12,2) NOT NULL,
    total_benefit           DECIMAL(12,2),
    total_gap               DECIMAL(12,2),

    excess_applied          DECIMAL(10,2) DEFAULT 0,
    co_payment_applied      DECIMAL(10,2) DEFAULT 0,

    rejection_reason_id     INTEGER,
    rejection_notes         VARCHAR(500),

    claim_channel           VARCHAR(20) NOT NULL,  -- Online/HICAPS/Paper/Hospital
    pay_to                  VARCHAR(20) DEFAULT 'Member',  -- Provider/Member

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50),

    -- Fraud metadata (NULL for legitimate claims)
    is_fraud                BOOLEAN DEFAULT FALSE,
    fraud_type              VARCHAR(30),   -- FraudType enum value
    fraud_original_charge   DECIMAL(12,2), -- Original charge before inflation
    fraud_inflation_amount  DECIMAL(12,2), -- Amount of inflation added
    fraud_inflation_ratio   DECIMAL(6,3),  -- Inflation ratio (inflated/original)
    fraud_source_claim_id   UUID,          -- Source claim for duplicates
    fraud_ring_id           UUID           -- Shared ID for phantom billing rings
);

CREATE INDEX IF NOT EXISTS idx_claim_number ON claims.claim(claim_number);
CREATE INDEX IF NOT EXISTS idx_claim_policy ON claims.claim(policy_id);
CREATE INDEX IF NOT EXISTS idx_claim_member ON claims.claim(member_id);
CREATE INDEX IF NOT EXISTS idx_claim_service_date ON claims.claim(service_date);
CREATE INDEX IF NOT EXISTS idx_claim_status ON claims.claim(claim_status);
CREATE INDEX IF NOT EXISTS idx_claim_is_fraud ON claims.claim(is_fraud) WHERE is_fraud = TRUE;
CREATE INDEX IF NOT EXISTS idx_claim_fraud_type ON claims.claim(fraud_type) WHERE fraud_type IS NOT NULL;

-- CLAIM_LINE: Individual claim service lines
CREATE TABLE IF NOT EXISTS claims.claim_line (
    claim_line_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id                UUID NOT NULL REFERENCES claims.claim(claim_id),

    line_number             INTEGER NOT NULL,

    item_code               VARCHAR(20) NOT NULL,  -- MBS/ADA/extras code
    item_description        VARCHAR(500),
    clinical_category_id    INTEGER,
    benefit_category_id     INTEGER,

    service_date            DATE NOT NULL,
    quantity                INTEGER NOT NULL DEFAULT 1,

    charge_amount           DECIMAL(12,2) NOT NULL,
    schedule_fee            DECIMAL(12,2),
    benefit_amount          DECIMAL(12,2),
    gap_amount              DECIMAL(12,2),

    line_status             VARCHAR(20) NOT NULL DEFAULT 'Pending',
    rejection_reason_id     INTEGER,

    provider_id             INTEGER,
    provider_number         VARCHAR(20),

    tooth_number            VARCHAR(10),  -- For dental claims
    body_part               VARCHAR(50),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_cl_claim ON claims.claim_line(claim_id);
CREATE INDEX IF NOT EXISTS idx_cl_item ON claims.claim_line(item_code);

-- HOSPITAL_ADMISSION: Hospital admission claims
CREATE TABLE IF NOT EXISTS claims.hospital_admission (
    admission_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id                UUID NOT NULL REFERENCES claims.claim(claim_id),

    hospital_id             INTEGER NOT NULL,
    admission_number        VARCHAR(30),
    admission_date          DATE NOT NULL,
    discharge_date          DATE,

    admission_type          VARCHAR(20) NOT NULL,  -- Elective/Emergency/Maternity
    accommodation_type      VARCHAR(30) NOT NULL,  -- PrivateRoom/SharedRoom/DaySurgery/ICU

    drg_code                VARCHAR(10),
    clinical_category_id    INTEGER NOT NULL,
    principal_diagnosis     VARCHAR(10),  -- ICD-10 code
    principal_procedure     VARCHAR(10),  -- ACHI code

    length_of_stay          INTEGER,
    theatre_minutes         INTEGER,

    accommodation_charge    DECIMAL(12,2),
    theatre_charge          DECIMAL(12,2),
    prosthesis_charge       DECIMAL(12,2),
    other_charges           DECIMAL(12,2),

    accommodation_benefit   DECIMAL(12,2),
    theatre_benefit         DECIMAL(12,2),

    excess_applicable       BOOLEAN DEFAULT TRUE,
    excess_amount           DECIMAL(10,2) DEFAULT 0,
    co_payment_amount       DECIMAL(10,2) DEFAULT 0,

    contracted_hospital     BOOLEAN DEFAULT TRUE,
    informed_financial_consent BOOLEAN DEFAULT TRUE,

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_ha_claim ON claims.hospital_admission(claim_id);
CREATE INDEX IF NOT EXISTS idx_ha_hospital ON claims.hospital_admission(hospital_id);
CREATE INDEX IF NOT EXISTS idx_ha_admission_date ON claims.hospital_admission(admission_date);

-- EXTRAS_CLAIM: Extras/ancillary claims detail
CREATE TABLE IF NOT EXISTS claims.extras_claim (
    extras_claim_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id                UUID NOT NULL REFERENCES claims.claim(claim_id),
    claim_line_id           UUID NOT NULL REFERENCES claims.claim_line(claim_line_id),

    service_type            VARCHAR(50) NOT NULL,  -- Dental/Optical/Physio/Chiro/etc.
    dental_service_type     VARCHAR(20),  -- Sub-category for dental: Preventative/General/Major
    extras_item_id          INTEGER NOT NULL,

    provider_id             INTEGER NOT NULL,
    provider_location_id    INTEGER,

    service_date            DATE NOT NULL,
    tooth_number            VARCHAR(10),  -- For dental

    charge_amount           DECIMAL(10,2) NOT NULL,
    benefit_amount          DECIMAL(10,2),
    annual_limit_impact     DECIMAL(10,2),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,  -- Set during benefit capping at ASSESSED transition
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_ec_claim ON claims.extras_claim(claim_id);
CREATE INDEX IF NOT EXISTS idx_ec_service_type ON claims.extras_claim(service_type);

-- AMBULANCE_CLAIM: Ambulance claims
CREATE TABLE IF NOT EXISTS claims.ambulance_claim (
    ambulance_claim_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id                UUID NOT NULL REFERENCES claims.claim(claim_id),

    incident_date           DATE NOT NULL,
    incident_location       VARCHAR(200),
    incident_state          VARCHAR(3) NOT NULL,

    transport_type          VARCHAR(30) NOT NULL,  -- Emergency/Non-Emergency/Air
    pickup_location         VARCHAR(200),
    destination             VARCHAR(200),
    distance_km             DECIMAL(8,2),

    charge_amount           DECIMAL(10,2) NOT NULL,
    benefit_amount          DECIMAL(10,2),
    state_scheme_contribution DECIMAL(10,2),

    ambulance_provider      VARCHAR(100),
    case_number             VARCHAR(30),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_amb_claim ON claims.ambulance_claim(claim_id);
CREATE INDEX IF NOT EXISTS idx_amb_incident_date ON claims.ambulance_claim(incident_date);

-- PROSTHESIS_CLAIM: Prosthesis items in hospital claims
CREATE TABLE IF NOT EXISTS claims.prosthesis_claim (
    prosthesis_claim_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id                UUID NOT NULL REFERENCES claims.claim(claim_id),
    admission_id            UUID NOT NULL REFERENCES claims.hospital_admission(admission_id),

    prosthesis_item_id      INTEGER NOT NULL,
    billing_code            VARCHAR(20) NOT NULL,
    item_description        VARCHAR(200),

    quantity                INTEGER NOT NULL DEFAULT 1,

    charge_amount           DECIMAL(12,2) NOT NULL,
    benefit_amount          DECIMAL(12,2),
    gap_amount              DECIMAL(12,2),

    implant_date            DATE NOT NULL,

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_pros_claim ON claims.prosthesis_claim(claim_id);
CREATE INDEX IF NOT EXISTS idx_pros_admission ON claims.prosthesis_claim(admission_id);

-- MEDICAL_SERVICE: MBS items billed by doctors for hospital admissions
CREATE TABLE IF NOT EXISTS claims.medical_service (
    medical_service_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id                UUID NOT NULL REFERENCES claims.claim(claim_id),
    admission_id            UUID NOT NULL REFERENCES claims.hospital_admission(admission_id),

    -- MBS item details
    mbs_item_number         VARCHAR(10) NOT NULL,
    mbs_item_description    VARCHAR(500),
    mbs_schedule_fee        DECIMAL(12,2),

    -- Provider details
    provider_id             INTEGER NOT NULL,
    provider_type           VARCHAR(30) NOT NULL,  -- Surgeon/Anesthetist/Assistant/Physician
    provider_number         VARCHAR(20),

    -- Service details
    service_date            DATE NOT NULL,
    service_text            VARCHAR(200),

    -- Financial details
    charge_amount           DECIMAL(12,2) NOT NULL,
    medicare_benefit        DECIMAL(12,2),
    fund_benefit            DECIMAL(12,2),
    gap_amount              DECIMAL(12,2),

    -- Gap cover scheme
    no_gap_indicator        BOOLEAN DEFAULT FALSE,
    gap_cover_scheme        VARCHAR(50),

    -- Clinical details
    clinical_category_id    INTEGER,
    body_part               VARCHAR(50),
    procedure_laterality    VARCHAR(10),  -- Left/Right/Bilateral

    -- Multiple service rule
    multiple_service_rule_applied BOOLEAN DEFAULT FALSE,
    multiple_service_percentage INTEGER,

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_ms_claim ON claims.medical_service(claim_id);
CREATE INDEX IF NOT EXISTS idx_ms_admission ON claims.medical_service(admission_id);
CREATE INDEX IF NOT EXISTS idx_ms_mbs ON claims.medical_service(mbs_item_number);

-- CLAIM_ASSESSMENT: Claim assessment records
CREATE TABLE IF NOT EXISTS claims.claim_assessment (
    assessment_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id                UUID NOT NULL REFERENCES claims.claim(claim_id),

    assessment_type         VARCHAR(30) NOT NULL,  -- Auto/Manual/Review
    assessment_date         TIMESTAMP NOT NULL,
    assessed_by             VARCHAR(50) NOT NULL,

    original_benefit        DECIMAL(12,2),
    adjusted_benefit        DECIMAL(12,2),
    adjustment_reason       VARCHAR(500),

    waiting_period_check    BOOLEAN,
    benefit_limit_check     BOOLEAN,
    eligibility_check       BOOLEAN,

    outcome                 VARCHAR(20) NOT NULL,  -- Approved/Rejected/PartiallyApproved
    notes                   VARCHAR(1000),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_ca_claim ON claims.claim_assessment(claim_id);

-- BENEFIT_USAGE: Benefit utilization tracking
CREATE TABLE IF NOT EXISTS claims.benefit_usage (
    benefit_usage_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy.policy(policy_id),
    member_id               UUID NOT NULL REFERENCES policy.member(member_id),
    claim_id                UUID REFERENCES claims.claim(claim_id),

    benefit_category_id     INTEGER NOT NULL,
    benefit_year            VARCHAR(9) NOT NULL,  -- Australian financial year e.g., "2024-2025"

    usage_date              DATE NOT NULL,
    usage_amount            DECIMAL(12,2) NOT NULL,
    usage_count             INTEGER DEFAULT 1,

    annual_limit            DECIMAL(12,2),
    remaining_limit         DECIMAL(12,2),
    limit_type              VARCHAR(20),  -- Dollar/Service/Days

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_bu_policy ON claims.benefit_usage(policy_id);
CREATE INDEX IF NOT EXISTS idx_bu_member ON claims.benefit_usage(member_id);
CREATE INDEX IF NOT EXISTS idx_bu_category ON claims.benefit_usage(benefit_category_id, benefit_year);
