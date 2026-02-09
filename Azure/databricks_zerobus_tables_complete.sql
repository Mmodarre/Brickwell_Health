-- Databricks ZeroBus Ingest Tables - Complete Schema
-- Matches PostgreSQL schema exactly with ZeroBus-compatible types:
-- - DECIMAL → DOUBLE
-- - UUID → STRING
-- - VARCHAR → STRING
-- - DATE → INT (days since epoch - handled by app)
-- - TIMESTAMP → BIGINT (microseconds since epoch - handled by app)

USE CATALOG brickwell_health;
USE SCHEMA ingest_schema_bwh;

-- Drop existing tables
DROP TABLE IF EXISTS claim;
DROP TABLE IF EXISTS claim_line;
DROP TABLE IF EXISTS extras_claim;
DROP TABLE IF EXISTS hospital_admission;
DROP TABLE IF EXISTS prosthesis_claim;
DROP TABLE IF EXISTS medical_service;
DROP TABLE IF EXISTS ambulance_claim;
DROP TABLE IF EXISTS claim_assessment;
DROP TABLE IF EXISTS benefit_usage;

-- ============================================================================
-- 1. CLAIM TABLE
-- ============================================================================
CREATE TABLE claim (
    -- Primary/Foreign keys
    claim_id STRING ,
    claim_number STRING ,
    policy_id STRING ,
    member_id STRING ,
    coverage_id STRING ,

    -- Claim classification
    claim_type STRING ,
    claim_status STRING ,

    -- Dates (stored as INT - days since epoch)
    service_date INT ,
    lodgement_date INT ,
    assessment_date INT,
    payment_date INT,

    -- Provider references
    provider_id INT,
    hospital_id INT,

    -- Financial totals
    total_charge DOUBLE ,
    total_benefit DOUBLE,
    total_gap DOUBLE,

    -- Excess and co-payments
    excess_applied DOUBLE,
    co_payment_applied DOUBLE,

    -- Rejection details
    rejection_reason_id INT,
    rejection_notes STRING,

    -- Processing metadata
    claim_channel STRING ,
    pay_to STRING,

    -- Audit fields (TIMESTAMP as BIGINT - microseconds since epoch)
    created_at BIGINT ,
    created_by STRING ,
    modified_at BIGINT,
    modified_by STRING,

    -- Fraud metadata
    is_fraud BOOLEAN,
    fraud_type STRING,
    fraud_original_charge DOUBLE,
    fraud_inflation_amount DOUBLE,
    fraud_inflation_ratio DOUBLE,
    fraud_source_claim_id STRING,
    fraud_ring_id STRING,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- 2. CLAIM_LINE TABLE
-- ============================================================================
CREATE TABLE claim_line (
    -- Primary/Foreign keys
    claim_line_id STRING ,
    claim_id STRING ,

    -- Line details
    line_number INT ,

    -- Item details
    item_code STRING ,
    item_description STRING,
    clinical_category_id INT,
    benefit_category_id INT,

    -- Service details
    service_date INT ,
    quantity INT ,

    -- Financial
    charge_amount DOUBLE ,
    schedule_fee DOUBLE,
    benefit_amount DOUBLE,
    gap_amount DOUBLE,

    -- Status
    line_status STRING ,
    rejection_reason_id INT,

    -- Provider
    provider_id INT,
    provider_number STRING,

    -- Clinical details
    tooth_number STRING,
    body_part STRING,

    -- Audit
    created_at BIGINT ,
    created_by STRING ,
    modified_at BIGINT,
    modified_by STRING,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- 3. EXTRAS_CLAIM TABLE
-- ============================================================================
CREATE TABLE extras_claim (
    -- Primary/Foreign keys
    extras_claim_id STRING ,
    claim_id STRING ,
    claim_line_id STRING ,

    -- Service classification
    service_type STRING ,
    dental_service_type STRING,
    extras_item_id INT ,

    -- Provider
    provider_id INT ,
    provider_location_id INT,

    -- Service details
    service_date INT ,
    tooth_number STRING,

    -- Financial
    charge_amount DOUBLE ,
    benefit_amount DOUBLE,
    annual_limit_impact DOUBLE,

    -- Audit
    created_at BIGINT ,
    created_by STRING ,
    modified_at BIGINT,
    modified_by STRING,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- 4. HOSPITAL_ADMISSION TABLE
-- ============================================================================
CREATE TABLE hospital_admission (
    -- Primary/Foreign keys
    admission_id STRING ,
    claim_id STRING ,

    -- Hospital details
    hospital_id INT ,
    admission_number STRING,
    admission_date INT ,
    discharge_date INT,

    -- Admission classification
    admission_type STRING ,
    accommodation_type STRING ,

    -- Clinical coding
    drg_code STRING,
    clinical_category_id INT ,
    principal_diagnosis STRING,
    principal_procedure STRING,

    -- Service metrics
    length_of_stay INT,
    theatre_minutes INT,

    -- Financial breakdown
    accommodation_charge DOUBLE,
    theatre_charge DOUBLE,
    prosthesis_charge DOUBLE,
    other_charges DOUBLE,
    accommodation_benefit DOUBLE,
    theatre_benefit DOUBLE,

    -- Excess/co-payment
    excess_applicable BOOLEAN,
    excess_amount DOUBLE,
    co_payment_amount DOUBLE,

    -- Hospital contract
    contracted_hospital BOOLEAN,
    informed_financial_consent BOOLEAN,

    -- Audit
    created_at BIGINT ,
    created_by STRING ,
    modified_at BIGINT,
    modified_by STRING,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- 5. PROSTHESIS_CLAIM TABLE
-- ============================================================================
CREATE TABLE prosthesis_claim (
    -- Primary/Foreign keys
    prosthesis_claim_id STRING ,
    claim_id STRING ,
    admission_id STRING ,

    -- Prosthesis details
    prosthesis_item_id INT ,
    billing_code STRING ,
    item_description STRING,

    -- Quantity
    quantity INT ,

    -- Financial
    charge_amount DOUBLE ,
    benefit_amount DOUBLE,
    gap_amount DOUBLE,

    -- Service date
    implant_date INT ,

    -- Audit
    created_at BIGINT ,
    created_by STRING ,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- 6. MEDICAL_SERVICE TABLE
-- ============================================================================
CREATE TABLE medical_service (
    -- Primary/Foreign keys
    medical_service_id STRING ,
    claim_id STRING ,
    admission_id STRING ,

    -- MBS item details
    mbs_item_number STRING ,
    mbs_item_description STRING,
    mbs_schedule_fee DOUBLE,

    -- Provider details
    provider_id INT ,
    provider_type STRING ,
    provider_number STRING,

    -- Service details
    service_date INT ,
    service_text STRING,

    -- Financial
    charge_amount DOUBLE ,
    medicare_benefit DOUBLE,
    fund_benefit DOUBLE,
    gap_amount DOUBLE,

    -- Gap cover
    no_gap_indicator BOOLEAN,
    gap_cover_scheme STRING,

    -- Clinical
    clinical_category_id INT,
    body_part STRING,
    procedure_laterality STRING,

    -- Multiple service rule
    multiple_service_rule_applied BOOLEAN,
    multiple_service_percentage INT,

    -- Audit
    created_at BIGINT ,
    created_by STRING ,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- 7. AMBULANCE_CLAIM TABLE
-- ============================================================================
CREATE TABLE ambulance_claim (
    -- Primary/Foreign keys
    ambulance_claim_id STRING ,
    claim_id STRING ,

    -- Incident details
    incident_date INT ,
    incident_location STRING,
    incident_state STRING ,

    -- Transport details
    transport_type STRING ,
    pickup_location STRING,
    destination STRING,
    distance_km DOUBLE,

    -- Financial
    charge_amount DOUBLE ,
    benefit_amount DOUBLE,
    state_scheme_contribution DOUBLE,

    -- Provider
    ambulance_provider STRING,
    case_number STRING,

    -- Audit
    created_at BIGINT ,
    created_by STRING ,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- 8. CLAIM_ASSESSMENT TABLE
-- ============================================================================
CREATE TABLE claim_assessment (
    -- Primary/Foreign keys
    assessment_id STRING ,
    claim_id STRING ,

    -- Assessment details
    assessment_type STRING ,
    assessment_date BIGINT ,
    assessed_by STRING ,

    -- Benefit adjustments
    original_benefit DOUBLE,
    adjusted_benefit DOUBLE,
    adjustment_reason STRING,

    -- Validation checks
    waiting_period_check BOOLEAN,
    benefit_limit_check BOOLEAN,
    eligibility_check BOOLEAN,

    -- Outcome
    outcome STRING ,
    notes STRING,

    -- Audit
    created_at BIGINT ,
    created_by STRING ,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- 9. BENEFIT_USAGE TABLE
-- ============================================================================
CREATE TABLE benefit_usage (
    -- Primary/Foreign keys
    benefit_usage_id STRING ,
    policy_id STRING ,
    member_id STRING ,
    claim_id STRING,

    -- Benefit tracking
    benefit_category_id INT ,
    benefit_year STRING ,

    -- Usage details
    usage_date INT ,
    usage_amount DOUBLE ,
    usage_count INT,

    -- Limits
    annual_limit DOUBLE,
    remaining_limit DOUBLE,
    limit_type STRING,

    -- Audit
    created_at BIGINT ,
    created_by STRING ,

    -- Event metadata
    _event_id STRING ,
    _event_type STRING ,
    _event_timestamp BIGINT ,
    _event_worker_id INT 
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- GRANTS
-- ============================================================================
GRANT USE CATALOG ON CATALOG brickwell_health TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT USE SCHEMA ON SCHEMA brickwell_health.ingest_schema_bwh TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;

GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.claim TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.claim_line TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.extras_claim TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.hospital_admission TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.prosthesis_claim TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.medical_service TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.ambulance_claim TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.claim_assessment TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
GRANT SELECT, MODIFY ON TABLE brickwell_health.ingest_schema_bwh.benefit_usage TO `8e9ecd66-44a5-460c-9490-5871c5dc0d02`;
