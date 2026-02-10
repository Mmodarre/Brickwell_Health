-- ============================================================================
-- REFERENCE DATA FOREIGN KEY CONSTRAINTS
-- Version: 1.0
-- Purpose: Add FK constraints from transactional tables to reference tables
-- ============================================================================
--
-- This file adds foreign key constraints AFTER reference data has been loaded.
-- Must be executed after:
--   1. schema_reference.sql (creates reference tables)
--   2. Reference data loader (loads JSON data into reference tables)
--   3. All transactional schema files (creates transactional tables)
--
-- ============================================================================

-- ============================================================================
-- POLICY DOMAIN FOREIGN KEYS
-- ============================================================================

-- policy.product_id -> product.product_id
ALTER TABLE policy.policy
    ADD CONSTRAINT fk_policy_product
    FOREIGN KEY (product_id) REFERENCES reference.product(product_id);

-- coverage.product_id -> product.product_id
ALTER TABLE policy.coverage
    ADD CONSTRAINT fk_coverage_product
    FOREIGN KEY (product_id) REFERENCES reference.product(product_id);

-- application.product_id -> product.product_id
ALTER TABLE policy.application
    ADD CONSTRAINT fk_application_product
    FOREIGN KEY (product_id) REFERENCES reference.product(product_id);

-- waiting_period.benefit_category_id -> benefit_category.benefit_category_id
-- NULL allowed - some waiting periods apply to all benefits
ALTER TABLE policy.waiting_period
    ADD CONSTRAINT fk_waiting_period_benefit_category
    FOREIGN KEY (benefit_category_id) REFERENCES reference.benefit_category(benefit_category_id);

-- waiting_period.clinical_category_id -> clinical_category.clinical_category_id
-- NULL allowed - some waiting periods apply to all clinical categories
ALTER TABLE policy.waiting_period
    ADD CONSTRAINT fk_waiting_period_clinical_category
    FOREIGN KEY (clinical_category_id) REFERENCES reference.clinical_category(clinical_category_id);

-- ============================================================================
-- REGULATORY DOMAIN FOREIGN KEYS
-- ============================================================================

-- upgrade_request.current_product_id -> product.product_id
ALTER TABLE regulatory.upgrade_request
    ADD CONSTRAINT fk_upgrade_request_current_product
    FOREIGN KEY (current_product_id) REFERENCES reference.product(product_id);

-- upgrade_request.requested_product_id -> product.product_id
ALTER TABLE regulatory.upgrade_request
    ADD CONSTRAINT fk_upgrade_request_requested_product
    FOREIGN KEY (requested_product_id) REFERENCES reference.product(product_id);

-- ============================================================================
-- CLAIMS DOMAIN FOREIGN KEYS
-- ============================================================================

-- claim.provider_id -> provider.provider_id
-- NULL allowed - not all claims have provider (e.g., ambulance)
ALTER TABLE claims.claim
    ADD CONSTRAINT fk_claim_provider
    FOREIGN KEY (provider_id) REFERENCES reference.provider(provider_id);

-- claim.hospital_id -> hospital.hospital_id
-- NULL allowed - only hospital claims have hospital_id
ALTER TABLE claims.claim
    ADD CONSTRAINT fk_claim_hospital
    FOREIGN KEY (hospital_id) REFERENCES reference.hospital(hospital_id);

-- claim.rejection_reason_id -> claim_rejection_reason.rejection_reason_id
-- NULL allowed - only rejected claims have rejection reason
ALTER TABLE claims.claim
    ADD CONSTRAINT fk_claim_rejection_reason
    FOREIGN KEY (rejection_reason_id) REFERENCES reference.claim_rejection_reason(rejection_reason_id);

-- claim_line.clinical_category_id -> clinical_category.clinical_category_id
-- NULL allowed - only hospital services have clinical category
ALTER TABLE claims.claim_line
    ADD CONSTRAINT fk_claim_line_clinical_category
    FOREIGN KEY (clinical_category_id) REFERENCES reference.clinical_category(clinical_category_id);

-- claim_line.benefit_category_id -> benefit_category.benefit_category_id
-- NULL allowed - some services may not have benefit category
ALTER TABLE claims.claim_line
    ADD CONSTRAINT fk_claim_line_benefit_category
    FOREIGN KEY (benefit_category_id) REFERENCES reference.benefit_category(benefit_category_id);

-- claim_line.provider_id -> provider.provider_id
-- NULL allowed - not all claim lines have provider
ALTER TABLE claims.claim_line
    ADD CONSTRAINT fk_claim_line_provider
    FOREIGN KEY (provider_id) REFERENCES reference.provider(provider_id);

-- claim_line.rejection_reason_id -> claim_rejection_reason.rejection_reason_id
-- NULL allowed - only rejected claim lines have rejection reason
ALTER TABLE claims.claim_line
    ADD CONSTRAINT fk_claim_line_rejection_reason
    FOREIGN KEY (rejection_reason_id) REFERENCES reference.claim_rejection_reason(rejection_reason_id);

-- hospital_admission.hospital_id -> hospital.hospital_id
ALTER TABLE claims.hospital_admission
    ADD CONSTRAINT fk_hospital_admission_hospital
    FOREIGN KEY (hospital_id) REFERENCES reference.hospital(hospital_id);

-- hospital_admission.clinical_category_id -> clinical_category.clinical_category_id
ALTER TABLE claims.hospital_admission
    ADD CONSTRAINT fk_hospital_admission_clinical_category
    FOREIGN KEY (clinical_category_id) REFERENCES reference.clinical_category(clinical_category_id);

-- extras_claim.extras_item_id -> extras_item_code.extras_item_id
ALTER TABLE claims.extras_claim
    ADD CONSTRAINT fk_extras_claim_item
    FOREIGN KEY (extras_item_id) REFERENCES reference.extras_item_code(extras_item_id);

-- extras_claim.provider_id -> provider.provider_id
ALTER TABLE claims.extras_claim
    ADD CONSTRAINT fk_extras_claim_provider
    FOREIGN KEY (provider_id) REFERENCES reference.provider(provider_id);

-- prosthesis_claim.prosthesis_item_id -> prosthesis_list_item.prosthesis_item_id
ALTER TABLE claims.prosthesis_claim
    ADD CONSTRAINT fk_prosthesis_claim_item
    FOREIGN KEY (prosthesis_item_id) REFERENCES reference.prosthesis_list_item(prosthesis_item_id);

-- medical_service.provider_id -> provider.provider_id
ALTER TABLE claims.medical_service
    ADD CONSTRAINT fk_medical_service_provider
    FOREIGN KEY (provider_id) REFERENCES reference.provider(provider_id);

-- medical_service.clinical_category_id -> clinical_category.clinical_category_id
-- NULL allowed
ALTER TABLE claims.medical_service
    ADD CONSTRAINT fk_medical_service_clinical_category
    FOREIGN KEY (clinical_category_id) REFERENCES reference.clinical_category(clinical_category_id);

-- Note: medical_service.mbs_item_number -> mbs_item.item_code
-- Cannot add FK constraint because column names differ and both are VARCHAR
-- Will be enforced at application level via ReferenceDataLoader

-- benefit_usage.benefit_category_id -> benefit_category.benefit_category_id
ALTER TABLE claims.benefit_usage
    ADD CONSTRAINT fk_benefit_usage_category
    FOREIGN KEY (benefit_category_id) REFERENCES reference.benefit_category(benefit_category_id);

-- ============================================================================
-- CRM DOMAIN FOREIGN KEYS
-- ============================================================================

-- interaction.interaction_type_id -> interaction_type.interaction_type_id
ALTER TABLE crm.interaction
    ADD CONSTRAINT fk_interaction_type
    FOREIGN KEY (interaction_type_id) REFERENCES reference.interaction_type(interaction_type_id);

-- interaction.outcome_id -> interaction_outcome.outcome_id
-- NULL allowed - outcome may be recorded later
ALTER TABLE crm.interaction
    ADD CONSTRAINT fk_interaction_outcome
    FOREIGN KEY (outcome_id) REFERENCES reference.interaction_outcome(outcome_id);

-- service_case.case_type_id -> case_type.case_type_id
ALTER TABLE crm.service_case
    ADD CONSTRAINT fk_service_case_type
    FOREIGN KEY (case_type_id) REFERENCES reference.case_type(case_type_id);

-- complaint.complaint_category_id -> complaint_category.complaint_category_id
ALTER TABLE crm.complaint
    ADD CONSTRAINT fk_complaint_category
    FOREIGN KEY (complaint_category_id) REFERENCES reference.complaint_category(complaint_category_id);

-- ============================================================================
-- COMMUNICATION DOMAIN FOREIGN KEYS
-- ============================================================================

-- communication.template_code -> communication_template.template_code
-- NULL allowed - not all communications use templates
ALTER TABLE communication.communication
    ADD CONSTRAINT fk_communication_template
    FOREIGN KEY (template_code) REFERENCES reference.communication_template(template_code);

-- ============================================================================
-- NOTES ON EXCLUDED FOREIGN KEYS
-- ============================================================================
--
-- The following potential FKs are NOT added due to data format mismatches:
--
-- 1. claim_line.item_code -> extras_item_code.item_code
--    - claim_line.item_code is mixed (MBS/ADA/extras codes)
--    - Only extras claims should reference extras_item_code
--    - Cannot add FK constraint without claim_type context
--    - Enforced at application level
--
-- 2. medical_service.mbs_item_number -> mbs_item.item_code
--    - Both are VARCHAR but different column names
--    - MBS items in JSON use codes like "30026" but may have different format
--    - Enforced at application level via ReferenceDataLoader
--
-- 3. member/policy state columns
--    - No direct state_territory_id columns in member/policy tables
--    - State is stored as VARCHAR(3) code (NSW, VIC, etc.)
--    - Could add FK but requires schema change to use state_territory_id
--    - Deferred to future enhancement
--
-- ============================================================================
