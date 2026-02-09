-- =============================================================================
-- SYSTEM / PLACEHOLDER RECORDS
-- =============================================================================
-- These placeholder records allow tracking claim attempts by members without coverage
-- while maintaining referential integrity. Used when uncovered_claim_attempt_rate > 0.

-- Placeholder product (required for FK constraints on placeholder policy/coverage)
INSERT INTO product (
    product_id,
    product_code,
    product_name,
    product_type_id,
    is_hospital,
    is_extras,
    is_ambulance,
    status,
    created_by
) VALUES (
    0,
    'SYSTEM-PLACEHOLDER',
    'System Placeholder Product',
    0,
    FALSE,
    FALSE,
    FALSE,
    'System',
    'SYSTEM'
) ON CONFLICT (product_id) DO NOTHING;

-- Placeholder policy (system record, not a real policy)
INSERT INTO policy (
    policy_id,
    policy_number,
    product_id,
    policy_status,
    policy_type,
    effective_date,
    payment_frequency,
    premium_amount,
    distribution_channel,
    state_of_residence,
    original_join_date,
    created_by
) VALUES (
    '00000000-0000-0000-0000-000000000000',
    'SYSTEM-NO-COVERAGE',
    0,
    'System',
    'System',
    '1900-01-01',
    'None',
    0,
    'System',
    'N/A',
    '1900-01-01',
    'SYSTEM'
) ON CONFLICT (policy_id) DO NOTHING;

-- Placeholder coverage for rejected claims
INSERT INTO coverage (
    coverage_id,
    policy_id,
    coverage_type,
    product_id,
    effective_date,
    status,
    created_by
) VALUES (
    '00000000-0000-0000-0000-000000000000',
    '00000000-0000-0000-0000-000000000000',
    'None',
    0,
    '1900-01-01',
    'Placeholder',
    'SYSTEM'
) ON CONFLICT (coverage_id) DO NOTHING;
