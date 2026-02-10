-- ============================================================================
-- Schema Initialization
-- ============================================================================
-- Creates all database schemas for the Brickwell Health Simulator
--
-- This file must be executed FIRST before any table creation scripts
-- Schemas are created in dependency order for clarity
-- ============================================================================

-- Create schemas in dependency order
CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS policy;
CREATE SCHEMA IF NOT EXISTS regulatory;
CREATE SCHEMA IF NOT EXISTS claims;
CREATE SCHEMA IF NOT EXISTS billing;
CREATE SCHEMA IF NOT EXISTS member_lifecycle;
CREATE SCHEMA IF NOT EXISTS crm;
CREATE SCHEMA IF NOT EXISTS communication;
CREATE SCHEMA IF NOT EXISTS digital;
CREATE SCHEMA IF NOT EXISTS survey;
CREATE SCHEMA IF NOT EXISTS nba;

-- Set role-level search path (fallback for ad-hoc queries)
-- This allows unqualified table references in debugging queries
-- Production code should always use explicit schema qualification
-- Uses ALTER ROLE instead of ALTER DATABASE to avoid needing database ownership
ALTER ROLE brickwell
SET search_path TO policy, reference, regulatory, claims, billing, member_lifecycle,
                  crm, communication, digital, survey, nba, public;

-- Schema documentation
COMMENT ON SCHEMA reference IS 'Master data: products, providers, benefits, reference codes';
COMMENT ON SCHEMA policy IS 'Core entities: member, policy, application, coverage, waiting periods';
COMMENT ON SCHEMA regulatory IS 'Compliance: LHC loading, PHI rebates, age discounts, suspensions';
COMMENT ON SCHEMA claims IS 'Claims processing: hospital, extras, ambulance claims with fraud detection';
COMMENT ON SCHEMA billing IS 'Financial transactions: invoices, payments, direct debit, arrears, refunds';
COMMENT ON SCHEMA member_lifecycle IS 'Member demographic changes and audit trail';
COMMENT ON SCHEMA crm IS 'Customer relationship: interactions, cases, complaints, SLA tracking';
COMMENT ON SCHEMA communication IS 'Marketing and messaging: campaigns, communications, preferences';
COMMENT ON SCHEMA digital IS 'Digital behavior: web sessions, digital events, intent signals';
COMMENT ON SCHEMA survey IS 'Feedback: NPS/CSAT surveys with LLM context for post-processing';
COMMENT ON SCHEMA nba IS 'Next Best Action: catalog, recommendations, executions with behavioral effects';
