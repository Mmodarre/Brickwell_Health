-- ============================================================================
-- Member Lifecycle Schema Migration
-- ============================================================================
-- This migration adds support for member lifecycle events:
-- 1. Adds marital_status column to member table
-- 2. Creates member_update table for tracking all member changes
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Add marital_status column to member table
-- ----------------------------------------------------------------------------
-- Default to 'Single' for existing records
ALTER TABLE policy.member
ADD COLUMN IF NOT EXISTS marital_status VARCHAR(20) DEFAULT 'Single';

-- Add check constraint for valid marital status values
-- (Optional - remove if your PostgreSQL version doesn't support it)
-- ALTER TABLE policy.member
-- ADD CONSTRAINT chk_marital_status
-- CHECK (marital_status IN ('Single', 'Married', 'DeFacto', 'Divorced', 'Separated', 'Widowed'));

-- Create index for queries filtering by marital status
CREATE INDEX IF NOT EXISTS idx_member_marital_status ON policy.member(marital_status);

-- ----------------------------------------------------------------------------
-- Create member_update table for tracking all member changes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS member_lifecycle.member_update (
    -- Primary key
    member_update_id UUID PRIMARY KEY,

    -- Reference to member
    member_id UUID NOT NULL REFERENCES policy.member(member_id),

    -- Change type (AddressChange, PhoneChange, EmailChange, NameChange,
    --              MaritalStatusChange, MedicareRenewal, PreferredNameUpdate, Death)
    change_type VARCHAR(50) NOT NULL,

    -- Date the change occurred
    change_date DATE NOT NULL,

    -- Previous and new values stored as JSON for flexibility
    -- Allows storing different fields depending on change type
    previous_values JSONB,
    new_values JSONB,

    -- Change context
    reason VARCHAR(200),
    triggered_by VARCHAR(50),  -- 'SIMULATION', 'POLICY_EVENT', etc.

    -- Audit fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

-- Index for finding changes by member
CREATE INDEX IF NOT EXISTS idx_member_update_member
ON member_lifecycle.member_update(member_id);

-- Index for finding changes by date (useful for reporting)
CREATE INDEX IF NOT EXISTS idx_member_update_date
ON member_lifecycle.member_update(change_date);

-- Index for finding changes by type (useful for analyzing specific change patterns)
CREATE INDEX IF NOT EXISTS idx_member_update_type
ON member_lifecycle.member_update(change_type);

-- Composite index for common query pattern: member + date range
CREATE INDEX IF NOT EXISTS idx_member_update_member_date
ON member_lifecycle.member_update(member_id, change_date);

-- ----------------------------------------------------------------------------
-- Comments for documentation
-- ----------------------------------------------------------------------------
COMMENT ON TABLE member_lifecycle.member_update IS
'Tracks all changes to member data for audit trail and downstream processing.
Each row represents a single change event (address, phone, email, name, marital status, medicare renewal, or death).';

COMMENT ON COLUMN member_lifecycle.member_update.change_type IS
'Type of change: AddressChange, PhoneChange, EmailChange, NameChange, MaritalStatusChange, MedicareRenewal, PreferredNameUpdate, Death';

COMMENT ON COLUMN member_lifecycle.member_update.previous_values IS
'JSON object containing the previous values of changed fields';

COMMENT ON COLUMN member_lifecycle.member_update.new_values IS
'JSON object containing the new values of changed fields';

COMMENT ON COLUMN member_lifecycle.member_update.triggered_by IS
'What triggered the change: SIMULATION (automated), POLICY_EVENT (cascaded from policy change), etc.';

COMMENT ON COLUMN policy.member.marital_status IS
'Member marital status: Single, Married, DeFacto, Divorced, Separated, Widowed';
