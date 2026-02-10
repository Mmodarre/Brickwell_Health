-- ============================================================================
-- COMMUNICATION DOMAIN SCHEMA
-- Version: 1.0
-- Purpose: Outbound communications, preferences, campaigns, and responses
-- ============================================================================

-- ============================================================================
-- COMMUNICATION_PREFERENCE TABLE
-- Tracks member opt-in/opt-out preferences by channel and type
-- ============================================================================

CREATE TABLE IF NOT EXISTS communication.communication_preference (
    -- Primary Key
    preference_id UUID PRIMARY KEY,

    -- Relationships
    member_id UUID NOT NULL,
    policy_id UUID NOT NULL,

    -- Preference Details
    preference_type VARCHAR(30) NOT NULL,
    channel VARCHAR(20) NOT NULL,

    -- Consent Status
    is_opted_in BOOLEAN NOT NULL DEFAULT TRUE,
    opt_in_date DATE,
    opt_out_date DATE,

    -- Preferences
    preferred_time VARCHAR(20),

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at TIMESTAMP,
    modified_by VARCHAR(50),

    -- Foreign Keys
    CONSTRAINT fk_comm_pref_member FOREIGN KEY (member_id)
        REFERENCES policy.member(member_id),
    CONSTRAINT fk_comm_pref_policy FOREIGN KEY (policy_id)
        REFERENCES policy.policy(policy_id),

    -- Unique constraint per member/type/channel
    CONSTRAINT uq_comm_pref_member_type_channel
        UNIQUE (member_id, preference_type, channel)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_comm_pref_member ON communication.communication_preference(member_id);
CREATE INDEX IF NOT EXISTS idx_comm_pref_policy ON communication.communication_preference(policy_id);
CREATE INDEX IF NOT EXISTS idx_comm_pref_opted_out ON communication.communication_preference(is_opted_in)
    WHERE is_opted_in = FALSE;

-- ============================================================================
-- CAMPAIGN TABLE
-- Marketing campaign definitions
-- ============================================================================

CREATE TABLE IF NOT EXISTS communication.campaign (
    -- Primary Key
    campaign_id UUID PRIMARY KEY,
    campaign_code VARCHAR(30) NOT NULL UNIQUE,

    -- Details
    campaign_name VARCHAR(100) NOT NULL,
    campaign_type VARCHAR(30) NOT NULL,
    description VARCHAR(1000),

    -- Timeline
    start_date DATE NOT NULL,
    end_date DATE,

    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'Active',

    -- Targeting
    target_audience VARCHAR(500),
    target_segment VARCHAR(100),

    -- Budget
    budget DECIMAL(12,2),
    actual_spend DECIMAL(12,2),

    -- Performance Metrics
    target_response_rate DECIMAL(5,4),
    actual_response_rate DECIMAL(5,4),
    target_conversion_rate DECIMAL(5,4),
    actual_conversion_rate DECIMAL(5,4),

    -- Counts
    members_targeted INT DEFAULT 0,
    communications_sent INT DEFAULT 0,
    responses_received INT DEFAULT 0,
    conversions INT DEFAULT 0,

    -- Owner
    owner VARCHAR(50),

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at TIMESTAMP,
    modified_by VARCHAR(50)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_campaign_status ON communication.campaign(status);
CREATE INDEX IF NOT EXISTS idx_campaign_type ON communication.campaign(campaign_type);
CREATE INDEX IF NOT EXISTS idx_campaign_dates ON communication.campaign(start_date, end_date);

-- ============================================================================
-- COMMUNICATION TABLE
-- All outbound messages (transactional and marketing)
-- ============================================================================

CREATE TABLE IF NOT EXISTS communication.communication (
    -- Primary Key
    communication_id UUID PRIMARY KEY,
    communication_reference VARCHAR(30) NOT NULL UNIQUE,

    -- Relationships
    policy_id UUID NOT NULL,
    member_id UUID NOT NULL,
    campaign_id UUID,

    -- Type & Channel
    communication_type VARCHAR(30) NOT NULL,
    template_code VARCHAR(50),

    -- Content
    subject VARCHAR(200),

    -- Recipient
    recipient_email VARCHAR(200),
    recipient_phone VARCHAR(20),

    -- Timing
    scheduled_date TIMESTAMP,
    sent_date TIMESTAMP,

    -- Status Tracking
    delivery_status VARCHAR(20) NOT NULL DEFAULT 'Pending',
    delivery_status_date TIMESTAMP,

    -- Engagement
    opened_date TIMESTAMP,
    clicked_date TIMESTAMP,

    -- Trigger Context
    trigger_event_type VARCHAR(50),
    trigger_event_id UUID,

    -- Related Entities
    claim_id UUID,
    invoice_id UUID,
    interaction_id UUID,

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',

    -- Foreign Keys
    CONSTRAINT fk_communication_policy FOREIGN KEY (policy_id)
        REFERENCES policy.policy(policy_id),
    CONSTRAINT fk_communication_member FOREIGN KEY (member_id)
        REFERENCES policy.member(member_id),
    CONSTRAINT fk_communication_campaign FOREIGN KEY (campaign_id)
        REFERENCES communication.campaign(campaign_id),
    CONSTRAINT fk_communication_claim FOREIGN KEY (claim_id)
        REFERENCES claims.claim(claim_id),
    CONSTRAINT fk_communication_invoice FOREIGN KEY (invoice_id)
        REFERENCES billing.invoice(invoice_id),
    CONSTRAINT fk_communication_interaction FOREIGN KEY (interaction_id)
        REFERENCES crm.interaction(interaction_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_communication_member ON communication.communication(member_id);
CREATE INDEX IF NOT EXISTS idx_communication_policy ON communication.communication(policy_id);
CREATE INDEX IF NOT EXISTS idx_communication_campaign ON communication.communication(campaign_id)
    WHERE campaign_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_communication_sent ON communication.communication(sent_date);
CREATE INDEX IF NOT EXISTS idx_communication_trigger ON communication.communication(trigger_event_type);
CREATE INDEX IF NOT EXISTS idx_communication_status ON communication.communication(delivery_status);
CREATE INDEX IF NOT EXISTS idx_communication_type ON communication.communication(communication_type);

-- ============================================================================
-- CAMPAIGN_RESPONSE TABLE
-- Member responses to campaigns (open, click, convert)
-- ============================================================================

CREATE TABLE IF NOT EXISTS communication.campaign_response (
    -- Primary Key
    response_id UUID PRIMARY KEY,

    -- Relationships
    campaign_id UUID NOT NULL,
    member_id UUID NOT NULL,
    policy_id UUID NOT NULL,
    communication_id UUID,

    -- Response Details
    response_type VARCHAR(30) NOT NULL,
    response_date TIMESTAMP NOT NULL,

    -- Conversion Details
    conversion_type VARCHAR(30),
    conversion_value DECIMAL(10,2),

    -- Channel
    response_channel VARCHAR(20),

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',

    -- Foreign Keys
    CONSTRAINT fk_campaign_resp_campaign FOREIGN KEY (campaign_id)
        REFERENCES communication.campaign(campaign_id),
    CONSTRAINT fk_campaign_resp_member FOREIGN KEY (member_id)
        REFERENCES policy.member(member_id),
    CONSTRAINT fk_campaign_resp_policy FOREIGN KEY (policy_id)
        REFERENCES policy.policy(policy_id),
    CONSTRAINT fk_campaign_resp_communication FOREIGN KEY (communication_id)
        REFERENCES communication.communication(communication_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_campaign_resp_campaign ON communication.campaign_response(campaign_id);
CREATE INDEX IF NOT EXISTS idx_campaign_resp_member ON communication.campaign_response(member_id);
CREATE INDEX IF NOT EXISTS idx_campaign_resp_date ON communication.campaign_response(response_date);
CREATE INDEX IF NOT EXISTS idx_campaign_resp_type ON communication.campaign_response(response_type);
CREATE INDEX IF NOT EXISTS idx_campaign_resp_conversion ON communication.campaign_response(conversion_type)
    WHERE conversion_type IS NOT NULL;
