-- ============================================================================
-- DIGITAL BEHAVIOR DOMAIN SCHEMA
-- Version: 1.0
-- Purpose: Web sessions and digital events for behavioral analytics
-- ============================================================================

-- ============================================================================
-- WEB_SESSION TABLE
-- Tracks website/app sessions with engagement metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS digital.web_session (
    -- Primary Key
    session_id UUID PRIMARY KEY,

    -- Relationships (member_id required for authenticated sessions)
    member_id UUID NOT NULL,
    policy_id UUID,

    -- Session Details
    session_start TIMESTAMP NOT NULL,
    session_end TIMESTAMP,
    duration_seconds INT,

    -- Engagement Metrics
    page_count INT DEFAULT 0,
    event_count INT DEFAULT 0,

    -- Device & Browser
    device_type VARCHAR(20),
    browser VARCHAR(50),
    operating_system VARCHAR(50),

    -- Entry/Exit
    entry_page VARCHAR(200),
    exit_page VARCHAR(200),
    referrer VARCHAR(200),

    -- Authentication
    is_authenticated BOOLEAN DEFAULT TRUE,

    -- Session Type
    session_type VARCHAR(20),

    -- Intent Signals (denormalized for analytics)
    viewed_cancel_page BOOLEAN DEFAULT FALSE,
    viewed_upgrade_page BOOLEAN DEFAULT FALSE,
    viewed_claims_page BOOLEAN DEFAULT FALSE,
    viewed_billing_page BOOLEAN DEFAULT FALSE,
    viewed_compare_page BOOLEAN DEFAULT FALSE,

    -- Trigger Context
    trigger_event_type VARCHAR(50),
    trigger_event_id UUID,

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Keys
    CONSTRAINT fk_session_member FOREIGN KEY (member_id)
        REFERENCES policy.member(member_id),
    CONSTRAINT fk_session_policy FOREIGN KEY (policy_id)
        REFERENCES policy.policy(policy_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_session_member ON digital.web_session(member_id);
CREATE INDEX IF NOT EXISTS idx_session_policy ON digital.web_session(policy_id)
    WHERE policy_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_session_start ON digital.web_session(session_start);
CREATE INDEX IF NOT EXISTS idx_session_cancel ON digital.web_session(viewed_cancel_page)
    WHERE viewed_cancel_page = TRUE;
CREATE INDEX IF NOT EXISTS idx_session_upgrade ON digital.web_session(viewed_upgrade_page)
    WHERE viewed_upgrade_page = TRUE;
CREATE INDEX IF NOT EXISTS idx_session_device ON digital.web_session(device_type);
CREATE INDEX IF NOT EXISTS idx_session_type ON digital.web_session(session_type);

-- ============================================================================
-- DIGITAL_EVENT TABLE
-- Captures page-level events for granular behavioral analysis
-- ============================================================================

CREATE TABLE IF NOT EXISTS digital.digital_event (
    -- Primary Key
    event_id UUID PRIMARY KEY,

    -- Session Context
    session_id UUID NOT NULL,

    -- Member (denormalized for performance)
    member_id UUID NOT NULL,

    -- Event Details
    event_timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,

    -- Page Context
    page_path VARCHAR(200),
    page_category VARCHAR(50),
    page_title VARCHAR(200),

    -- Element Details (for clicks)
    element_id VARCHAR(100),
    element_text VARCHAR(200),

    -- Search Details
    search_query VARCHAR(200),
    search_results_count INT,

    -- Form Details
    form_name VARCHAR(100),
    form_field VARCHAR(100),
    form_completed BOOLEAN,

    -- Event Sequence
    event_sequence INT,

    -- Timing
    time_on_page_seconds INT,

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Keys
    CONSTRAINT fk_event_session FOREIGN KEY (session_id)
        REFERENCES digital.web_session(session_id),
    CONSTRAINT fk_event_member FOREIGN KEY (member_id)
        REFERENCES policy.member(member_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_digital_event_session ON digital.digital_event(session_id);
CREATE INDEX IF NOT EXISTS idx_digital_event_member ON digital.digital_event(member_id);
CREATE INDEX IF NOT EXISTS idx_digital_event_timestamp ON digital.digital_event(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_digital_event_category ON digital.digital_event(page_category);
CREATE INDEX IF NOT EXISTS idx_digital_event_type ON digital.digital_event(event_type);
CREATE INDEX IF NOT EXISTS idx_digital_event_path ON digital.digital_event(page_path);
