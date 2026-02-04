-- ============================================================================
-- SURVEY DOMAIN SCHEMA
-- Version: 1.0
-- Purpose: NPS and CSAT surveys with deferred LLM processing
-- ============================================================================

-- ============================================================================
-- NPS_SURVEY_PENDING TABLE
-- Stores pending NPS surveys for deferred LLM processing
-- ============================================================================

CREATE TABLE IF NOT EXISTS nps_survey_pending (
    -- Primary Key
    pending_id UUID PRIMARY KEY,
    survey_reference VARCHAR(30) NOT NULL,
    
    -- Relationships
    member_id UUID NOT NULL,
    policy_id UUID NOT NULL,
    
    -- Survey Context
    survey_type VARCHAR(30) NOT NULL,
    trigger_event VARCHAR(50),
    trigger_entity_id UUID,
    claim_id UUID,
    interaction_id UUID,
    
    -- Timing
    simulation_date DATE NOT NULL,
    sent_datetime TIMESTAMP NOT NULL,
    
    -- Response Prediction (pre-calculated during simulation)
    will_respond BOOLEAN NOT NULL,
    response_probability DECIMAL(5,4),
    completed_datetime TIMESTAMP,
    response_time_minutes INT,
    
    -- LLM Context (JSON)
    llm_context JSONB NOT NULL,
    
    -- Prior Survey Context (populated during processing)
    prior_surveys_context JSONB,
    
    -- Processing Status
    processing_status VARCHAR(20) DEFAULT 'pending',
    processing_order INT,
    processed_at TIMESTAMP,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    
    -- Final Survey ID (after processing)
    final_survey_id UUID,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    CONSTRAINT fk_nps_pending_member FOREIGN KEY (member_id) 
        REFERENCES member(member_id),
    CONSTRAINT fk_nps_pending_policy FOREIGN KEY (policy_id) 
        REFERENCES policy(policy_id),
    CONSTRAINT fk_nps_pending_claim FOREIGN KEY (claim_id) 
        REFERENCES claim(claim_id),
    CONSTRAINT fk_nps_pending_interaction FOREIGN KEY (interaction_id) 
        REFERENCES interaction(interaction_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_nps_pending_status ON nps_survey_pending(processing_status);
CREATE INDEX IF NOT EXISTS idx_nps_pending_member_date ON nps_survey_pending(member_id, simulation_date);
CREATE INDEX IF NOT EXISTS idx_nps_pending_will_respond ON nps_survey_pending(will_respond) 
    WHERE will_respond = TRUE;
CREATE INDEX IF NOT EXISTS idx_nps_pending_trigger ON nps_survey_pending(trigger_event);

-- ============================================================================
-- NPS_SURVEY TABLE
-- Final NPS survey responses (populated after LLM processing)
-- ============================================================================

CREATE TABLE IF NOT EXISTS nps_survey (
    -- Primary Key
    survey_id UUID PRIMARY KEY,
    survey_reference VARCHAR(30) NOT NULL UNIQUE,
    
    -- Relationships
    member_id UUID NOT NULL,
    policy_id UUID NOT NULL,
    
    -- Survey Context
    survey_type VARCHAR(30) NOT NULL,
    trigger_event VARCHAR(50),
    trigger_entity_id UUID,
    claim_id UUID,
    interaction_id UUID,
    
    -- Survey Lifecycle
    sent_date TIMESTAMP NOT NULL,
    completed_date TIMESTAMP,
    
    -- Q1: Core NPS Score (0-10)
    nps_score INT CHECK (nps_score BETWEEN 0 AND 10),
    nps_category VARCHAR(20),
    
    -- Q2: Verbatim Feedback
    feedback_text VARCHAR(2000),
    feedback_improvement VARCHAR(1000),
    
    -- Q3-7: Driver Scores (0-10)
    driver_claims_processing INT CHECK (driver_claims_processing BETWEEN 0 AND 10),
    driver_customer_service INT CHECK (driver_customer_service BETWEEN 0 AND 10),
    driver_value_for_money INT CHECK (driver_value_for_money BETWEEN 0 AND 10),
    driver_coverage_clarity INT CHECK (driver_coverage_clarity BETWEEN 0 AND 10),
    driver_digital_experience INT CHECK (driver_digital_experience BETWEEN 0 AND 10),
    
    -- Sentiment Analysis
    sentiment_score DECIMAL(3,2),
    sentiment_label VARCHAR(20),
    feedback_themes VARCHAR(500),
    
    -- Survey Metadata
    survey_channel VARCHAR(20),
    response_time_minutes INT,
    follow_up_consent BOOLEAN,
    
    -- Processing Info
    pending_id UUID,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    
    -- Foreign Keys
    CONSTRAINT fk_nps_member FOREIGN KEY (member_id) 
        REFERENCES member(member_id),
    CONSTRAINT fk_nps_policy FOREIGN KEY (policy_id) 
        REFERENCES policy(policy_id),
    CONSTRAINT fk_nps_claim FOREIGN KEY (claim_id) 
        REFERENCES claim(claim_id),
    CONSTRAINT fk_nps_interaction FOREIGN KEY (interaction_id) 
        REFERENCES interaction(interaction_id),
    CONSTRAINT fk_nps_pending FOREIGN KEY (pending_id) 
        REFERENCES nps_survey_pending(pending_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_nps_member ON nps_survey(member_id);
CREATE INDEX IF NOT EXISTS idx_nps_policy ON nps_survey(policy_id);
CREATE INDEX IF NOT EXISTS idx_nps_sent ON nps_survey(sent_date);
CREATE INDEX IF NOT EXISTS idx_nps_score ON nps_survey(nps_score) WHERE nps_score IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nps_category ON nps_survey(nps_category) WHERE nps_category IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nps_claim ON nps_survey(claim_id) WHERE claim_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nps_detractors ON nps_survey(nps_category) WHERE nps_category = 'Detractor';
CREATE INDEX IF NOT EXISTS idx_nps_trigger ON nps_survey(trigger_event);

-- ============================================================================
-- CSAT_SURVEY_PENDING TABLE
-- Stores pending CSAT surveys for deferred LLM processing
-- ============================================================================

CREATE TABLE IF NOT EXISTS csat_survey_pending (
    -- Primary Key
    pending_id UUID PRIMARY KEY,
    survey_reference VARCHAR(30) NOT NULL,
    
    -- Relationships
    member_id UUID NOT NULL,
    policy_id UUID NOT NULL,
    
    -- Survey Context
    survey_type VARCHAR(30) NOT NULL,
    interaction_id UUID,
    case_id UUID,
    
    -- Timing
    simulation_date DATE NOT NULL,
    sent_datetime TIMESTAMP NOT NULL,
    
    -- Response Prediction (pre-calculated during simulation)
    will_respond BOOLEAN NOT NULL,
    response_probability DECIMAL(5,4),
    completed_datetime TIMESTAMP,
    response_time_minutes INT,
    
    -- LLM Context (JSON)
    llm_context JSONB NOT NULL,
    
    -- Processing Status
    processing_status VARCHAR(20) DEFAULT 'pending',
    processing_order INT,
    processed_at TIMESTAMP,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    
    -- Final Survey ID
    final_survey_id UUID,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    CONSTRAINT fk_csat_pending_member FOREIGN KEY (member_id) 
        REFERENCES member(member_id),
    CONSTRAINT fk_csat_pending_policy FOREIGN KEY (policy_id) 
        REFERENCES policy(policy_id),
    CONSTRAINT fk_csat_pending_interaction FOREIGN KEY (interaction_id) 
        REFERENCES interaction(interaction_id),
    CONSTRAINT fk_csat_pending_case FOREIGN KEY (case_id) 
        REFERENCES service_case(case_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_csat_pending_status ON csat_survey_pending(processing_status);
CREATE INDEX IF NOT EXISTS idx_csat_pending_member_date ON csat_survey_pending(member_id, simulation_date);
CREATE INDEX IF NOT EXISTS idx_csat_pending_will_respond ON csat_survey_pending(will_respond) 
    WHERE will_respond = TRUE;

-- ============================================================================
-- CSAT_SURVEY TABLE
-- Final CSAT survey responses
-- ============================================================================

CREATE TABLE IF NOT EXISTS csat_survey (
    -- Primary Key
    survey_id UUID PRIMARY KEY,
    survey_reference VARCHAR(30) NOT NULL UNIQUE,
    
    -- Relationships
    member_id UUID NOT NULL,
    policy_id UUID NOT NULL,
    
    -- Survey Context
    survey_type VARCHAR(30) NOT NULL,
    interaction_id UUID,
    case_id UUID,
    
    -- Survey Lifecycle
    sent_date TIMESTAMP NOT NULL,
    completed_date TIMESTAMP,
    
    -- CSAT Response
    csat_score INT CHECK (csat_score BETWEEN 1 AND 5),
    csat_label VARCHAR(30),
    
    -- Additional Questions
    effort_score INT CHECK (effort_score BETWEEN 1 AND 5),
    recommend_agent BOOLEAN,
    
    -- Verbatim
    feedback_text VARCHAR(1000),
    
    -- Sentiment
    sentiment_label VARCHAR(20),
    
    -- Survey Channel
    survey_channel VARCHAR(20),
    response_time_minutes INT,
    
    -- Processing Info
    pending_id UUID,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    
    -- Foreign Keys
    CONSTRAINT fk_csat_member FOREIGN KEY (member_id) 
        REFERENCES member(member_id),
    CONSTRAINT fk_csat_policy FOREIGN KEY (policy_id) 
        REFERENCES policy(policy_id),
    CONSTRAINT fk_csat_interaction FOREIGN KEY (interaction_id) 
        REFERENCES interaction(interaction_id),
    CONSTRAINT fk_csat_case FOREIGN KEY (case_id) 
        REFERENCES service_case(case_id),
    CONSTRAINT fk_csat_pending FOREIGN KEY (pending_id) 
        REFERENCES csat_survey_pending(pending_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_csat_member ON csat_survey(member_id);
CREATE INDEX IF NOT EXISTS idx_csat_interaction ON csat_survey(interaction_id) 
    WHERE interaction_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_csat_case ON csat_survey(case_id) WHERE case_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_csat_score ON csat_survey(csat_score) WHERE csat_score IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_csat_sent ON csat_survey(sent_date);
