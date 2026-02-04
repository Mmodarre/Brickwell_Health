-- ============================================================================
-- CRM DOMAIN SCHEMA
-- Version: 1.0
-- Purpose: Interactions, Cases, and Complaints for NBA/NPS data simulation
-- ============================================================================

-- ============================================================================
-- INTERACTION TABLE
-- Records every member contact with the insurer across all channels
-- ============================================================================

CREATE TABLE IF NOT EXISTS interaction (
    -- Primary Key
    interaction_id UUID PRIMARY KEY,
    interaction_reference VARCHAR(30) NOT NULL UNIQUE,
    
    -- Relationships
    policy_id UUID NOT NULL,
    member_id UUID NOT NULL,
    interaction_type_id INT NOT NULL,
    
    -- Channel & Direction
    channel VARCHAR(30) NOT NULL,
    direction VARCHAR(20) NOT NULL,
    
    -- Timing
    start_datetime TIMESTAMP NOT NULL,
    end_datetime TIMESTAMP,
    duration_seconds INT,
    
    -- Content
    subject VARCHAR(200),
    summary VARCHAR(2000),
    
    -- Outcome
    outcome_id INT,
    handled_by VARCHAR(50),
    queue_name VARCHAR(50),
    wait_time_seconds INT,
    
    -- Resolution
    first_contact_resolution BOOLEAN DEFAULT FALSE,
    satisfaction_score INT CHECK (satisfaction_score BETWEEN 1 AND 5),
    
    -- Trigger Context
    trigger_event_type VARCHAR(50),
    trigger_event_id UUID,
    
    -- Linked Records
    case_id UUID,
    claim_id UUID,
    invoice_id UUID,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at TIMESTAMP,
    modified_by VARCHAR(50),
    
    -- Foreign Keys
    CONSTRAINT fk_interaction_policy FOREIGN KEY (policy_id) 
        REFERENCES policy(policy_id),
    CONSTRAINT fk_interaction_member FOREIGN KEY (member_id) 
        REFERENCES member(member_id),
    CONSTRAINT fk_interaction_claim FOREIGN KEY (claim_id) 
        REFERENCES claim(claim_id),
    CONSTRAINT fk_interaction_invoice FOREIGN KEY (invoice_id) 
        REFERENCES invoice(invoice_id)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_interaction_member ON interaction(member_id);
CREATE INDEX IF NOT EXISTS idx_interaction_policy ON interaction(policy_id);
CREATE INDEX IF NOT EXISTS idx_interaction_date ON interaction(start_datetime);
CREATE INDEX IF NOT EXISTS idx_interaction_trigger ON interaction(trigger_event_type, trigger_event_id);
CREATE INDEX IF NOT EXISTS idx_interaction_claim ON interaction(claim_id) WHERE claim_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_interaction_channel ON interaction(channel);
CREATE INDEX IF NOT EXISTS idx_interaction_fcr ON interaction(first_contact_resolution);

-- ============================================================================
-- SERVICE_CASE TABLE
-- Service tickets that require follow-up or multi-step resolution
-- Using service_case to avoid SQL reserved word "case"
-- ============================================================================

CREATE TABLE IF NOT EXISTS service_case (
    -- Primary Key
    case_id UUID PRIMARY KEY,
    case_number VARCHAR(30) NOT NULL UNIQUE,
    
    -- Type & Classification
    case_type_id INT NOT NULL,
    
    -- Relationships
    policy_id UUID NOT NULL,
    member_id UUID NOT NULL,
    
    -- Context
    subject VARCHAR(200) NOT NULL,
    description VARCHAR(4000),
    
    -- Priority & Status
    priority VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Open',
    
    -- Assignment
    assigned_to VARCHAR(50),
    assigned_team VARCHAR(50),
    
    -- Source
    source_interaction_id UUID,
    
    -- Related Entities
    related_claim_id UUID,
    related_invoice_id UUID,
    
    -- SLA
    due_date DATE,
    resolution_date TIMESTAMP,
    resolution_summary VARCHAR(1000),
    sla_breached BOOLEAN DEFAULT FALSE,
    
    -- Metrics
    note_count INT DEFAULT 0,
    task_count INT DEFAULT 0,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at TIMESTAMP,
    modified_by VARCHAR(50),
    
    -- Foreign Keys
    CONSTRAINT fk_case_policy FOREIGN KEY (policy_id) 
        REFERENCES policy(policy_id),
    CONSTRAINT fk_case_member FOREIGN KEY (member_id) 
        REFERENCES member(member_id),
    CONSTRAINT fk_case_interaction FOREIGN KEY (source_interaction_id) 
        REFERENCES interaction(interaction_id),
    CONSTRAINT fk_case_claim FOREIGN KEY (related_claim_id) 
        REFERENCES claim(claim_id),
    CONSTRAINT fk_case_invoice FOREIGN KEY (related_invoice_id) 
        REFERENCES invoice(invoice_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_case_member ON service_case(member_id);
CREATE INDEX IF NOT EXISTS idx_case_policy ON service_case(policy_id);
CREATE INDEX IF NOT EXISTS idx_case_status ON service_case(status);
CREATE INDEX IF NOT EXISTS idx_case_claim ON service_case(related_claim_id) WHERE related_claim_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_case_invoice ON service_case(related_invoice_id) WHERE related_invoice_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_case_due_date ON service_case(due_date) WHERE status NOT IN ('Resolved', 'Closed');
CREATE INDEX IF NOT EXISTS idx_case_sla_breached ON service_case(sla_breached) WHERE sla_breached = TRUE;

-- ============================================================================
-- COMPLAINT TABLE
-- Formal complaints including PHIO escalations
-- ============================================================================

CREATE TABLE IF NOT EXISTS complaint (
    -- Primary Key
    complaint_id UUID PRIMARY KEY,
    complaint_number VARCHAR(30) NOT NULL UNIQUE,
    
    -- Relationships
    case_id UUID,
    policy_id UUID NOT NULL,
    member_id UUID NOT NULL,
    
    -- Classification
    complaint_category_id INT NOT NULL,
    
    -- Content
    subject VARCHAR(200) NOT NULL,
    description VARCHAR(4000),
    
    -- Severity & Status
    severity VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Received',
    source VARCHAR(30) NOT NULL,
    
    -- Timeline
    received_date DATE NOT NULL,
    acknowledged_date DATE,
    due_date DATE NOT NULL,
    
    -- Assignment
    assigned_to VARCHAR(50),
    
    -- Resolution
    resolution_date DATE,
    resolution_summary VARCHAR(2000),
    resolution_outcome VARCHAR(50),
    compensation_amount DECIMAL(10,2),
    
    -- PHIO Escalation
    phio_escalated BOOLEAN DEFAULT FALSE,
    phio_reference VARCHAR(30),
    phio_escalation_date DATE,
    phio_decision_outcome VARCHAR(50),
    
    -- Internal Review
    internal_review_requested BOOLEAN DEFAULT FALSE,
    internal_review_outcome VARCHAR(50),
    
    -- Escalation Tracking
    escalation_count INT DEFAULT 0,
    
    -- Related Entities
    related_claim_id UUID,
    related_invoice_id UUID,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at TIMESTAMP,
    modified_by VARCHAR(50),
    
    -- Foreign Keys
    CONSTRAINT fk_complaint_case FOREIGN KEY (case_id) 
        REFERENCES service_case(case_id),
    CONSTRAINT fk_complaint_policy FOREIGN KEY (policy_id) 
        REFERENCES policy(policy_id),
    CONSTRAINT fk_complaint_member FOREIGN KEY (member_id) 
        REFERENCES member(member_id),
    CONSTRAINT fk_complaint_claim FOREIGN KEY (related_claim_id) 
        REFERENCES claim(claim_id),
    CONSTRAINT fk_complaint_invoice FOREIGN KEY (related_invoice_id) 
        REFERENCES invoice(invoice_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_complaint_member ON complaint(member_id);
CREATE INDEX IF NOT EXISTS idx_complaint_policy ON complaint(policy_id);
CREATE INDEX IF NOT EXISTS idx_complaint_status ON complaint(status);
CREATE INDEX IF NOT EXISTS idx_complaint_phio ON complaint(phio_escalated) WHERE phio_escalated = TRUE;
CREATE INDEX IF NOT EXISTS idx_complaint_claim ON complaint(related_claim_id) WHERE related_claim_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_complaint_category ON complaint(complaint_category_id);
CREATE INDEX IF NOT EXISTS idx_complaint_due ON complaint(due_date) WHERE status NOT IN ('Resolved', 'Closed');
CREATE INDEX IF NOT EXISTS idx_complaint_severity ON complaint(severity);
