-- =============================================================================
-- BILLING DOMAIN
-- =============================================================================

-- INVOICE: Premium invoices
CREATE TABLE IF NOT EXISTS invoice (
    invoice_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_number          VARCHAR(25) NOT NULL UNIQUE,
    
    policy_id               UUID NOT NULL REFERENCES policy(policy_id),
    
    invoice_date            DATE NOT NULL,
    due_date                DATE NOT NULL,
    period_start            DATE NOT NULL,
    period_end              DATE NOT NULL,
    
    invoice_status          VARCHAR(20) NOT NULL DEFAULT 'Issued',
    
    -- Amounts
    gross_premium           DECIMAL(10,2) NOT NULL,
    lhc_loading_amount      DECIMAL(10,2) DEFAULT 0,
    age_discount_amount     DECIMAL(10,2) DEFAULT 0,
    rebate_amount           DECIMAL(10,2) DEFAULT 0,
    other_adjustments       DECIMAL(10,2) DEFAULT 0,
    net_amount              DECIMAL(10,2) NOT NULL,
    
    gst_amount              DECIMAL(10,2) DEFAULT 0,
    total_amount            DECIMAL(10,2) NOT NULL,
    
    paid_amount             DECIMAL(10,2) DEFAULT 0,
    balance_due             DECIMAL(10,2),
    
    -- Retry state for checkpoint resume (supports incremental simulation)
    retry_attempts          INTEGER DEFAULT 0,
    next_retry_date         DATE,
    arrears_created         BOOLEAN DEFAULT FALSE,
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_inv_number ON invoice(invoice_number);
CREATE INDEX IF NOT EXISTS idx_inv_policy ON invoice(policy_id);
CREATE INDEX IF NOT EXISTS idx_inv_date ON invoice(invoice_date);
CREATE INDEX IF NOT EXISTS idx_inv_status ON invoice(invoice_status);

-- PAYMENT: Premium payments received
CREATE TABLE IF NOT EXISTS payment (
    payment_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_number          VARCHAR(25) NOT NULL UNIQUE,
    
    policy_id               UUID NOT NULL REFERENCES policy(policy_id),
    invoice_id              UUID REFERENCES invoice(invoice_id),
    
    payment_date            DATE NOT NULL,
    payment_amount          DECIMAL(10,2) NOT NULL,
    
    payment_method          VARCHAR(20) NOT NULL,  -- DirectDebit/BPay/Card/EFT
    payment_status          VARCHAR(20) NOT NULL DEFAULT 'Completed',
    
    bank_reference          VARCHAR(50),
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_pay_number ON payment(payment_number);
CREATE INDEX IF NOT EXISTS idx_pay_policy ON payment(policy_id);
CREATE INDEX IF NOT EXISTS idx_pay_invoice ON payment(invoice_id);
CREATE INDEX IF NOT EXISTS idx_pay_date ON payment(payment_date);

-- DIRECT_DEBIT_MANDATE: Direct debit arrangements
CREATE TABLE IF NOT EXISTS direct_debit_mandate (
    direct_debit_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy(policy_id),
    bank_account_id         UUID NOT NULL REFERENCES bank_account(bank_account_id),
    
    debit_day               INTEGER NOT NULL,  -- Day of month
    frequency               VARCHAR(20) NOT NULL DEFAULT 'Monthly',
    max_debit_amount        DECIMAL(10,2),
    
    mandate_reference       VARCHAR(50) NOT NULL UNIQUE,
    authorization_date      DATE NOT NULL,
    authorization_method    VARCHAR(30) NOT NULL,  -- Online/PaperForm/Phone
    
    status                  VARCHAR(20) NOT NULL DEFAULT 'Active',
    cancellation_date       DATE,
    cancellation_reason     VARCHAR(200),
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_ddm_policy ON direct_debit_mandate(policy_id);
CREATE INDEX IF NOT EXISTS idx_ddm_status ON direct_debit_mandate(status);

-- DIRECT_DEBIT_RESULT: Debit attempt results
CREATE TABLE IF NOT EXISTS direct_debit_result (
    result_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    direct_debit_id         UUID NOT NULL REFERENCES direct_debit_mandate(direct_debit_id),
    invoice_id              UUID REFERENCES invoice(invoice_id),
    
    attempt_date            DATE NOT NULL,
    attempt_number          INTEGER NOT NULL DEFAULT 1,
    
    requested_amount        DECIMAL(10,2) NOT NULL,
    result_status           VARCHAR(20) NOT NULL,  -- Success/Dishonoured/InsufficientFunds/AccountClosed
    result_code             VARCHAR(10),
    result_description      VARCHAR(200),
    
    settlement_date         DATE,
    payment_id              UUID REFERENCES payment(payment_id),
    
    retry_scheduled         BOOLEAN NOT NULL DEFAULT FALSE,
    retry_date              DATE,
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_ddr_mandate ON direct_debit_result(direct_debit_id);
CREATE INDEX IF NOT EXISTS idx_ddr_invoice ON direct_debit_result(invoice_id);
CREATE INDEX IF NOT EXISTS idx_ddr_attempt ON direct_debit_result(attempt_date);

-- ARREARS: Overdue premium tracking
CREATE TABLE IF NOT EXISTS arrears (
    arrears_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy(policy_id),
    invoice_id              UUID NOT NULL REFERENCES invoice(invoice_id),
    
    arrears_date            DATE NOT NULL,
    arrears_amount          DECIMAL(10,2) NOT NULL,
    days_overdue            INTEGER NOT NULL,
    
    arrears_status          VARCHAR(20) NOT NULL,  -- Current/Resolved/WrittenOff
    resolution_date         DATE,
    resolution_method       VARCHAR(30),  -- Payment/WriteOff/Cancellation
    
    reminder_sent           BOOLEAN DEFAULT FALSE,
    reminder_date           DATE,
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION',
    modified_at             TIMESTAMP,
    modified_by             VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_arr_policy ON arrears(policy_id);
CREATE INDEX IF NOT EXISTS idx_arr_invoice ON arrears(invoice_id);
CREATE INDEX IF NOT EXISTS idx_arr_status ON arrears(arrears_status);

-- REFUND: Premium refunds
CREATE TABLE IF NOT EXISTS refund (
    refund_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    refund_reference        VARCHAR(30) NOT NULL UNIQUE,
    policy_id               UUID NOT NULL REFERENCES policy(policy_id),
    member_id               UUID REFERENCES member(member_id),
    
    refund_date             DATE NOT NULL,
    refund_amount           DECIMAL(10,2) NOT NULL,
    refund_reason           VARCHAR(200) NOT NULL,
    refund_type             VARCHAR(30) NOT NULL,  -- Cancellation/Overpayment/Adjustment
    
    payment_method          VARCHAR(30) NOT NULL,  -- EFT/OriginalMethod/Cheque
    bank_account_id         UUID REFERENCES bank_account(bank_account_id),
    
    status                  VARCHAR(20) NOT NULL DEFAULT 'Pending',
    processed_date          DATE,
    bank_reference          VARCHAR(50),
    
    approved_by             VARCHAR(50),
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_ref_policy ON refund(policy_id);
CREATE INDEX IF NOT EXISTS idx_ref_status ON refund(status);

-- PREMIUM_DISCOUNT: Applied discounts
CREATE TABLE IF NOT EXISTS premium_discount (
    premium_discount_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id               UUID NOT NULL REFERENCES policy(policy_id),
    
    discount_type           VARCHAR(30) NOT NULL,  -- AgeBased/Corporate/MultiPolicy/Loyalty
    discount_percentage     DECIMAL(5,2) NOT NULL,
    discount_amount         DECIMAL(10,2),
    
    effective_date          DATE NOT NULL,
    end_date                DATE,
    
    reason                  VARCHAR(200),
    corporate_account_id    INTEGER,
    
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(50) NOT NULL DEFAULT 'SIMULATION'
);

CREATE INDEX IF NOT EXISTS idx_pd_policy ON premium_discount(policy_id);
CREATE INDEX IF NOT EXISTS idx_pd_type ON premium_discount(discount_type);
