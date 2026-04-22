--- =============================================================================
--- Management Expense Schema
--- =============================================================================
--- Fund-level management expense double-entry GL journal lines.
--- Generated post-simulation by the ManagementExpenseEngine.
---
--- Tables:
---   finance.journal_line   - Debit/credit postings per (category, month)
--- =============================================================================

CREATE TABLE IF NOT EXISTS finance.journal_line (
    journal_line_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reporting_month         DATE NOT NULL,
    gl_period_id            INTEGER NOT NULL REFERENCES reference.gl_period(period_id),
    gl_account_id           INTEGER NOT NULL REFERENCES reference.gl_account(account_id),
    cost_centre_id          INTEGER REFERENCES reference.cost_centre(cost_centre_id),
    expense_category        VARCHAR(60) NOT NULL,
    debit_amount            DECIMAL(14,2) NOT NULL DEFAULT 0,
    credit_amount           DECIMAL(14,2) NOT NULL DEFAULT 0,
    journal_source          VARCHAR(30) NOT NULL DEFAULT 'MGMT_EXPENSE_ENGINE',
    description             VARCHAR(200),
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    CHECK ((debit_amount = 0) <> (credit_amount = 0))
);

CREATE INDEX IF NOT EXISTS idx_finjl_month ON finance.journal_line(reporting_month);
CREATE INDEX IF NOT EXISTS idx_finjl_account ON finance.journal_line(gl_account_id);
CREATE INDEX IF NOT EXISTS idx_finjl_period ON finance.journal_line(gl_period_id);
CREATE INDEX IF NOT EXISTS idx_finjl_category ON finance.journal_line(expense_category);
CREATE INDEX IF NOT EXISTS idx_finjl_cost_centre ON finance.journal_line(cost_centre_id);
