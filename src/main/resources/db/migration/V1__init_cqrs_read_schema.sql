-- V1__init_cqrs_read_schema.sql
-- CQRS Reporting Read Schema for PostgreSQL 18 with native UUID Primary & Foreign Keys

CREATE TABLE charge_projection (
    id UUID PRIMARY KEY,
    client_id VARCHAR(64) NOT NULL,
    charge_type VARCHAR(32) NOT NULL,
    total_amount NUMERIC(19, 2) NOT NULL,
    paid_amount NUMERIC(19, 2) NOT NULL DEFAULT 0.00,
    remaining_amount NUMERIC(19, 2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_charge_proj_client_status ON charge_projection(client_id, status);
CREATE INDEX idx_charge_proj_created ON charge_projection(created_at DESC);

CREATE TABLE sibling_va_projection (
    id UUID PRIMARY KEY,
    charge_id UUID NOT NULL REFERENCES charge_projection(id) ON DELETE CASCADE,
    bank_code VARCHAR(32) NOT NULL,
    va_number VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_bank_va UNIQUE (bank_code, va_number)
);

CREATE INDEX idx_sibling_va_charge ON sibling_va_projection(charge_id);

CREATE TABLE payment_projection (
    id UUID PRIMARY KEY,
    charge_id UUID NOT NULL,
    bank_code VARCHAR(32) NOT NULL,
    va_number VARCHAR(64) NOT NULL,
    bank_reference VARCHAR(128) NOT NULL,
    amount NUMERIC(19, 2) NOT NULL,
    payment_timestamp TIMESTAMPTZ NOT NULL,
    is_double_settlement BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_payment_proj_charge ON payment_projection(charge_id);
CREATE INDEX idx_payment_proj_bank_ref ON payment_projection(bank_code, bank_reference);
CREATE INDEX idx_payment_proj_ts ON payment_projection(payment_timestamp DESC);

CREATE TABLE reconciliation_projection (
    id UUID PRIMARY KEY,
    settlement_date DATE NOT NULL,
    bank_code VARCHAR(32) NOT NULL,
    total_records INT NOT NULL,
    matched_records INT NOT NULL,
    discrepancy_records INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_recon_proj_bank_date ON reconciliation_projection(bank_code, settlement_date);
