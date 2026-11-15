-- DialPulse Production PostgreSQL Schema
-- Migration 001: Campaigns, Leads, Call Attempts & Dead Letter Queue (DLQ)

CREATE TABLE IF NOT EXISTS campaigns (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL,
    mode VARCHAR(32) NOT NULL DEFAULT 'PREDICTIVE',
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT',
    target_abandon_rate DOUBLE PRECISION NOT NULL DEFAULT 0.025,
    max_dial_multiplier DOUBLE PRECISION NOT NULL DEFAULT 3.5,
    allocated_agents INT NOT NULL DEFAULT 10,
    average_handle_sec DOUBLE PRECISION NOT NULL DEFAULT 90.0,
    average_ring_sec DOUBLE PRECISION NOT NULL DEFAULT 15.0,
    historical_answer_rate DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS leads (
    id VARCHAR(64) PRIMARY KEY,
    campaign_id VARCHAR(64) NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    phone_number VARCHAR(32) NOT NULL,
    name VARCHAR(255),
    priority INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    failure_reason VARCHAR(255),
    locked_by VARCHAR(64),
    locked_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Strict Idempotency: Prevent duplicate active leads for the same phone within a campaign
    CONSTRAINT uq_campaign_phone UNIQUE (campaign_id, phone_number)
);

CREATE INDEX IF NOT EXISTS idx_leads_dispatch 
    ON leads (campaign_id, status, priority DESC, next_attempt_at ASC);

CREATE TABLE IF NOT EXISTS call_attempts (
    id VARCHAR(64) PRIMARY KEY,
    campaign_id VARCHAR(64) NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    lead_id VARCHAR(64) NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    phone_number VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    ring_duration_ms INT NOT NULL DEFAULT 0,
    talk_duration_ms INT NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_call_attempts_campaign 
    ON call_attempts (campaign_id, created_at DESC);

-- Do-Not-Call (DNC) Persistent Registry
CREATE TABLE IF NOT EXISTS dnc_numbers (
    phone_number VARCHAR(32) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
