-- DialPulse Migration 002: Transactional Outbox, Downstream Idempotency, and Outcome Aggregates

CREATE TABLE IF NOT EXISTS outbox_events (
    id VARCHAR(128) PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed 
    ON outbox_events (created_at ASC) 
    WHERE published_at IS NULL;

-- Downstream Consumer Idempotency Table
CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(128) NOT NULL,
    consumer_name VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (event_id, consumer_name)
);

-- Per-Campaign Aggregated Outcome Counters (for Consumer DB Effect)
CREATE TABLE IF NOT EXISTS campaign_outcomes (
    campaign_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (campaign_id, status)
);
