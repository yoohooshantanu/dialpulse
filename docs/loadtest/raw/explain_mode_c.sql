BEGIN;
CREATE INDEX IF NOT EXISTS idx_leads_partial_queued ON leads (campaign_id, priority DESC, created_at ASC) WHERE status = 'QUEUED';
ANALYZE leads;
SET statement_timeout = '60s';
EXPLAIN (ANALYZE, BUFFERS)
UPDATE leads
SET status = 'DIALING',
    locked_by = 'bench-worker',
    locked_at = clock_timestamp(),
    lease_token = gen_random_uuid()::text
WHERE id IN (
	SELECT id FROM leads
	WHERE campaign_id = 'camp-load-1'
	  AND status = 'QUEUED'
	  AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
	ORDER BY priority DESC, created_at ASC
	LIMIT 50
	FOR UPDATE SKIP LOCKED
);
ROLLBACK;
