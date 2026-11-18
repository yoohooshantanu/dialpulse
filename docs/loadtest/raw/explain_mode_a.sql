BEGIN;
DROP INDEX IF EXISTS idx_leads_comp;
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
	  AND (
	    ((status = 'QUEUED' OR status = 'DRAFT') AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
	    OR
	    (status = 'DIALING' AND locked_at IS NOT NULL AND locked_at < clock_timestamp() - '15.000000 seconds'::interval)
	  )
	ORDER BY priority DESC, created_at ASC
	LIMIT 50
	FOR UPDATE SKIP LOCKED
);
ROLLBACK;
