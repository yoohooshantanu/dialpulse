package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"

	"github.com/yoohooshantanu/dialpulse/internal/model"
	"github.com/yoohooshantanu/dialpulse/internal/queue"
	"github.com/yoohooshantanu/dialpulse/internal/storage"
)

type Config struct {
	Scenario          string
	Workers           int
	NumLeads          int
	BatchSize         int
	DatabaseURL       string
	NATSURL           string
	CampaignID        string
	Tiny              bool
	Explain           bool
	QueryMode         string // "unified" or "split"
	Runs              int
	Ramp              bool
	DatasetExperiment bool
}

type StepMetrics struct {
	Scenario          string
	Workers           int
	NumLeads          int
	DurationSec       float64
	ClaimsPerSec      float64
	RecordsPerSec     float64
	ClaimP50Ms        float64
	ClaimP95Ms        float64
	ClaimP99Ms        float64
	RecordP50Ms       float64
	RecordP95Ms       float64
	RecordP99Ms       float64
	TotalP50Ms        float64
	TotalP95Ms        float64
	TotalP99Ms        float64
	PoolAcquireWaitMs float64
	PoolEmptyAcquires int64
	MaxActiveConns    int
	MaxLockWaits      int
	MaxPostgresCPU    float64
	OutboxDrainSec    float64
	MaxOutboxLagSec   float64
	IndexUsed         string
	QueryMeanTimeMs   float64
	QueuedTimeline    []string
}

type RangeMetrics struct {
	Scenario           string
	Workers            int
	NumLeads           int
	ClaimsPerSecRange  string
	RecordsPerSecRange string
	ClaimP50Range      string
	ClaimP99Range      string
	RecordP50Range     string
	RecordP99Range     string
	TotalP50Range      string
	TotalP99Range      string
	PoolWaitRange      string
	EmptyAcquiresRange string
	ActiveConnsRange   string
	LockWaitsRange     string
	PostgresCPURange   string
	OutboxDrainRange   string
	MaxOutboxLagRange  string
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Scenario, "scenario", "A", "Scenario: A (claiming only) or AB (claiming + outbox relay/consumer)")
	flag.IntVar(&cfg.Workers, "workers", 2, "Number of concurrent claiming workers")
	flag.IntVar(&cfg.NumLeads, "leads", 2000, "Number of leads to seed")
	flag.IntVar(&cfg.BatchSize, "batch-size", 50, "Batch size for claiming")
	flag.StringVar(&cfg.DatabaseURL, "db-url", "postgres://postgres:password@localhost:5432/dialpulse_test?sslmode=disable", "PostgreSQL connection URL")
	flag.StringVar(&cfg.NATSURL, "nats-url", "nats://localhost:4222", "NATS connection URL")
	flag.StringVar(&cfg.CampaignID, "campaign", "camp-load-1", "Campaign ID")
	flag.BoolVar(&cfg.Tiny, "tiny", false, "Run tiny self-check (500 leads)")
	flag.BoolVar(&cfg.Explain, "explain", false, "Run EXPLAIN ANALYZE index experiment")
	flag.StringVar(&cfg.QueryMode, "query-mode", "split", "Query mode: unified or split")
	flag.IntVar(&cfg.Runs, "runs", 3, "Number of runs per step (for range reporting)")
	flag.BoolVar(&cfg.Ramp, "ramp", false, "Run full ramp across W=1,2,4,8,16,32")
	flag.BoolVar(&cfg.DatasetExperiment, "dataset-experiment", false, "Run dataset drain experiment comparing table sizes")
	flag.Parse()

	if cfg.Tiny {
		cfg.NumLeads = 500
		cfg.Workers = 2
		cfg.Runs = 1
	}

	ctx := context.Background()

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to parse DB URL: %v", err)
	}
	poolCfg.MaxConns = 80
	poolCfg.MinConns = 4
	poolCfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("Failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping postgres: %v", err)
	}

	repo := storage.NewPostgresRepository(pool)

	if cfg.Explain {
		runIndexExperiment(ctx, pool, cfg)
		return
	}

	if cfg.DatasetExperiment {
		runDatasetDrainExperiment(ctx, pool, repo, cfg)
		return
	}

	if cfg.Ramp {
		runFullRamp(ctx, pool, repo, cfg)
		return
	}

	// Single step multi-run
	var stepRuns []StepMetrics
	for r := 1; r <= cfg.Runs; r++ {
		fmt.Printf("\n--- Run %d of %d ---\n", r, cfg.Runs)
		res, err := runBenchmark(ctx, pool, repo, cfg)
		if err != nil {
			log.Fatalf("Benchmark run %d failed: %v", r, err)
		}
		stepRuns = append(stepRuns, res)
	}

	rMetric := summarizeRanges(stepRuns)
	printRangeTable([]RangeMetrics{rMetric})
}

func runFullRamp(ctx context.Context, pool *pgxpool.Pool, repo *storage.PostgresRepository, baseCfg Config) {
	workerCounts := []int{1, 2, 4, 8, 16, 32}
	var allRangeMetrics []RangeMetrics

	fmt.Printf("\n=================================================================\n")
	fmt.Printf("STARTING FULL RAMP: Scenario %s across W=%v (%d runs each)\n",
		baseCfg.Scenario, workerCounts, baseCfg.Runs)
	fmt.Printf("=================================================================\n")

	for _, w := range workerCounts {
		cfg := baseCfg
		cfg.Workers = w
		var runs []StepMetrics

		for r := 1; r <= cfg.Runs; r++ {
			fmt.Printf("\n>>> Step W=%d | Run %d/%d (Leads=%d, Mode=%s) <<<\n",
				w, r, cfg.Runs, cfg.NumLeads, cfg.QueryMode)
			res, err := runBenchmark(ctx, pool, repo, cfg)
			if err != nil {
				log.Fatalf("Step W=%d Run %d failed: %v", w, r, err)
			}
			runs = append(runs, res)
			time.Sleep(1 * time.Second)
		}

		rm := summarizeRanges(runs)
		allRangeMetrics = append(allRangeMetrics, rm)
	}

	printRangeTable(allRangeMetrics)
}

func runDatasetDrainExperiment(ctx context.Context, pool *pgxpool.Pool, repo *storage.PostgresRepository, cfg Config) {
	fmt.Println("=================================================================")
	fmt.Println("DATASET DRAIN EXPERIMENT: 100k Leads Drain Progression")
	fmt.Println("=================================================================")

	cfg.Scenario = "A"
	cfg.Workers = 8
	cfg.NumLeads = 100000
	cfg.BatchSize = 50

	res, err := runBenchmark(ctx, pool, repo, cfg)
	if err != nil {
		log.Fatalf("Drain experiment failed: %v", err)
	}

	fmt.Println("\nQueue Drain Timeline (sampled every 250ms):")
	for i, q := range res.QueuedTimeline {
		if i%10 == 0 || i == len(res.QueuedTimeline)-1 {
			fmt.Printf("  T+%.1fs: %s QUEUED leads remaining\n", float64(i)*0.25, q)
		}
	}
	printMetricsTable([]StepMetrics{res})
}

func runIndexExperiment(ctx context.Context, pool *pgxpool.Pool, cfg Config) {
	fmt.Println("=================================================================")
	fmt.Println("INDEX EXPERIMENT: EXPLAIN (ANALYZE, BUFFERS)")
	fmt.Printf("Dataset size: %d leads in campaign %s\n", cfg.NumLeads, cfg.CampaignID)
	fmt.Println("=================================================================")

	resetTables(ctx, pool)
	ensureCampaign(ctx, pool, cfg.CampaignID)
	seedLeads(ctx, pool, cfg.CampaignID, cfg.NumLeads)

	_, _ = pool.Exec(ctx, "ANALYZE leads;")

	limit := cfg.BatchSize
	intervalStr := fmt.Sprintf("%f seconds", model.DefaultLeaseTTL.Seconds())

	// 1. Current Unified Query with existing index: idx_leads_dispatch ON leads (campaign_id, status, priority DESC, next_attempt_at ASC)
	fmt.Println("\n--- [Experiment 1] Current Unified Query with Migration 001 Index ---")
	runExplain(ctx, pool, unifiedQuery, "Unified Query (Migration 001 idx_leads_dispatch)", cfg.CampaignID, limit, intervalStr)

	// 2. Add Partial Index: idx_leads_queued ON leads (campaign_id, priority DESC, created_at ASC) WHERE status = 'QUEUED'
	fmt.Println("\n--- [Experiment 2] Adding Partial Index on status = 'QUEUED' ---")
	_, err := pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_leads_queued ON leads (campaign_id, priority DESC, created_at ASC) WHERE status = 'QUEUED';`)
	if err != nil {
		log.Printf("Error creating partial index: %v", err)
	}
	_, _ = pool.Exec(ctx, "ANALYZE leads;")
	runExplain(ctx, pool, unifiedQuery, "Unified Query with Partial Index (status='QUEUED')", cfg.CampaignID, limit, intervalStr)

	// 3. Add Composite Index: idx_leads_comp ON leads (campaign_id, priority DESC, created_at ASC)
	fmt.Println("\n--- [Experiment 3] Adding Composite Index on (campaign_id, priority DESC, created_at ASC) ---")
	_, err = pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_leads_comp ON leads (campaign_id, priority DESC, created_at ASC);`)
	if err != nil {
		log.Printf("Error creating composite index: %v", err)
	}
	_, _ = pool.Exec(ctx, "ANALYZE leads;")
	runExplain(ctx, pool, unifiedQuery, "Unified Query with Composite Index (campaign_id, priority DESC, created_at ASC)", cfg.CampaignID, limit, intervalStr)

	// 4. Split Claim: Fast Path (QUEUED only)
	fmt.Println("\n--- [Experiment 4] Split Claim: Fast Path (status = 'QUEUED' only) ---")
	runExplain(ctx, pool, fastPathQuery, "Split Claim Fast Path (status='QUEUED')", cfg.CampaignID, limit, intervalStr)

	// 5. Split Claim: Expired Lease Path (status = 'DIALING' AND locked_at < clock_timestamp() - interval)
	fmt.Println("\n--- [Experiment 5] Split Claim: Expired Lease Fallback Path ---")
	runExplain(ctx, pool, expiredPathQuery, "Split Claim Expired Lease Path", cfg.CampaignID, limit, intervalStr)
}

const unifiedQuery = `
EXPLAIN (ANALYZE, BUFFERS)
UPDATE leads
SET status = 'DIALING',
    locked_by = 'bench-worker',
    locked_at = clock_timestamp(),
    lease_token = gen_random_uuid()::text
WHERE id IN (
	SELECT id FROM leads
	WHERE campaign_id = $1
	  AND (
	    ((status = 'QUEUED' OR status = 'DRAFT') AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
	    OR
	    (status = 'DIALING' AND locked_at IS NOT NULL AND locked_at < clock_timestamp() - $3::interval)
	  )
	ORDER BY priority DESC, created_at ASC
	LIMIT $2
	FOR UPDATE SKIP LOCKED
);`

const fastPathQuery = `
EXPLAIN (ANALYZE, BUFFERS)
UPDATE leads
SET status = 'DIALING',
    locked_by = 'bench-worker',
    locked_at = clock_timestamp(),
    lease_token = gen_random_uuid()::text
WHERE id IN (
	SELECT id FROM leads
	WHERE campaign_id = $1
	  AND status = 'QUEUED'
	  AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
	ORDER BY priority DESC, created_at ASC
	LIMIT $2
	FOR UPDATE SKIP LOCKED
);`

const expiredPathQuery = `
EXPLAIN (ANALYZE, BUFFERS)
UPDATE leads
SET status = 'DIALING',
    locked_by = 'bench-worker',
    locked_at = clock_timestamp(),
    lease_token = gen_random_uuid()::text
WHERE id IN (
	SELECT id FROM leads
	WHERE campaign_id = $1
	  AND status = 'DIALING'
	  AND locked_at IS NOT NULL
	  AND locked_at < clock_timestamp() - $3::interval
	ORDER BY priority DESC, created_at ASC
	LIMIT $2
	FOR UPDATE SKIP LOCKED
);`

func runExplain(ctx context.Context, pool *pgxpool.Pool, query, label, campaignID string, limit int, intervalStr string) {
	fmt.Printf(">>> EXPLAIN for: %s\n", label)
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Printf("tx begin err: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	var rows pgx.Rows
	if strings.Contains(query, "$3") {
		rows, err = tx.Query(ctx, query, campaignID, limit, intervalStr)
	} else {
		rows, err = tx.Query(ctx, query, campaignID, limit)
	}
	if err != nil {
		log.Printf("query error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			log.Printf("scan error: %v", err)
			return
		}
		fmt.Println("  " + line)
	}
}

func runBenchmark(ctx context.Context, pool *pgxpool.Pool, repo *storage.PostgresRepository, cfg Config) (StepMetrics, error) {
	resetTables(ctx, pool)
	ensureCampaign(ctx, pool, cfg.CampaignID)
	seedLeads(ctx, pool, cfg.CampaignID, cfg.NumLeads)

	_, _ = pool.Exec(ctx, "SELECT pg_stat_statements_reset();")

	var nc *nats.Conn
	var js nats.JetStreamContext
	var relay *queue.OutboxRelay
	var consumer *queue.OutboxConsumer
	var streamName = "DIALPULSE_EVENTS"
	var consumerName = "dialpulse_outcome_worker"

	if cfg.Scenario == "AB" {
		var err error
		nc, err = nats.Connect(cfg.NATSURL, nats.Timeout(5*time.Second))
		if err != nil {
			return StepMetrics{}, fmt.Errorf("nats connect: %w", err)
		}
		defer nc.Close()

		js, err = nc.JetStream()
		if err != nil {
			return StepMetrics{}, fmt.Errorf("nats jetstream: %w", err)
		}

		_ = js.DeleteStream(streamName)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:       streamName,
			Subjects:   []string{"dialpulse.events.>", "dialpulse.dlq.>"},
			Storage:    nats.MemoryStorage,
			Duplicates: 30 * time.Second,
			MaxAge:     5 * time.Minute,
		})
		if err != nil {
			return StepMetrics{}, fmt.Errorf("add stream: %w", err)
		}

		relayCfg := queue.DefaultOutboxRelayConfig()
		relayCfg.BatchSize = 50
		relayCfg.PollInterval = 10 * time.Millisecond
		relayPublisher := queue.NewNATSJetStreamPublisher(js)
		relay = queue.NewOutboxRelay(pool, relayPublisher, relayCfg)
		relay.Start(ctx)

		consumerCfg := queue.DefaultOutboxConsumerConfig()
		consumerCfg.ConsumerName = consumerName
		consumerCfg.StreamName = streamName
		consumerCfg.SubjectFilter = "dialpulse.events.>"
		consumerHandler := queue.NewCampaignOutcomeAggregator()
		consumer = queue.NewOutboxConsumer(pool, js, consumerHandler, consumerCfg)
		if err := consumer.StartPullConsumer(ctx); err != nil {
			return StepMetrics{}, fmt.Errorf("start consumer: %w", err)
		}
	}

	stopMonitor := make(chan struct{})
	var maxActiveConns int32
	var maxLockWaits int32
	var maxCPU float64
	var maxOutboxLag float64
	var queuedCountTimeline []string
	var monitorWg sync.WaitGroup

	monitorWg.Add(1)
	go func() {
		defer monitorWg.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				var activeConns, lockWaits int
				_ = pool.QueryRow(ctx, `
					SELECT 
						COUNT(*) FILTER (WHERE state = 'active'),
						COUNT(*) FILTER (WHERE wait_event_type = 'Lock')
					FROM pg_stat_activity 
					WHERE datname = 'dialpulse_test';
				`).Scan(&activeConns, &lockWaits)

				if int32(activeConns) > atomic.LoadInt32(&maxActiveConns) {
					atomic.StoreInt32(&maxActiveConns, int32(activeConns))
				}
				if int32(lockWaits) > atomic.LoadInt32(&maxLockWaits) {
					atomic.StoreInt32(&maxLockWaits, int32(lockWaits))
				}

				var queuedCount int64
				_ = pool.QueryRow(ctx, `SELECT count(*) FROM leads WHERE campaign_id = $1 AND status = 'QUEUED';`, cfg.CampaignID).Scan(&queuedCount)
				queuedCountTimeline = append(queuedCountTimeline, fmt.Sprintf("%d", queuedCount))

				if cfg.Scenario == "AB" {
					lag, _ := repo.GetOutboxLagSeconds(ctx)
					if lag > maxOutboxLag {
						maxOutboxLag = lag
					}
				}

				cpu := sampleDockerCPU("dialpulse-postgres")
				if cpu > maxCPU {
					maxCPU = cpu
				}
			}
		}
	}()

	var claimedLeadIDs sync.Map
	var totalClaimed atomic.Int64
	var totalRecorded atomic.Int64
	var claimLatencies []float64
	var recordLatencies []float64
	var totalLatencies []float64
	var latMu sync.Mutex

	recordLat := func(cLat, rLat, tLat float64) {
		latMu.Lock()
		claimLatencies = append(claimLatencies, cLat)
		recordLatencies = append(recordLatencies, rLat)
		totalLatencies = append(totalLatencies, tLat)
		latMu.Unlock()
	}

	benchStart := time.Now()
	var workerWg sync.WaitGroup

	for w := 0; w < cfg.Workers; w++ {
		workerWg.Add(1)
		workerID := fmt.Sprintf("worker-%d", w)

		go func(wID string) {
			defer workerWg.Done()

			for {
				startClaim := time.Now()
				var leads []*model.Lead
				var err error

				if cfg.QueryMode == "split" {
					leads, err = claimSplitQuery(ctx, pool, cfg.CampaignID, cfg.BatchSize, wID)
				} else {
					leads, err = repo.ClaimPendingLeads(ctx, cfg.CampaignID, cfg.BatchSize, wID)
				}

				claimDurMs := float64(time.Since(startClaim).Microseconds()) / 1000.0

				if err != nil {
					log.Printf("[%s] Claim error: %v", wID, err)
					time.Sleep(10 * time.Millisecond)
					continue
				}

				if len(leads) == 0 {
					var queuedRemaining int64
					_ = pool.QueryRow(ctx, `SELECT count(*) FROM leads WHERE campaign_id = $1 AND status = 'QUEUED';`, cfg.CampaignID).Scan(&queuedRemaining)
					if queuedRemaining == 0 {
						break
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}

				perLeadClaimMs := claimDurMs / float64(len(leads))

				for _, lead := range leads {
					totalClaimed.Add(1)

					// INVARIANT 1: No duplicate claim
					if _, loaded := claimedLeadIDs.LoadOrStore(lead.ID, wID); loaded {
						log.Fatalf("FATAL: Invariant violated! Lead %s claimed twice!", lead.ID)
					}

					startRecord := time.Now()
					attemptID := uuid.New().String()

					// RecordAttemptStart
					if err := repo.RecordAttemptStart(ctx, lead.ID, attemptID, lead.Attempts+1, lead.LeaseToken); err != nil {
						log.Fatalf("RecordAttemptStart failed: %v", err)
					}

					// RecordCallOutcomeTx
					attempt := &model.CallAttempt{
						ID:             attemptID,
						CampaignID:     cfg.CampaignID,
						LeadID:         lead.ID,
						PhoneNumber:    lead.PhoneNumber,
						Status:         model.StatusCompleted,
						RingDurationMs: 0,
						TalkDurationMs: 0,
						CreatedAt:      time.Now().UTC(),
					}

					if err := repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusCompleted, "answered", nil, attempt, lead.Attempts+1, lead.LeaseToken); err != nil {
						log.Fatalf("RecordCallOutcomeTx failed: %v", err)
					}

					recordDurMs := float64(time.Since(startRecord).Microseconds()) / 1000.0
					totalRecorded.Add(1)

					recordLat(perLeadClaimMs, recordDurMs, perLeadClaimMs+recordDurMs)
				}
			}
		}(workerID)
	}

	workerWg.Wait()
	benchDuration := time.Since(benchStart)

	outboxStart := time.Now()
	var outboxDrainDuration time.Duration

	if cfg.Scenario == "AB" {
		for {
			var unpublished int64
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL;`).Scan(&unpublished)

			var processed int64
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE consumer_name = $1;`, consumerName).Scan(&processed)

			if unpublished == 0 && processed == int64(cfg.NumLeads) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		outboxDrainDuration = time.Since(outboxStart)

		relay.Stop()
		consumer.Stop()
	}

	close(stopMonitor)
	monitorWg.Wait()

	// Invariant Checks
	var queuedLeft int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM leads WHERE campaign_id = $1 AND status = 'QUEUED';`, cfg.CampaignID).Scan(&queuedLeft)
	if queuedLeft != 0 {
		log.Fatalf("Invariant violated: %d QUEUED leads remain!", queuedLeft)
	}

	var totalAttempts, totalOutbox int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM call_attempts WHERE campaign_id = $1;`, cfg.CampaignID).Scan(&totalAttempts)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events;`).Scan(&totalOutbox)
	if totalAttempts != int64(cfg.NumLeads) || totalOutbox != int64(cfg.NumLeads) {
		log.Fatalf("Invariant violated: call_attempts=%d, outbox_events=%d, expected=%d", totalAttempts, totalOutbox, cfg.NumLeads)
	}

	if cfg.Scenario == "AB" {
		var processedCount, unpublishedCount int64
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE consumer_name = $1;`, consumerName).Scan(&processedCount)
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL;`).Scan(&unpublishedCount)
		if processedCount != int64(cfg.NumLeads) || unpublishedCount != 0 {
			log.Fatalf("Invariant violated in AB: processed=%d, unpublished=%d, expected=%d", processedCount, unpublishedCount, cfg.NumLeads)
		}
	}

	sort.Float64s(claimLatencies)
	sort.Float64s(recordLatencies)
	sort.Float64s(totalLatencies)

	claimP50 := percentile(claimLatencies, 0.50)
	claimP95 := percentile(claimLatencies, 0.95)
	claimP99 := percentile(claimLatencies, 0.99)

	recP50 := percentile(recordLatencies, 0.50)
	recP95 := percentile(recordLatencies, 0.95)
	recP99 := percentile(recordLatencies, 0.99)

	totP50 := percentile(totalLatencies, 0.50)
	totP95 := percentile(totalLatencies, 0.95)
	totP99 := percentile(totalLatencies, 0.99)

	poolStat := pool.Stat()

	var queryMeanTime float64
	_ = pool.QueryRow(ctx, `
		SELECT mean_exec_time 
		FROM pg_stat_statements 
		WHERE query ILIKE '%UPDATE leads%' AND query ILIKE '%SKIP LOCKED%'
		ORDER BY calls DESC LIMIT 1;
	`).Scan(&queryMeanTime)

	metrics := StepMetrics{
		Scenario:          cfg.Scenario,
		Workers:           cfg.Workers,
		NumLeads:          cfg.NumLeads,
		DurationSec:       benchDuration.Seconds(),
		ClaimsPerSec:      float64(totalClaimed.Load()) / benchDuration.Seconds(),
		RecordsPerSec:     float64(totalRecorded.Load()) / benchDuration.Seconds(),
		ClaimP50Ms:        claimP50,
		ClaimP95Ms:        claimP95,
		ClaimP99Ms:        claimP99,
		RecordP50Ms:       recP50,
		RecordP95Ms:       recP95,
		RecordP99Ms:       recP99,
		TotalP50Ms:        totP50,
		TotalP95Ms:        totP95,
		TotalP99Ms:        totP99,
		PoolAcquireWaitMs: float64(poolStat.AcquireDuration().Microseconds()) / 1000.0,
		PoolEmptyAcquires: poolStat.EmptyAcquireCount(),
		MaxActiveConns:    int(atomic.LoadInt32(&maxActiveConns)),
		MaxLockWaits:      int(atomic.LoadInt32(&maxLockWaits)),
		MaxPostgresCPU:    maxCPU,
		OutboxDrainSec:    outboxDrainDuration.Seconds(),
		MaxOutboxLagSec:   maxOutboxLag,
		QueryMeanTimeMs:   queryMeanTime,
		QueuedTimeline:    queuedCountTimeline,
	}

	return metrics, nil
}

func claimSplitQuery(ctx context.Context, pool *pgxpool.Pool, campaignID string, limit int, workerID string) ([]*model.Lead, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	query := `
		UPDATE leads
		SET status = 'DIALING',
		    locked_by = $1,
		    locked_at = clock_timestamp(),
		    lease_token = gen_random_uuid()::text
		WHERE id IN (
			SELECT id FROM leads
			WHERE campaign_id = $2
			  AND status = 'QUEUED'
			  AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
			ORDER BY priority DESC, created_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, campaign_id, phone_number, name, priority,
		          status, attempts, max_attempts, failure_reason,
		          locked_by, locked_at, last_attempt_at, next_attempt_at, created_at, lease_token;`

	rows, err := tx.Query(ctx, query, workerID, campaignID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var claimed []*model.Lead
	for rows.Next() {
		l := &model.Lead{}
		var status string
		var lockedBy *string
		var leaseToken *string
		var failureReason *string
		err := rows.Scan(
			&l.ID, &l.CampaignID, &l.PhoneNumber, &l.Name, &l.Priority,
			&status, &l.Attempts, &l.MaxAttempts, &failureReason,
			&lockedBy, &l.LockedAt, &l.LastAttemptAt, &l.NextAttemptAt, &l.CreatedAt, &leaseToken,
		)
		if err != nil {
			return nil, err
		}
		l.Status = model.CallStatus(status)
		if lockedBy != nil {
			l.LockedBy = *lockedBy
		}
		if leaseToken != nil {
			l.LeaseToken = *leaseToken
		}
		if failureReason != nil {
			l.FailureReason = *failureReason
		}
		claimed = append(claimed, l)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

func resetTables(ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, `
		TRUNCATE TABLE processed_events, outbox_events, call_attempts, leads, campaign_outcomes CASCADE;
	`)
	if err != nil {
		log.Fatalf("resetTables error: %v", err)
	}
}

func ensureCampaign(ctx context.Context, pool *pgxpool.Pool, campaignID string) {
	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, tenant_id, name, mode, status, created_at, updated_at)
		VALUES ($1, 'tenant-load', 'Load Test Campaign', 'PREDICTIVE', 'RUNNING', clock_timestamp(), clock_timestamp())
		ON CONFLICT (id) DO UPDATE SET status = 'RUNNING';
	`, campaignID)
	if err != nil {
		log.Fatalf("ensureCampaign error: %v", err)
	}
}

func seedLeads(ctx context.Context, pool *pgxpool.Pool, campaignID string, numLeads int) {
	start := time.Now()
	batchSize := 5000
	cols := []string{"id", "campaign_id", "phone_number", "name", "priority", "status", "attempts", "max_attempts", "created_at"}

	inserted := 0
	r := rand.New(rand.NewSource(42))

	for inserted < numLeads {
		toInsert := batchSize
		if inserted+toInsert > numLeads {
			toInsert = numLeads - inserted
		}

		rows := make([][]interface{}, toInsert)
		for i := 0; i < toInsert; i++ {
			leadIdx := inserted + i
			id := fmt.Sprintf("lead-%07d", leadIdx)
			phone := fmt.Sprintf("+1555%07d", leadIdx)
			name := fmt.Sprintf("Lead %d", leadIdx)
			priority := r.Intn(10)
			rows[i] = []interface{}{
				id,
				campaignID,
				phone,
				name,
				priority,
				"QUEUED",
				0,
				3,
				time.Now().UTC(),
			}
		}

		_, err := pool.CopyFrom(ctx, pgx.Identifier{"leads"}, cols, pgx.CopyFromRows(rows))
		if err != nil {
			log.Fatalf("CopyFrom seeding error: %v", err)
		}
		inserted += toInsert
	}

	fmt.Printf("Seeded %d leads in %.2fs (%.0f leads/s)\n", numLeads, time.Since(start).Seconds(), float64(numLeads)/time.Since(start).Seconds())
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * pct)
	return sorted[idx]
}

func sampleDockerCPU(container string) float64 {
	cmd := exec.Command("docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", container)
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(out))
	s = strings.TrimSuffix(s, "%")
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return val
}

func summarizeRanges(runs []StepMetrics) RangeMetrics {
	if len(runs) == 0 {
		return RangeMetrics{}
	}

	minC, maxC := runs[0].ClaimsPerSec, runs[0].ClaimsPerSec
	minR, maxR := runs[0].RecordsPerSec, runs[0].RecordsPerSec
	minCP50, maxCP50 := runs[0].ClaimP50Ms, runs[0].ClaimP50Ms
	minCP99, maxCP99 := runs[0].ClaimP99Ms, runs[0].ClaimP99Ms
	minRP50, maxRP50 := runs[0].RecordP50Ms, runs[0].RecordP50Ms
	minRP99, maxRP99 := runs[0].RecordP99Ms, runs[0].RecordP99Ms
	minTP50, maxTP50 := runs[0].TotalP50Ms, runs[0].TotalP50Ms
	minTP99, maxTP99 := runs[0].TotalP99Ms, runs[0].TotalP99Ms
	minWait, maxWait := runs[0].PoolAcquireWaitMs, runs[0].PoolAcquireWaitMs
	minEmpty, maxEmpty := runs[0].PoolEmptyAcquires, runs[0].PoolEmptyAcquires
	minConn, maxConn := runs[0].MaxActiveConns, runs[0].MaxActiveConns
	minLock, maxLock := runs[0].MaxLockWaits, runs[0].MaxLockWaits
	minCPU, maxCPU := runs[0].MaxPostgresCPU, runs[0].MaxPostgresCPU
	minDrain, maxDrain := runs[0].OutboxDrainSec, runs[0].OutboxDrainSec
	minLag, maxLag := runs[0].MaxOutboxLagSec, runs[0].MaxOutboxLagSec

	for _, r := range runs[1:] {
		if r.ClaimsPerSec < minC { minC = r.ClaimsPerSec }
		if r.ClaimsPerSec > maxC { maxC = r.ClaimsPerSec }

		if r.RecordsPerSec < minR { minR = r.RecordsPerSec }
		if r.RecordsPerSec > maxR { maxR = r.RecordsPerSec }

		if r.ClaimP50Ms < minCP50 { minCP50 = r.ClaimP50Ms }
		if r.ClaimP50Ms > maxCP50 { maxCP50 = r.ClaimP50Ms }

		if r.ClaimP99Ms < minCP99 { minCP99 = r.ClaimP99Ms }
		if r.ClaimP99Ms > maxCP99 { maxCP99 = r.ClaimP99Ms }

		if r.RecordP50Ms < minRP50 { minRP50 = r.RecordP50Ms }
		if r.RecordP50Ms > maxRP50 { maxRP50 = r.RecordP50Ms }

		if r.RecordP99Ms < minRP99 { minRP99 = r.RecordP99Ms }
		if r.RecordP99Ms > maxRP99 { maxRP99 = r.RecordP99Ms }

		if r.TotalP50Ms < minTP50 { minTP50 = r.TotalP50Ms }
		if r.TotalP50Ms > maxTP50 { maxTP50 = r.TotalP50Ms }

		if r.TotalP99Ms < minTP99 { minTP99 = r.TotalP99Ms }
		if r.TotalP99Ms > maxTP99 { maxTP99 = r.TotalP99Ms }

		if r.PoolAcquireWaitMs < minWait { minWait = r.PoolAcquireWaitMs }
		if r.PoolAcquireWaitMs > maxWait { maxWait = r.PoolAcquireWaitMs }

		if r.PoolEmptyAcquires < minEmpty { minEmpty = r.PoolEmptyAcquires }
		if r.PoolEmptyAcquires > maxEmpty { maxEmpty = r.PoolEmptyAcquires }

		if r.MaxActiveConns < minConn { minConn = r.MaxActiveConns }
		if r.MaxActiveConns > maxConn { maxConn = r.MaxActiveConns }

		if r.MaxLockWaits < minLock { minLock = r.MaxLockWaits }
		if r.MaxLockWaits > maxLock { maxLock = r.MaxLockWaits }

		if r.MaxPostgresCPU < minCPU { minCPU = r.MaxPostgresCPU }
		if r.MaxPostgresCPU > maxCPU { maxCPU = r.MaxPostgresCPU }

		if r.OutboxDrainSec < minDrain { minDrain = r.OutboxDrainSec }
		if r.OutboxDrainSec > maxDrain { maxDrain = r.OutboxDrainSec }

		if r.MaxOutboxLagSec < minLag { minLag = r.MaxOutboxLagSec }
		if r.MaxOutboxLagSec > maxLag { maxLag = r.MaxOutboxLagSec }
	}

	return RangeMetrics{
		Scenario:           runs[0].Scenario,
		Workers:            runs[0].Workers,
		NumLeads:           runs[0].NumLeads,
		ClaimsPerSecRange:  fmt.Sprintf("[%.1f - %.1f]", minC, maxC),
		RecordsPerSecRange: fmt.Sprintf("[%.1f - %.1f]", minR, maxR),
		ClaimP50Range:      fmt.Sprintf("[%.1f - %.1f]", minCP50, maxCP50),
		ClaimP99Range:      fmt.Sprintf("[%.1f - %.1f]", minCP99, maxCP99),
		RecordP50Range:     fmt.Sprintf("[%.1f - %.1f]", minRP50, maxRP50),
		RecordP99Range:     fmt.Sprintf("[%.1f - %.1f]", minRP99, maxRP99),
		TotalP50Range:      fmt.Sprintf("[%.1f - %.1f]", minTP50, maxTP50),
		TotalP99Range:      fmt.Sprintf("[%.1f - %.1f]", minTP99, maxTP99),
		PoolWaitRange:      fmt.Sprintf("[%.2f - %.2f]", minWait, maxWait),
		EmptyAcquiresRange: fmt.Sprintf("[%d - %d]", minEmpty, maxEmpty),
		ActiveConnsRange:   fmt.Sprintf("[%d - %d]", minConn, maxConn),
		LockWaitsRange:     fmt.Sprintf("[%d - %d]", minLock, maxLock),
		PostgresCPURange:   fmt.Sprintf("[%.1f%% - %.1f%%]", minCPU, maxCPU),
		OutboxDrainRange:   fmt.Sprintf("[%.2fs - %.2fs]", minDrain, maxDrain),
		MaxOutboxLagRange:  fmt.Sprintf("[%.2fs - %.2fs]", minLag, maxLag),
	}
}

func printRangeTable(results []RangeMetrics) {
	fmt.Println("\n==========================================================================================================================")
	fmt.Println("DIALPULSE BENCHMARK SUMMARY (Ranges across 3 runs) - Max Claim-and-Record Rate (No Dial Time)")
	fmt.Println("==========================================================================================================================")
	fmt.Printf("| %-4s | %-3s | %-15s | %-15s | %-15s | %-15s | %-15s | %-8s | %-8s |\n",
		"Scen", "W", "Throughput/s", "Tot p50 (ms)", "Tot p99 (ms)", "Claim p50 (ms)", "Record p50 (ms)", "PG CPU%", "Locks")
	fmt.Println("|------|-----|-----------------|-----------------|-----------------|-----------------|-----------------|----------|----------|")
	for _, m := range results {
		fmt.Printf("| %-4s | %-3d | %-15s | %-15s | %-15s | %-15s | %-15s | %-8s | %-8s |\n",
			m.Scenario, m.Workers, m.ClaimsPerSecRange, m.TotalP50Range, m.TotalP99Range, m.ClaimP50Range, m.RecordP50Range, m.PostgresCPURange, m.LockWaitsRange)
	}
	fmt.Println("==========================================================================================================================")
}

func printMetricsTable(results []StepMetrics) {
	fmt.Println("\n==========================================================================================")
	fmt.Println("DIALPULSE STEP RESULTS: Max Claim-and-Record Rate (No Dial Time)")
	fmt.Println("==========================================================================================")
	fmt.Printf("%-4s | %-3s | %-6s | %-10s | %-10s | %-10s | %-10s | %-8s | %-6s | %-6s\n",
		"Scen", "W", "Leads", "Claim/s", "Record/s", "Tot p50(ms)", "Tot p99(ms)", "PG CPU%", "Conns", "Locks")
	fmt.Println("------------------------------------------------------------------------------------------")
	for _, m := range results {
		fmt.Printf("%-4s | %-3d | %-6d | %-10.1f | %-10.1f | %-10.2f | %-10.2f | %-8.1f | %-6d | %-6d\n",
			m.Scenario, m.Workers, m.NumLeads, m.ClaimsPerSec, m.RecordsPerSec, m.TotalP50Ms, m.TotalP99Ms, m.MaxPostgresCPU, m.MaxActiveConns, m.MaxLockWaits)
	}
	fmt.Println("==========================================================================================")
}
