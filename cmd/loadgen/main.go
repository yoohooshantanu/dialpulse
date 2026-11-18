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

	"github.com/yoohooshantanu/predictive-dialer/internal/model"
	"github.com/yoohooshantanu/predictive-dialer/internal/queue"
	"github.com/yoohooshantanu/predictive-dialer/internal/storage"
)

type Config struct {
	Scenario          string // "A", "A_RELAY", "AB_NOOP", "AB"
	Workers           int
	NumLeads          int
	BatchSize         int
	DatabaseURL       string
	NATSURL           string
	CampaignID        string
	Tiny              bool
	Explain           bool
	QueryMode         string // "unified", "split", or "partial"
	Runs              int
	Ramp              bool
	DatasetExperiment bool
	SyncCommit        string // "on" or "off"
	PingBench         bool
	OneRoundTrip      bool
}

type StepMetrics struct {
	Scenario              string
	QueryMode             string
	Workers               int
	NumLeads              int
	DurationSec           float64
	ClaimsPerSec          float64
	RecordsPerSec         float64
	ClaimP50Ms            float64
	ClaimP95Ms            float64
	ClaimP99Ms            float64
	RecordP50Ms           float64
	RecordP95Ms           float64
	RecordP99Ms           float64
	TotalP50Ms            float64
	TotalP95Ms            float64
	TotalP99Ms            float64
	PoolAcquireWaitMs     float64
	PoolEmptyAcquires     int64
	MaxActiveConns        int
	MaxLockWaits          int
	LockWaitDetails       string
	MaxPostgresCPU        float64
	OutboxDrainSec        float64
	MaxOutboxLagSec       float64
	IndexUsed             string
	QueryMeanTimeMs       float64
	QueuedTimeline        []string
	OutcomeRetries        int64
	SyncCommit            string
	OneRoundTrip          bool
	DockerStatsAttempted  int64
	DockerStatsTaken      int64
	CgroupMeanCPU         float64
	CgroupNrThrottled     uint64
	CgroupThrottledTimeUs uint64
}

type RangeMetrics struct {
	Scenario              string
	QueryMode             string
	Workers               int
	NumLeads              int
	ClaimsPerSecRange     string
	RecordsPerSecRange    string
	ClaimP50Range         string
	ClaimP99Range         string
	RecordP50Range        string
	RecordP99Range        string
	TotalP50Range         string
	TotalP99Range         string
	PoolWaitRange         string
	EmptyAcquiresRange    string
	ActiveConnsRange      string
	LockWaitsRange        string
	PostgresCPURange      string
	OutboxDrainRange      string
	MaxOutboxLagRange     string
	RetriesRange          string
	CgroupCPURange        string
	CgroupThrottledRange  string
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Scenario, "scenario", "A", "Scenario: A, A_RELAY, AB_NOOP, AB")
	flag.IntVar(&cfg.Workers, "workers", 2, "Number of concurrent claiming workers")
	flag.IntVar(&cfg.NumLeads, "leads", 2000, "Number of leads to seed")
	flag.IntVar(&cfg.BatchSize, "batch-size", 50, "Batch size for claiming")
	flag.StringVar(&cfg.DatabaseURL, "db-url", "postgres://postgres:password@localhost:5432/predictive_dialer_test?sslmode=disable", "PostgreSQL connection URL")
	flag.StringVar(&cfg.NATSURL, "nats-url", "nats://localhost:4222", "NATS connection URL")
	flag.StringVar(&cfg.CampaignID, "campaign", "camp-load-1", "Campaign ID")
	flag.BoolVar(&cfg.Tiny, "tiny", false, "Run tiny self-check (500 leads)")
	flag.BoolVar(&cfg.Explain, "explain", false, "Run EXPLAIN ANALYZE index experiment")
	flag.StringVar(&cfg.QueryMode, "query-mode", "split", "Query mode: unified (idx_leads_dispatch only), split (composite index), or partial (WHERE status='QUEUED')")
	flag.IntVar(&cfg.Runs, "runs", 3, "Number of runs per step (for range reporting)")
	flag.BoolVar(&cfg.Ramp, "ramp", false, "Run full ramp across W=1,2,4,8,16,32")
	flag.BoolVar(&cfg.DatasetExperiment, "dataset-experiment", false, "Run 1M leads dataset experiment")
	flag.StringVar(&cfg.SyncCommit, "sync-commit", "on", "synchronous_commit: on or off")
	flag.BoolVar(&cfg.PingBench, "ping-bench", false, "Run 1000-sample SELECT 1 ping round-trip benchmark")
	flag.BoolVar(&cfg.OneRoundTrip, "one-round-trip", false, "Use single-round-trip CTE transaction for record path")
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

	if cfg.PingBench {
		runPingBench(ctx, pool)
		return
	}

	// Apply synchronous_commit setting
	_, _ = pool.Exec(ctx, fmt.Sprintf("ALTER SYSTEM SET synchronous_commit = '%s';", cfg.SyncCommit))
	_, _ = pool.Exec(ctx, "SELECT pg_reload_conf();")

	repo := storage.NewPostgresRepository(pool)

	if cfg.Explain {
		runIndexExperiment(ctx, pool, cfg)
		return
	}

	if cfg.DatasetExperiment {
		run1MDatasetExperiment(ctx, pool, repo, cfg)
		return
	}

	if cfg.Ramp {
		runFullRamp(ctx, pool, repo, cfg)
		return
	}

	// Single step multi-run
	var stepRuns []StepMetrics
	for r := 1; r <= cfg.Runs; r++ {
		fmt.Printf("\n--- Run %d of %d (Scenario %s, W=%d, Leads=%d, Mode=%s) ---\n",
			r, cfg.Runs, cfg.Scenario, cfg.Workers, cfg.NumLeads, cfg.QueryMode)
		res, err := runBenchmark(ctx, pool, repo, cfg)
		if err != nil {
			log.Fatalf("Benchmark run %d failed: %v", r, err)
		}
		stepRuns = append(stepRuns, res)
	}

	rMetric := summarizeRanges(stepRuns)
	printRangeTable([]RangeMetrics{rMetric}, cfg.QueryMode)
}

func runPingBench(ctx context.Context, pool *pgxpool.Pool) {
	fmt.Println("\n=======================================================")
	fmt.Println("BENCHMARK: SELECT 1 Round-Trip Latency (1,000 samples)")
	fmt.Println("=======================================================")
	samples := make([]float64, 1000)
	var sum float64
	for i := 0; i < 1000; i++ {
		start := time.Now()
		var one int
		if err := pool.QueryRow(ctx, "SELECT 1;").Scan(&one); err != nil {
			log.Fatalf("SELECT 1 failed: %v", err)
		}
		durMs := float64(time.Since(start).Microseconds()) / 1000.0
		samples[i] = durMs
		sum += durMs
	}
	sort.Float64s(samples)
	fmt.Printf("Samples: %d | Mean: %.2f ms | Min: %.2f ms | p50: %.2f ms | p90: %.2f ms | p95: %.2f ms | p99: %.2f ms | Max: %.2f ms\n",
		len(samples), sum/float64(len(samples)), samples[0],
		percentile(samples, 0.50), percentile(samples, 0.90), percentile(samples, 0.95), percentile(samples, 0.99), samples[len(samples)-1])
	fmt.Println("=======================================================")
}

func runFullRamp(ctx context.Context, pool *pgxpool.Pool, repo *storage.PostgresRepository, baseCfg Config) {
	workerCounts := []int{1, 2, 4, 8, 16, 32}
	var allRangeMetrics []RangeMetrics

	fmt.Printf("\n=================================================================\n")
	fmt.Printf("STARTING FULL RAMP: Scenario %s across W=%v (%d runs each, Mode=%s, sync_commit=%s)\n",
		baseCfg.Scenario, workerCounts, baseCfg.Runs, baseCfg.QueryMode, baseCfg.SyncCommit)
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
			time.Sleep(500 * time.Millisecond)
		}

		rm := summarizeRanges(runs)
		allRangeMetrics = append(allRangeMetrics, rm)
	}

	printRangeTable(allRangeMetrics, baseCfg.QueryMode)
}

func run1MDatasetExperiment(ctx context.Context, pool *pgxpool.Pool, repo *storage.PostgresRepository, cfg Config) {
	fmt.Println("=================================================================")
	fmt.Println("DATASET EXPERIMENT: 1,000,000 LEADS INDEX COMPARISON AT SCALE")
	fmt.Println("=================================================================")

	var queuedCount int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM leads WHERE campaign_id = $1 AND status = 'QUEUED'", cfg.CampaignID).Scan(&queuedCount)

	if queuedCount >= 500000 {
		fmt.Printf("✔ Table already contains %d queued leads in campaign %s; skipping re-seeding.\n", queuedCount, cfg.CampaignID)
	} else {
		// Reset tables
		resetTables(ctx, pool)
		ensureCampaign(ctx, pool, cfg.CampaignID)

		// Seed 1,000,000 leads
		fmt.Println("Seeding 1,000,000 leads (batch size 10,000)...")
		seedStart := time.Now()
		totalLeads := 1000000
		batchSize := 10000
		cols := []string{"id", "campaign_id", "phone_number", "name", "priority", "status", "attempts", "max_attempts", "created_at"}
		r := rand.New(rand.NewSource(42))

		for inserted := 0; inserted < totalLeads; inserted += batchSize {
			rows := make([][]interface{}, batchSize)
			for i := 0; i < batchSize; i++ {
				idx := inserted + i
				// Note: time.Now().UTC() is called per row inside the loop. Due to fast CPU clock resolution (~1-100us per call),
				// 1M leads produce ~5,652 distinct timestamps across the 100 CopyFrom batches (~177 leads per distinct timestamp tick).
				rows[i] = []interface{}{
					fmt.Sprintf("lead-1m-%07d", idx),
					cfg.CampaignID,
					fmt.Sprintf("+1800%07d", idx),
					fmt.Sprintf("Lead 1M %d", idx),
					r.Intn(10),
					"QUEUED",
					0,
					3,
					time.Now().UTC(),
				}
			}
			_, err := pool.CopyFrom(ctx, pgx.Identifier{"leads"}, cols, pgx.CopyFromRows(rows))
			if err != nil {
				log.Fatalf("CopyFrom error: %v", err)
			}
			if (inserted+batchSize)%200000 == 0 {
				fmt.Printf("  Seeded %d / %d leads (%.1fs elapsed)\n", inserted+batchSize, totalLeads, time.Since(seedStart).Seconds())
			}
		}
		fmt.Printf("✔ 1,000,000 leads seeded in %.2fs (%.0f leads/s)\n", time.Since(seedStart).Seconds(), float64(totalLeads)/time.Since(seedStart).Seconds())
	}

	_, _ = pool.Exec(ctx, "ANALYZE leads;")

	modes := []string{"unified", "split", "partial"}
	workerLevels := []int{8, 32}
	runsPerStep := 3

	type ExpResult struct {
		Mode       string
		Workers    int
		Throughput string
		ClaimP50   string
		RecordP50  string
		TotalP50   string
	}
	var summaryResults []ExpResult

	totalDrainedSoFar := 0

	for _, mode := range modes {
		// Set up indexes for this mode
		if mode == "unified" {
			_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS idx_leads_comp; DROP INDEX IF EXISTS idx_leads_queued; DROP INDEX IF EXISTS idx_leads_partial_queued;`)
		} else if mode == "split" {
			_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS idx_leads_partial_queued; CREATE INDEX IF NOT EXISTS idx_leads_comp ON leads (campaign_id, priority DESC, created_at ASC);`)
		} else if mode == "partial" {
			_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS idx_leads_comp; CREATE INDEX IF NOT EXISTS idx_leads_partial_queued ON leads (campaign_id, priority DESC, created_at ASC) WHERE status = 'QUEUED';`)
		}
		_, _ = pool.Exec(ctx, "ANALYZE leads;")

		leadsPerRun := 1500
		if mode == "unified" {
			leadsPerRun = 100 // 2 batches of 50 leads (each batch takes ~7-9s to sort 979k rows)
		}

		for _, workers := range workerLevels {
			var tpList, cp50List, rp50List, totp50List []float64
			var cMeanList, rMeanList, totMeanList []float64

			for run := 1; run <= runsPerStep; run++ {
				fmt.Printf("\n>>> Running 1M Drain: Mode=%s, W=%d (Run %d/%d, Draining %d leads)...\n", mode, workers, run, runsPerStep, leadsPerRun)
				tp, cp50, rp50, totp50, cMean, rMean, totMean := run1MDrainStep(ctx, pool, repo, cfg.CampaignID, mode, workers, leadsPerRun, cfg.BatchSize)
				tpList = append(tpList, tp)
				cp50List = append(cp50List, cp50)
				rp50List = append(rp50List, rp50)
				totp50List = append(totp50List, totp50)
				cMeanList = append(cMeanList, cMean)
				rMeanList = append(rMeanList, rMean)
				totMeanList = append(totMeanList, totMean)
				totalDrainedSoFar += leadsPerRun
				fmt.Printf("    Run %d: %.1f leads/s | Tot p50: %.1fms (mean: %.1fms) | Claim p50: %.1fms (mean: %.1fms) | Record p50: %.1fms (mean: %.1fms) (Total Drained: %d)\n",
					run, tp, totp50, totMean, cp50, cMean, rp50, rMean, totalDrainedSoFar)
			}

			minTp, maxTp := minMax(tpList)
			minC, maxC := minMax(cp50List)
			minR, maxR := minMax(rp50List)
			minTot, maxTot := minMax(totp50List)

			summaryResults = append(summaryResults, ExpResult{
				Mode:       mode,
				Workers:    workers,
				Throughput: fmt.Sprintf("[%.1f - %.1f]", minTp, maxTp),
				ClaimP50:   fmt.Sprintf("[%.1f - %.1f]", minC, maxC),
				RecordP50:  fmt.Sprintf("[%.1f - %.1f]", minR, maxR),
				TotalP50:   fmt.Sprintf("[%.1f - %.1f]", minTot, maxTot),
			})
		}
	}

	// Print summary table
	fmt.Printf("\n===================================================================================================\n")
	fmt.Printf("1,000,000 LEADS BENCHMARK SUMMARY (Total Drained: %d leads) - Max Claim-and-Record Rate (No Dial Time)\n", totalDrainedSoFar)
	fmt.Printf("===================================================================================================\n")
	fmt.Printf("| %-10s | %-3s | %-15s | %-15s | %-15s | %-15s |\n", "Mode", "W", "Throughput/s", "Tot p50 (ms)", "Claim p50 (ms)", "Record p50 (ms)")
	fmt.Println("|------------|-----|-----------------|-----------------|-----------------|-----------------|")
	for _, res := range summaryResults {
		fmt.Printf("| %-10s | %-3d | %-15s | %-15s | %-15s | %-15s |\n",
			res.Mode, res.Workers, res.Throughput, res.TotalP50, res.ClaimP50, res.RecordP50)
	}
	fmt.Printf("===================================================================================================\n")

	// Now run EXPLAIN (ANALYZE, BUFFERS) on the partly drained table (>20,000 leads drained!)
	fmt.Printf("\n=================================================================\n")
	fmt.Printf("EXPLAIN (ANALYZE, BUFFERS) ON PARTLY DRAINED TABLE (%d Leads Processed)\n", totalDrainedSoFar)
	fmt.Printf("=================================================================\n")
	limit := 50
	intervalStr := fmt.Sprintf("%f seconds", model.DefaultLeaseTTL.Seconds())

	// 1. Unified Query with Migration 001 idx_leads_dispatch
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS idx_leads_comp; DROP INDEX IF EXISTS idx_leads_partial_queued;`)
	_, _ = pool.Exec(ctx, "ANALYZE leads;")
	fmt.Println("\n--- [Partly Drained] Unified Query with idx_leads_dispatch ---")
	runExplain(ctx, pool, unifiedQuery, "Unified Query (Migration 001)", cfg.CampaignID, limit, intervalStr)

	// 2. Split Query with Composite Index idx_leads_comp
	_, _ = pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_leads_comp ON leads (campaign_id, priority DESC, created_at ASC);`)
	_, _ = pool.Exec(ctx, "ANALYZE leads;")
	fmt.Println("\n--- [Partly Drained] Split Fast Path with idx_leads_comp (Composite Index) ---")
	runExplain(ctx, pool, fastPathQuery, "Split Fast Path (Composite Index)", cfg.CampaignID, limit, intervalStr)

	// 3. Split Query with Partial Index idx_leads_partial_queued
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS idx_leads_comp; CREATE INDEX IF NOT EXISTS idx_leads_partial_queued ON leads (campaign_id, priority DESC, created_at ASC) WHERE status = 'QUEUED';`)
	_, _ = pool.Exec(ctx, "ANALYZE leads;")
	fmt.Println("\n--- [Partly Drained] Split Fast Path with idx_leads_partial_queued (Partial Index) ---")
	runExplain(ctx, pool, fastPathQuery, "Split Fast Path (Partial Index WHERE status='QUEUED')", cfg.CampaignID, limit, intervalStr)
}

func run1MDrainStep(ctx context.Context, pool *pgxpool.Pool, repo *storage.PostgresRepository, campaignID string, mode string, workers int, numLeads int, batchSize int) (float64, float64, float64, float64, float64, float64, float64) {
	stepStart := time.Now()
	var claimed atomic.Int64
	var wg sync.WaitGroup

	var claimLats, recLats, totLats []float64
	var latMu sync.Mutex

	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerID := fmt.Sprintf("worker-1m-%s-%d-%d", mode, workers, w)

		go func(wID string) {
			defer wg.Done()
			for claimed.Load() < int64(numLeads) {
				startClaim := time.Now()
				var leads []*model.Lead
				var err error

				if mode == "split" {
					leads, err = claimSplitQuery(ctx, pool, campaignID, batchSize, wID)
				} else if mode == "partial" {
					leads, err = claimPartialQuery(ctx, pool, campaignID, batchSize, wID)
				} else {
					leads, err = repo.ClaimPendingLeads(ctx, campaignID, batchSize, wID)
				}
				claimDurMs := float64(time.Since(startClaim).Microseconds()) / 1000.0

				if err != nil || len(leads) == 0 {
					time.Sleep(5 * time.Millisecond)
					continue
				}

				perLeadClaimMs := claimDurMs / float64(len(leads))

				for _, lead := range leads {
					if claimed.Add(1) > int64(numLeads) {
						break
					}

					startRecord := time.Now()
					attemptID := uuid.New().String()
					_ = repo.RecordAttemptStart(ctx, lead.ID, attemptID, lead.Attempts+1, lead.LeaseToken)
					attempt := &model.CallAttempt{
						ID:             attemptID,
						CampaignID:     campaignID,
						LeadID:         lead.ID,
						PhoneNumber:    lead.PhoneNumber,
						Status:         model.StatusCompleted,
						RingDurationMs: 0,
						TalkDurationMs: 0,
						CreatedAt:      time.Now().UTC(),
					}
					_ = repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusCompleted, "answered", nil, attempt, lead.Attempts+1, lead.LeaseToken)
					recDurMs := float64(time.Since(startRecord).Microseconds()) / 1000.0

					latMu.Lock()
					claimLats = append(claimLats, perLeadClaimMs)
					recLats = append(recLats, recDurMs)
					totLats = append(totLats, perLeadClaimMs+recDurMs)
					latMu.Unlock()
				}
			}
		}(workerID)
	}

	wg.Wait()
	dur := time.Since(stepStart).Seconds()
	nClaimed := float64(claimed.Load())
	tp := nClaimed / dur

	sort.Float64s(claimLats)
	sort.Float64s(recLats)
	sort.Float64s(totLats)

	cp50 := percentile(claimLats, 0.50)
	rp50 := percentile(recLats, 0.50)
	totp50 := percentile(totLats, 0.50)

	calcMean := func(vals []float64) float64 {
		if len(vals) == 0 {
			return 0
		}
		var s float64
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals))
	}

	return tp, cp50, rp50, totp50, calcMean(claimLats), calcMean(recLats), calcMean(totLats)
}

func minMax(vals []float64) (float64, float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	minVal, maxVal := vals[0], vals[0]
	for _, v := range vals[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	return minVal, maxVal
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

// NoOpConsumerHandler implements queue.ConsumerTxHandler without hot-row campaign_outcomes updates
type NoOpConsumerHandler struct{}

func (h *NoOpConsumerHandler) HandleEventTx(ctx context.Context, tx pgx.Tx, event *model.OutboxEvent) error {
	return nil
}

func runBenchmark(ctx context.Context, pool *pgxpool.Pool, repo *storage.PostgresRepository, cfg Config) (StepMetrics, error) {
	// Setup index configuration based on QueryMode
	if cfg.QueryMode == "unified" {
		_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS idx_leads_comp; DROP INDEX IF EXISTS idx_leads_queued; DROP INDEX IF EXISTS idx_leads_partial_queued;`)
	} else if cfg.QueryMode == "partial" {
		_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS idx_leads_comp; DROP INDEX IF EXISTS idx_leads_queued; CREATE INDEX IF NOT EXISTS idx_leads_partial_queued ON leads (campaign_id, priority DESC, created_at ASC) WHERE status = 'QUEUED';`)
	} else {
		_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS idx_leads_partial_queued; CREATE INDEX IF NOT EXISTS idx_leads_comp ON leads (campaign_id, priority DESC, created_at ASC);`)
	}

	resetTables(ctx, pool)
	ensureCampaign(ctx, pool, cfg.CampaignID)
	seedLeads(ctx, pool, cfg.CampaignID, cfg.NumLeads)

	_, _ = pool.Exec(ctx, "SELECT pg_stat_statements_reset();")

	cgroupStartUsage, _ := readCgroupUsage("predictive-dialer-postgres")
	throttleStart, throttledTimeStart, _ := readCgroupThrottle("predictive-dialer-postgres")

	var nc *nats.Conn
	var js nats.JetStreamContext
	var relay *queue.OutboxRelay
	var consumer *queue.OutboxConsumer
	var streamName = "DIALPULSE_EVENTS"
	var consumerName = "predictive_dialer_outcome_worker"

	needsNATS := cfg.Scenario == "A_RELAY" || cfg.Scenario == "AB_NOOP" || cfg.Scenario == "AB"

	if needsNATS {
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
			Subjects:   []string{"predictive_dialer.events.>", "predictive_dialer.dlq.>"},
			Storage:    nats.MemoryStorage,
			Duplicates: 30 * time.Second,
			MaxAge:     5 * time.Minute,
		})
		if err != nil {
			return StepMetrics{}, fmt.Errorf("add stream: %w", err)
		}

		// Start OutboxRelay
		relayCfg := queue.DefaultOutboxRelayConfig()
		relayCfg.BatchSize = 50
		relayCfg.PollInterval = 10 * time.Millisecond
		relayPublisher := queue.NewNATSJetStreamPublisher(js)
		relay = queue.NewOutboxRelay(pool, relayPublisher, relayCfg)
		relay.Start(ctx)

		// Start OutboxConsumer if AB or AB_NOOP
		if cfg.Scenario == "AB" || cfg.Scenario == "AB_NOOP" {
			consumerCfg := queue.DefaultOutboxConsumerConfig()
			consumerCfg.ConsumerName = consumerName
			consumerCfg.StreamName = streamName
			consumerCfg.SubjectFilter = "predictive_dialer.events.>"

			var consumerHandler queue.ConsumerTxHandler
			if cfg.Scenario == "AB_NOOP" {
				consumerHandler = &NoOpConsumerHandler{}
			} else {
				consumerHandler = queue.NewCampaignOutcomeAggregator()
			}

			consumer = queue.NewOutboxConsumer(pool, js, consumerHandler, consumerCfg)
			if err := consumer.StartPullConsumer(ctx); err != nil {
				return StepMetrics{}, fmt.Errorf("start consumer: %w", err)
			}
		}
	}

	stopMonitor := make(chan struct{})
	var maxActiveConns int32
	var maxLockWaits int32
	var lockWaitDetails sync.Map
	var maxCPU float64
	var maxOutboxLag float64
	var queuedCountTimeline []string
	var monitorWg sync.WaitGroup
	var dockerStatsAttempted atomic.Int64
	var dockerStatsTaken atomic.Int64

	monitorWg.Add(1)
	go func() {
		defer monitorWg.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		var tickCount int
		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				tickCount++
				var activeConns, lockWaits int
				_ = pool.QueryRow(ctx, `
					SELECT 
						COUNT(*) FILTER (WHERE state = 'active'),
						COUNT(*) FILTER (WHERE wait_event_type = 'Lock')
					FROM pg_stat_activity 
					WHERE datname = 'predictive_dialer_test';
				`).Scan(&activeConns, &lockWaits)

				if int32(activeConns) > atomic.LoadInt32(&maxActiveConns) {
					atomic.StoreInt32(&maxActiveConns, int32(activeConns))
				}
				if int32(lockWaits) > atomic.LoadInt32(&maxLockWaits) {
					atomic.StoreInt32(&maxLockWaits, int32(lockWaits))
				}

				// Scrape exact wait_event_type / wait_event names
				rows, err := pool.Query(ctx, `
					SELECT wait_event_type, wait_event, COUNT(*) 
					FROM pg_stat_activity 
					WHERE wait_event_type IS NOT NULL AND datname = 'predictive_dialer_test'
					GROUP BY wait_event_type, wait_event;
				`)
				if err == nil {
					for rows.Next() {
						var wet, we string
						var cnt int
						if err := rows.Scan(&wet, &we, &cnt); err == nil {
							key := fmt.Sprintf("%s:%s", wet, we)
							lockWaitDetails.Store(key, cnt)
						}
					}
					rows.Close()
				}

				var queuedCount int64
				_ = pool.QueryRow(ctx, `SELECT count(*) FROM leads WHERE campaign_id = $1 AND status = 'QUEUED';`, cfg.CampaignID).Scan(&queuedCount)
				queuedCountTimeline = append(queuedCountTimeline, fmt.Sprintf("%d", queuedCount))

				if needsNATS {
					lag, _ := repo.GetOutboxLagSeconds(ctx)
					if lag > maxOutboxLag {
						maxOutboxLag = lag
					}
				}

				if tickCount%5 == 0 {
					dockerStatsAttempted.Add(1)
					cpu, ok := sampleDockerCPUCounted("predictive-dialer-postgres")
					if ok {
						dockerStatsTaken.Add(1)
						if cpu > maxCPU {
							maxCPU = cpu
						}
					}
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

	initialRetries := repo.OutcomeTxRetries()

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
				} else if cfg.QueryMode == "partial" {
					leads, err = claimPartialQuery(ctx, pool, cfg.CampaignID, cfg.BatchSize, wID)
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

					if cfg.OneRoundTrip {
						if err := recordOneRoundTrip(ctx, pool, cfg.CampaignID, lead.ID, lead.LeaseToken, attemptID, lead.PhoneNumber, lead.Attempts+1); err != nil {
							log.Fatalf("recordOneRoundTrip failed: %v", err)
						}
					} else {
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

	if cfg.Scenario == "AB" || cfg.Scenario == "AB_NOOP" {
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
	} else if cfg.Scenario == "A_RELAY" {
		for {
			var unpublished int64
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL;`).Scan(&unpublished)
			if unpublished == 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		outboxDrainDuration = time.Since(outboxStart)
		relay.Stop()
	}

	close(stopMonitor)
	monitorWg.Wait()

	cgroupEndUsage, _ := readCgroupUsage("predictive-dialer-postgres")
	throttleEnd, throttledTimeEnd, _ := readCgroupThrottle("predictive-dialer-postgres")
	var cgroupMeanCPU float64
	if benchDuration.Nanoseconds() > 0 && cgroupEndUsage > cgroupStartUsage {
		cgroupMeanCPU = (float64(cgroupEndUsage-cgroupStartUsage) / float64(benchDuration.Nanoseconds())) * 100.0
	}
	nrThrottled := throttleEnd - throttleStart
	var throttledTimeUs uint64
	if throttledTimeEnd > throttledTimeStart {
		throttledTimeUs = throttledTimeEnd - throttledTimeStart
	}

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

	var waitEvents []string
	lockWaitDetails.Range(func(key, val interface{}) bool {
		waitEvents = append(waitEvents, fmt.Sprintf("%v(%v)", key, val))
		return true
	})

	retriesThisRun := repo.OutcomeTxRetries() - initialRetries

	metrics := StepMetrics{
		Scenario:              cfg.Scenario,
		QueryMode:             cfg.QueryMode,
		Workers:               cfg.Workers,
		NumLeads:              cfg.NumLeads,
		DurationSec:           benchDuration.Seconds(),
		ClaimsPerSec:          float64(totalClaimed.Load()) / benchDuration.Seconds(),
		RecordsPerSec:         float64(totalRecorded.Load()) / benchDuration.Seconds(),
		ClaimP50Ms:            claimP50,
		ClaimP95Ms:            claimP95,
		ClaimP99Ms:            claimP99,
		RecordP50Ms:           recP50,
		RecordP95Ms:           recP95,
		RecordP99Ms:           recP99,
		TotalP50Ms:            totP50,
		TotalP95Ms:            totP95,
		TotalP99Ms:            totP99,
		PoolAcquireWaitMs:     float64(poolStat.AcquireDuration().Microseconds()) / 1000.0,
		PoolEmptyAcquires:     poolStat.EmptyAcquireCount(),
		MaxActiveConns:        int(atomic.LoadInt32(&maxActiveConns)),
		MaxLockWaits:          int(atomic.LoadInt32(&maxLockWaits)),
		LockWaitDetails:       strings.Join(waitEvents, ", "),
		MaxPostgresCPU:        maxCPU,
		OutboxDrainSec:        outboxDrainDuration.Seconds(),
		MaxOutboxLagSec:       maxOutboxLag,
		QueryMeanTimeMs:       queryMeanTime,
		QueuedTimeline:        queuedCountTimeline,
		OutcomeRetries:        retriesThisRun,
		SyncCommit:            cfg.SyncCommit,
		OneRoundTrip:          cfg.OneRoundTrip,
		DockerStatsAttempted:  dockerStatsAttempted.Load(),
		DockerStatsTaken:      dockerStatsTaken.Load(),
		CgroupMeanCPU:         cgroupMeanCPU,
		CgroupNrThrottled:     nrThrottled,
		CgroupThrottledTimeUs: throttledTimeUs,
	}

	// Print pg_stat_statements top statements
	printTopStatements(ctx, pool)

	// Print dead tuples
	printDeadTuples(ctx, pool)

	return metrics, nil
}

func printTopStatements(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `
		SELECT query, calls, round(mean_exec_time::numeric, 2), round(total_exec_time::numeric, 2), rows
		FROM pg_stat_statements
		ORDER BY total_exec_time DESC
		LIMIT 5;
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	fmt.Println("\n  [pg_stat_statements Top 5 by Total Execution Time]:")
	fmt.Printf("  %-8s | %-12s | %-12s | %-6s | %s\n", "Calls", "Mean (ms)", "Total (ms)", "Rows", "Query Prefix")
	fmt.Println("  ------------------------------------------------------------------------------------------------")
	for rows.Next() {
		var q string
		var calls, rowsCount int64
		var meanMs, totalMs float64
		if err := rows.Scan(&q, &calls, &meanMs, &totalMs, &rowsCount); err == nil {
			cleanQ := strings.ReplaceAll(strings.TrimSpace(q), "\n", " ")
			cleanQ = strings.Join(strings.Fields(cleanQ), " ")
			if len(cleanQ) > 55 {
				cleanQ = cleanQ[:55] + "..."
			}
			fmt.Printf("  %-8d | %-12.2f | %-12.2f | %-6d | %s\n", calls, meanMs, totalMs, rowsCount, cleanQ)
		}
	}
}

func printDeadTuples(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `
		SELECT relname, n_live_tup, n_dead_tup, autovacuum_count
		FROM pg_stat_user_tables
		WHERE schemaname = 'public' AND relname IN ('leads', 'call_attempts', 'outbox_events', 'campaign_outcomes', 'processed_events')
		ORDER BY relname;
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	fmt.Println("  [pg_stat_user_tables Dead Tuples & Autovacuum Count]:")
	for rows.Next() {
		var table string
		var live, dead, avCnt int64
		if err := rows.Scan(&table, &live, &dead, &avCnt); err == nil {
			fmt.Printf("    %-18s: live=%-6d dead=%-6d autovacuums=%d\n", table, live, dead, avCnt)
		}
	}
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

func sampleDockerCPUCounted(container string) (float64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", container)
	out, err := cmd.Output()
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(out))
	s = strings.TrimSuffix(s, "%")
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func readCgroupUsage(container string) (uint64, error) {
	cmd := exec.Command("docker", "exec", container, "cat", "/sys/fs/cgroup/cpuacct/cpuacct.usage")
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	return val, err
}

func readCgroupThrottle(container string) (nrThrottled uint64, throttledTimeUs uint64, err error) {
	cmd := exec.Command("docker", "exec", container, "cat", "/sys/fs/cgroup/cpu/cpu.stat")
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}
	lines := strings.Split(string(out), "\n")
	for _, l := range lines {
		fields := strings.Fields(l)
		if len(fields) == 2 {
			if fields[0] == "nr_throttled" {
				nrThrottled, _ = strconv.ParseUint(fields[1], 10, 64)
			} else if fields[0] == "throttled_time" {
				tNs, _ := strconv.ParseUint(fields[1], 10, 64)
				throttledTimeUs = tNs / 1000
			}
		}
	}
	return nrThrottled, throttledTimeUs, nil
}

func recordOneRoundTrip(ctx context.Context, pool *pgxpool.Pool, campaignID, leadID, leaseToken, attemptID, phone string, attemptNum int) error {
	payloadJSON := fmt.Sprintf(`{"lead_id":"%s","status":"CONNECTED","attempt":%d}`, leadID, attemptNum)
	query := `
		WITH locked_lead AS (
			SELECT id, campaign_id, phone_number, attempts
			FROM leads
			WHERE id = $1 AND lease_token = $2
			FOR UPDATE
		),
		upd_lead AS (
			UPDATE leads
			SET status = 'CONNECTED',
			    attempts = attempts + 1,
			    locked_by = NULL,
			    locked_at = NULL,
			    lease_token = NULL,
			    last_attempt_at = clock_timestamp()
			WHERE id IN (SELECT id FROM locked_lead)
			RETURNING id, campaign_id, phone_number, attempts
		),
		ins_attempt AS (
			INSERT INTO call_attempts (
				id, campaign_id, lead_id, phone_number, status, ring_duration_ms, talk_duration_ms, created_at
			)
			SELECT $3, u.campaign_id, u.id, u.phone_number, 'COMPLETED', 0, 0, clock_timestamp()
			FROM upd_lead u
		)
		INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at)
		SELECT $4, 'call', u.id, 'call.outcome_recorded', $5::jsonb, clock_timestamp()
		FROM upd_lead u;
	`
	tag, err := pool.Exec(ctx, query, leadID, leaseToken, attemptID, uuid.New().String(), payloadJSON)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("stale lease token or lead not found for %s", leadID)
	}
	return nil
}

func claimPartialQuery(ctx context.Context, pool *pgxpool.Pool, campaignID string, limit int, workerID string) ([]*model.Lead, error) {
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
	minRet, maxRet := runs[0].OutcomeRetries, runs[0].OutcomeRetries
	minCgCPU, maxCgCPU := runs[0].CgroupMeanCPU, runs[0].CgroupMeanCPU
	minThrot, maxThrot := runs[0].CgroupNrThrottled, runs[0].CgroupNrThrottled

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

		if r.OutcomeRetries < minRet { minRet = r.OutcomeRetries }
		if r.OutcomeRetries > maxRet { maxRet = r.OutcomeRetries }

		if r.CgroupMeanCPU < minCgCPU { minCgCPU = r.CgroupMeanCPU }
		if r.CgroupMeanCPU > maxCgCPU { maxCgCPU = r.CgroupMeanCPU }

		if r.CgroupNrThrottled < minThrot { minThrot = r.CgroupNrThrottled }
		if r.CgroupNrThrottled > maxThrot { maxThrot = r.CgroupNrThrottled }
	}

	return RangeMetrics{
		Scenario:             runs[0].Scenario,
		QueryMode:            runs[0].QueryMode,
		Workers:              runs[0].Workers,
		NumLeads:             runs[0].NumLeads,
		ClaimsPerSecRange:    fmt.Sprintf("[%.1f - %.1f]", minC, maxC),
		RecordsPerSecRange:   fmt.Sprintf("[%.1f - %.1f]", minR, maxR),
		ClaimP50Range:        fmt.Sprintf("[%.1f - %.1f]", minCP50, maxCP50),
		ClaimP99Range:        fmt.Sprintf("[%.1f - %.1f]", minCP99, maxCP99),
		RecordP50Range:       fmt.Sprintf("[%.1f - %.1f]", minRP50, maxRP50),
		RecordP99Range:       fmt.Sprintf("[%.1f - %.1f]", minRP99, maxRP99),
		TotalP50Range:        fmt.Sprintf("[%.1f - %.1f]", minTP50, maxTP50),
		TotalP99Range:        fmt.Sprintf("[%.1f - %.1f]", minTP99, maxTP99),
		PoolWaitRange:        fmt.Sprintf("[%.2f - %.2f]", minWait, maxWait),
		EmptyAcquiresRange:   fmt.Sprintf("[%d - %d]", minEmpty, maxEmpty),
		ActiveConnsRange:     fmt.Sprintf("[%d - %d]", minConn, maxConn),
		LockWaitsRange:       fmt.Sprintf("[%d - %d]", minLock, maxLock),
		PostgresCPURange:     fmt.Sprintf("[%.1f%% - %.1f%%]", minCPU, maxCPU),
		OutboxDrainRange:     fmt.Sprintf("[%.2fs - %.2fs]", minDrain, maxDrain),
		MaxOutboxLagRange:    fmt.Sprintf("[%.2fs - %.2fs]", minLag, maxLag),
		RetriesRange:         fmt.Sprintf("[%d - %d]", minRet, maxRet),
		CgroupCPURange:       fmt.Sprintf("[%.1f%% - %.1f%%]", minCgCPU, maxCgCPU),
		CgroupThrottledRange: fmt.Sprintf("[%d - %d]", minThrot, maxThrot),
	}
}

func printRangeTable(results []RangeMetrics, mode string) {
	fmt.Printf("\n=======================================================================================================================================\n")
	fmt.Printf("DIALPULSE BENCHMARK SUMMARY (Mode: %s) - Max Claim-and-Record Rate (No Dial Time)\n", mode)
	fmt.Printf("=======================================================================================================================================\n")
	fmt.Printf("| %-6s | %-7s | %-3s | %-15s | %-15s | %-15s | %-15s | %-15s | %-10s | %-10s | %-6s | %-8s |\n",
		"Scen", "Mode", "W", "Throughput/s", "Tot p50 (ms)", "Tot p99 (ms)", "Claim p50 (ms)", "Record p50 (ms)", "PG CPU%", "Cgroup CPU", "Locks", "Retries")
	fmt.Println("|--------|---------|-----|-----------------|-----------------|-----------------|-----------------|-----------------|------------|------------|--------|----------|")
	for _, m := range results {
		fmt.Printf("| %-6s | %-7s | %-3d | %-15s | %-15s | %-15s | %-15s | %-15s | %-10s | %-10s | %-6s | %-8s |\n",
			m.Scenario, m.QueryMode, m.Workers, m.ClaimsPerSecRange, m.TotalP50Range, m.TotalP99Range, m.ClaimP50Range, m.RecordP50Range, m.PostgresCPURange, m.CgroupCPURange, m.LockWaitsRange, m.RetriesRange)
	}
	fmt.Println("=======================================================================================================================================")
}
