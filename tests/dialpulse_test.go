package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yoohooshantanu/dialpulse/internal/dnc"
	"github.com/yoohooshantanu/dialpulse/internal/model"
	"github.com/yoohooshantanu/dialpulse/internal/pacer"
	"github.com/yoohooshantanu/dialpulse/internal/queue"
	"github.com/yoohooshantanu/dialpulse/internal/storage"
	"github.com/yoohooshantanu/dialpulse/internal/timezone"
)

func TestDNCBloomFilter(t *testing.T) {
	bf := dnc.NewBloomFilter(10000, 0.001)

	testNumbers := []string{
		"+1 (212) 555-0199",
		"13125550144",
		"4155550188",
		"(206) 555-0122",
	}

	for _, num := range testNumbers {
		bf.Add(num)
	}

	if bf.Count() != uint64(len(testNumbers)) {
		t.Fatalf("expected count %d, got %d", len(testNumbers), bf.Count())
	}

	// Verify all inserted numbers match regardless of format
	for _, num := range testNumbers {
		if !bf.Contains(num) {
			t.Errorf("expected DNC filter to contain %s", num)
		}
	}

	// Verify unlisted numbers return false
	unlisted := []string{"2125559999", "3125558888", "4155557777"}
	for _, num := range unlisted {
		if bf.Contains(num) {
			t.Errorf("expected DNC filter NOT to contain unlisted %s", num)
		}
	}
}

func TestTimezoneGovernor(t *testing.T) {
	gov := timezone.NewTimezoneGovernor()

	// 212 is New York (Eastern)
	nyPhone := "2125551234"

	loc, _ := time.LoadLocation("America/New_York")
	if loc == nil {
		loc = time.FixedZone("EST", -5*3600)
	}

	// 10:00 AM NY time -> Allowed
	allowedTime := time.Date(2026, 11, 15, 10, 0, 0, 0, loc)
	if err := gov.CanCall(nyPhone, allowedTime); err != nil {
		t.Errorf("expected 10:00 AM NY time to be allowed, got error: %v", err)
	}

	// 11:30 PM NY time -> Blocked by curfew
	curfewTime := time.Date(2026, 11, 15, 23, 30, 0, 0, loc)
	if err := gov.CanCall(nyPhone, curfewTime); err != model.ErrTimezoneRestricted {
		t.Errorf("expected curfew error at 11:30 PM, got: %v", err)
	}

	// 06:00 AM NY time -> Blocked by morning curfew
	earlyTime := time.Date(2026, 11, 15, 6, 0, 0, 0, loc)
	if err := gov.CanCall(nyPhone, earlyTime); err != model.ErrTimezoneRestricted {
		t.Errorf("expected curfew error at 06:00 AM, got: %v", err)
	}
}

func TestPredictivePacerCalculation(t *testing.T) {
	camp := &model.Campaign{
		ID:                   "camp_pacer_test",
		AllocatedAgents:      10,
		TargetAbandonRate:    0.025,
		MaxDialMultiplier:    3.5,
		HistoricalAnswerRate: 0.20, // 20% answer rate
		AverageHandleSec:     90.0,
		AverageRingSec:       15.0,
	}

	p := pacer.NewPredictivePacer(camp)

	// Case 1: 10 idle agents, 0 active calls -> should dial ~35 calls (capped at 3.5 * 10)
	dials := p.CalculateDialsNeeded(10, 0)
	if dials < 10 || dials > 35 {
		t.Errorf("expected dials between 10 and 35, got %d", dials)
	}

	// Case 2: Record high abandonment (simulate exceeding 2.5% FTC limit)
	for i := 0; i < 90; i++ {
		p.RecordOutcome(model.StatusConnected)
	}
	for i := 0; i < 10; i++ {
		p.RecordOutcome(model.StatusAbandoned) // 10% abandonment rate!
	}

	// Pacer must aggressively brake down to progressive mode (<= idle agents)
	throttledDials := p.CalculateDialsNeeded(5, 5)
	if throttledDials > 5 {
		t.Errorf("expected pacer to throttle down to progressive mode under high abandonment, got %d dials", throttledDials)
	}
}

func TestCampaignPriorityDispatching(t *testing.T) {
	camp := &model.Campaign{
		ID:              "camp_priority_test",
		AllocatedAgents: 5,
	}

	dncFilter := dnc.NewBloomFilter(1000, 0.01)
	tzGov := timezone.NewTimezoneGovernor()

	engine := queue.NewCampaignEngine(camp, dncFilter, tzGov, &queue.MockTelephonyDialer{
		HumanAnswerRate: 0.5,
	})

	leads := []*model.Lead{
		{ID: "lead_low", PhoneNumber: "2125550001", Priority: 10},
		{ID: "lead_vip", PhoneNumber: "2125550002", Priority: 100}, // Should be first
		{ID: "lead_med", PhoneNumber: "2125550003", Priority: 50},
	}

	ingested, _, err := engine.IngestLeads(context.Background(), leads)
	if err != nil {
		t.Fatalf("unexpected ingest error: %v", err)
	}
	if ingested != 3 {
		t.Fatalf("expected 3 leads ingested, got %d", ingested)
	}

	// Verify stats
	stats := engine.GetStats()
	if stats.TotalLeads != 3 {
		t.Errorf("expected total leads 3 in stats, got %d", stats.TotalLeads)
	}
}

func TestCampaignExecutionLifecycle(t *testing.T) {
	camp := &model.Campaign{
		ID:                   "camp_exec_test",
		AllocatedAgents:      5,
		HistoricalAnswerRate: 0.30,
	}

	dncFilter := dnc.NewBloomFilter(1000, 0.01)
	// Add one number to DNC to test filtering
	dncFilter.Add("2125559999")

	engine := queue.NewCampaignEngine(camp, dncFilter, nil, &queue.MockTelephonyDialer{
		HumanAnswerRate: 0.60,
		VoicemailRate:   0.20,
	})

	leads := []*model.Lead{
		{PhoneNumber: "2125551111", Priority: 10},
		{PhoneNumber: "2125552222", Priority: 20},
		{PhoneNumber: "2125559999", Priority: 30}, // DNC blocked
		{PhoneNumber: "2125554444", Priority: 40},
	}

	_, _, _ = engine.IngestLeads(context.Background(), leads)
	_ = engine.Start()

	// Wait for background dispatch ticks
	time.Sleep(200 * time.Millisecond)

	stats := engine.GetStats()
	engine.Stop()

	if stats.DialedCalls == 0 {
		t.Errorf("expected some calls to be dialed during campaign run")
	}
}

func TestCurfewHoldoffNoBusySpinAndCleanShutdown(t *testing.T) {
	gov := timezone.NewTimezoneGovernor()

	// 1. Verify NextPermissibleCallTime calculation
	nyPhone := "2125551234"
	loc, _ := time.LoadLocation("America/New_York")
	if loc == nil {
		loc = time.FixedZone("EST", -5*3600)
	}

	// At 11:30 PM, next permissible should be tomorrow 8:00 AM
	nightTime := time.Date(2026, 11, 15, 23, 30, 0, 0, loc)
	nextCallTime := gov.NextPermissibleCallTime(nyPhone, nightTime)
	expectedTarget := time.Date(2026, 11, 16, 8, 0, 0, 0, loc).UTC()

	if !nextCallTime.Equal(expectedTarget) {
		t.Errorf("expected next permissible time %v, got %v", expectedTarget, nextCallTime)
	}

	// 2. Verify engine holdoff and GetLead
	camp := &model.Campaign{
		ID:              "camp_holdoff_test",
		AllocatedAgents: 5,
	}
	engine := queue.NewCampaignEngine(camp, nil, gov, &queue.MockTelephonyDialer{
		HumanAnswerRate: 0.5,
	})

	lead := &model.Lead{
		ID:          "lead_curfew_hold",
		PhoneNumber: nyPhone,
		Priority:    100,
	}
	_, _, _ = engine.IngestLeads(context.Background(), []*model.Lead{lead})

	// Set lead NextAttemptAt 1 hour into future
	future := time.Now().UTC().Add(1 * time.Hour)
	lead.NextAttemptAt = &future

	_ = engine.Start()
	time.Sleep(120 * time.Millisecond) // Let ticker fire multiple times

	// The lead must NOT have been dialed because NextAttemptAt is in the future
	retrieved, ok := engine.GetLead("lead_curfew_hold")
	if !ok {
		t.Fatalf("expected to find lead by ID")
	}
	if retrieved.Status == model.StatusDialing || retrieved.Status == model.StatusConnected {
		t.Errorf("expected lead to remain held off, got status %s", retrieved.Status)
	}

	// 3. Graceful termination drains workers cleanly
	engine.Stop()
}

func TestIdempotentLeadIngestionDeduplication(t *testing.T) {
	camp := &model.Campaign{
		ID:              "camp_idempotent_test",
		AllocatedAgents: 5,
	}
	engine := queue.NewCampaignEngine(camp, nil, nil, nil)

	leads := []*model.Lead{
		{PhoneNumber: "+1 (212) 555-0100", Priority: 10},
		{PhoneNumber: "12125550100", Priority: 20}, // Duplicate normalized number!
		{PhoneNumber: "2125550100", Priority: 30},  // Another duplicate!
		{PhoneNumber: "4155550200", Priority: 40},  // Distinct number
	}

	accepted, duplicates, err := engine.IngestLeads(context.Background(), leads)
	if err != nil {
		t.Fatalf("unexpected ingest error: %v", err)
	}
	if accepted != 2 {
		t.Errorf("expected 2 unique accepted leads, got %d", accepted)
	}
	if duplicates != 2 {
		t.Errorf("expected 2 duplicates skipped, got %d", duplicates)
	}

	stats := engine.GetStats()
	if stats.TotalLeads != 2 {
		t.Errorf("expected total leads 2, got %d", stats.TotalLeads)
	}
}

func TestDeadLetterQueueRouting(t *testing.T) {
	camp := &model.Campaign{
		ID:              "camp_dlq_test",
		AllocatedAgents: 2,
	}

	dncFilter := dnc.NewBloomFilter(100, 0.01)
	dncFilter.Add("2125550099") // Prohibited number

	// Dialer that always returns Busy
	dialer := &queue.MockTelephonyDialer{
		BusyRate: 1.0,
	}

	engine := queue.NewCampaignEngine(camp, dncFilter, nil, dialer)

	leads := []*model.Lead{
		{ID: "lead_dnc_fail", PhoneNumber: "2125550099", Priority: 10, MaxAttempts: 1},
		{ID: "lead_exhaust_fail", PhoneNumber: "2125550088", Priority: 20, MaxAttempts: 1},
	}
	_, _, _ = engine.IngestLeads(context.Background(), leads)

	_ = engine.Start()
	time.Sleep(150 * time.Millisecond)

	dlq := engine.GetDLQ()
	if len(dlq) != 2 {
		t.Fatalf("expected 2 leads routed to DLQ, got %d", len(dlq))
	}

	for _, l := range dlq {
		if l.Status != model.StatusDLQ {
			t.Errorf("expected status DEAD_LETTER_QUEUE, got %s", l.Status)
		}
		if l.FailureReason == "" {
			t.Errorf("expected failure reason populated on DLQ lead")
		}
	}

	engine.Stop()
}

func TestMultiCampaignConcurrencyIsolation(t *testing.T) {
	numCampaigns := 5
	engines := make([]*queue.CampaignEngine, numCampaigns)

	for i := 0; i < numCampaigns; i++ {
		camp := &model.Campaign{
			ID:              "camp_concurrent_" + string(rune('A'+i)),
			AllocatedAgents: 3,
		}
		engine := queue.NewCampaignEngine(camp, nil, nil, &queue.MockTelephonyDialer{
			HumanAnswerRate: 0.8,
		})

		leads := []*model.Lead{
			{PhoneNumber: "212555100" + string(rune('0'+i)), Priority: 50},
			{PhoneNumber: "212555200" + string(rune('0'+i)), Priority: 40},
		}
		_, _, _ = engine.IngestLeads(context.Background(), leads)
		engines[i] = engine
	}

	// Start all campaigns simultaneously
	for _, e := range engines {
		_ = e.Start()
	}

	time.Sleep(150 * time.Millisecond)

	// Verify each campaign maintains independent stats
	for i, e := range engines {
		stats := e.GetStats()
		if stats.TotalLeads != 2 {
			t.Errorf("campaign %d expected 2 total leads, got %d", i, stats.TotalLeads)
		}
		if stats.DialedCalls == 0 {
			t.Errorf("campaign %d expected dials to occur independently", i)
		}
		e.Stop()
	}
}

func TestBusyCallRetryBackoff(t *testing.T) {
	camp := &model.Campaign{
		ID:              "camp_retry_test",
		AllocatedAgents: 2,
	}

	// Mock dialer that always returns Busy
	busyDialer := &queue.MockTelephonyDialer{
		HumanAnswerRate: 0.0,
		VoicemailRate:   0.0,
		BusyRate:        1.0, // 100% busy
	}

	engine := queue.NewCampaignEngine(camp, nil, nil, busyDialer)

	lead := &model.Lead{
		ID:          "lead_busy_retry",
		PhoneNumber: "2125559876",
		Priority:    50,
		MaxAttempts: 3,
	}
	_, _, _ = engine.IngestLeads(context.Background(), []*model.Lead{lead})

	engine.Start()
	time.Sleep(120 * time.Millisecond)

	retrieved, ok := engine.GetLead("lead_busy_retry")
	if !ok {
		t.Fatalf("expected to find lead")
	}

	// Must have been attempted once and rescheduled
	if retrieved.Attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", retrieved.Attempts)
	}
	if retrieved.NextAttemptAt == nil {
		t.Errorf("expected NextAttemptAt to be populated for retry")
	} else if !retrieved.NextAttemptAt.After(time.Now().UTC()) {
		t.Errorf("expected NextAttemptAt to be in the future")
	}
	if retrieved.Status != model.StatusQueued {
		t.Errorf("expected status QUEUED for retry, got %s", retrieved.Status)
	}

	engine.Stop()
}

func TestStoragePersistenceAndIdempotency(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryPostgresRepository()

	camp := &model.Campaign{
		ID:                "camp_store_test",
		Name:              "Store Test",
		AllocatedAgents:   5,
		TargetAbandonRate: 0.025,
	}

	if err := repo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	fetchedCamp, err := repo.GetCampaign(ctx, "camp_store_test")
	if err != nil || fetchedCamp.ID != "camp_store_test" {
		t.Fatalf("failed fetching campaign: %v", err)
	}

	leads := []*model.Lead{
		{ID: "l1", CampaignID: camp.ID, PhoneNumber: "+12125550111", Priority: 10},
		{ID: "l2", CampaignID: camp.ID, PhoneNumber: "+12125550111", Priority: 20}, // Duplicate!
		{ID: "l3", CampaignID: camp.ID, PhoneNumber: "+14155550222", Priority: 30},
	}

	insertedIDs, duplicates, err := repo.IngestLeadsIdempotent(ctx, leads)
	if err != nil {
		t.Fatalf("ingest error: %v", err)
	}
	if len(insertedIDs) != 2 || duplicates != 1 {
		t.Errorf("expected 2 inserted, 1 duplicate, got %d, %d", len(insertedIDs), duplicates)
	}

	// Claim leads to obtain active lease token for l1
	claimed, err := repo.ClaimPendingLeads(ctx, camp.ID, 2, "test-worker-store")
	if err != nil || len(claimed) != 2 {
		t.Fatalf("failed claiming leads: %v", err)
	}
	var lead1 *model.Lead
	for _, cl := range claimed {
		if cl.ID == "l1" {
			lead1 = cl
			break
		}
	}
	if lead1 == nil {
		t.Fatalf("expected l1 to be claimed")
	}

	// Persist outcome transactionally to DLQ with active lease token
	attempt := &model.CallAttempt{
		ID:             "att_1",
		CampaignID:     camp.ID,
		LeadID:         "l1",
		PhoneNumber:    "+12125550111",
		Status:         model.StatusFailed,
		RingDurationMs: 120,
		ErrorMessage:   "carrier timeout",
	}
	if err := repo.RecordCallOutcomeTx(ctx, "l1", model.StatusDLQ, "CARRIER_TIMEOUT", nil, attempt, 1, lead1.LeaseToken); err != nil {
		t.Fatalf("failed recording call outcome tx: %v", err)
	}

	dlq, err := repo.GetDeadLetterQueue(ctx, camp.ID)
	if err != nil || len(dlq) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d (err: %v)", len(dlq), err)
	}
	if dlq[0].FailureReason != "CARRIER_TIMEOUT" {
		t.Errorf("expected failure reason CARRIER_TIMEOUT, got %s", dlq[0].FailureReason)
	}
}

type SlowTelephonyDialer struct {
	ringDelay time.Duration
}

func (s *SlowTelephonyDialer) Dial(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error) {
	select {
	case <-time.After(s.ringDelay):
		return model.StatusAnswered, s.ringDelay, nil
	case <-ctx.Done():
		return model.StatusFailed, 0, ctx.Err()
	}
}

func TestInFlightDialLimiting(t *testing.T) {
	camp := &model.Campaign{
		ID:                   "camp_inflight_test",
		AllocatedAgents:      2,
		HistoricalAnswerRate: 0.50,
		AverageHandleSec:     60.0,
		AverageRingSec:       15.0,
	}

	// Ring delay is 100ms, ticker is 50ms
	slowDialer := &SlowTelephonyDialer{ringDelay: 100 * time.Millisecond}
	engine := queue.NewCampaignEngine(camp, nil, nil, slowDialer)

	var leads []*model.Lead
	for i := 0; i < 20; i++ {
		leads = append(leads, &model.Lead{
			PhoneNumber: "+12125550" + string(rune('0'+i)),
			Priority:    10,
		})
	}
	_, _, _ = engine.IngestLeads(context.Background(), leads)

	_ = engine.Start()
	// Let ticker fire twice (100ms)
	time.Sleep(120 * time.Millisecond)

	stats := engine.GetStats()
	// Because in-flight ringing calls are subtracted from capacity, the engine must NOT have dispatched all 20 leads
	if stats.DialedCalls > 10 {
		t.Errorf("expected in-flight limiter to throttle dials, got %d dials", stats.DialedCalls)
	}

	engine.Stop()
}

func TestPacerSingleOutcomeAccounting(t *testing.T) {
	camp := &model.Campaign{
		ID:                   "camp_accounting_test",
		AllocatedAgents:      1,    // 1 agent slot
		HistoricalAnswerRate: 0.20, // 20% estimated answer rate triggers predictive overdials (3 dials)
		MaxDialMultiplier:    3.5,
	}

	dialer := &queue.MockTelephonyDialer{
		HumanAnswerRate: 1.0, // But 100% of dialed calls actually answer simultaneously
	}

	engine := queue.NewCampaignEngine(camp, nil, nil, dialer)
	_, _, _ = engine.IngestLeads(context.Background(), []*model.Lead{
		{PhoneNumber: "+12125559091", Priority: 100},
		{PhoneNumber: "+12125559092", Priority: 90},
		{PhoneNumber: "+12125559093", Priority: 80},
	})

	_ = engine.Start()
	time.Sleep(150 * time.Millisecond)

	stats := engine.GetStats()
	// Out of 3 simultaneous answers with 1 agent slot:
	// Exactly 1 must connect and 2 must abandon. Total answered = 3.
	// Outcomes must be recorded exactly once without double counting
	if stats.ConnectedCalls != 1 {
		t.Errorf("expected exactly 1 connected call, got %d", stats.ConnectedCalls)
	}
	if stats.AbandonedCalls != 2 {
		t.Errorf("expected exactly 2 abandoned calls, got %d", stats.AbandonedCalls)
	}
	if stats.AnsweredCalls != 3 {
		t.Errorf("expected total 3 answered calls, got %d", stats.AnsweredCalls)
	}

	engine.Stop()
}


type failingRepo struct {
	storage.CampaignRepository
}

func (f *failingRepo) IngestLeadsIdempotent(ctx context.Context, leads []*model.Lead) ([]string, int, error) {
	return nil, 0, fmt.Errorf("simulated postgres connection failure / deadlock")
}

func TestSynchronousIngestRejectsOnStorageFailure(t *testing.T) {
	camp := &model.Campaign{
		ID:              "camp_fail_test",
		AllocatedAgents: 5,
	}

	engine := queue.NewCampaignEngine(camp, nil, nil, nil)
	engine.SetRepository(&failingRepo{})

	leads := []*model.Lead{
		{PhoneNumber: "+12125559901", Priority: 50},
		{PhoneNumber: "+12125559902", Priority: 40},
	}

	accepted, duplicates, err := engine.IngestLeads(context.Background(), leads)
	if err == nil {
		t.Fatalf("expected error from failing repository, got nil")
	}
	if accepted != 0 {
		t.Errorf("expected 0 accepted leads on persistence error, got %d", accepted)
	}
	if duplicates != 0 {
		t.Errorf("expected 0 duplicates on error, got %d", duplicates)
	}

	// Verify nothing was admitted to in-memory queue
	stats := engine.GetStats()
	if stats.TotalLeads != 0 {
		t.Errorf("expected 0 leads admitted to queue, got %d", stats.TotalLeads)
	}
}


func TestDatabaseDuplicateRejectedAfterRestart(t *testing.T) {
	ctx := context.Background()
	sharedRepo := storage.NewMemoryPostgresRepository()

	camp := &model.Campaign{
		ID:              "camp_restart_idempotency",
		AllocatedAgents: 5,
	}

	// 1. Initial application run: ingest lead
	engine1 := queue.NewCampaignEngine(camp, nil, nil, nil)
	engine1.SetRepository(sharedRepo)

	lead1 := &model.Lead{
		PhoneNumber: "+1 (212) 555-0999",
		Priority:    50,
	}
	accepted1, dups1, err1 := engine1.IngestLeads(ctx, []*model.Lead{lead1})
	if err1 != nil || accepted1 != 1 || dups1 != 0 {
		t.Fatalf("engine1 ingest failed: accepted=%d, dups=%d, err=%v", accepted1, dups1, err1)
	}

	// 2. Simulate application restart: new engine instance with empty in-memory index, but same DB
	engine2 := queue.NewCampaignEngine(camp, nil, nil, nil)
	engine2.SetRepository(sharedRepo)

	lead2 := &model.Lead{
		PhoneNumber: "12125550999", // Same normalized number
		Priority:    80,
	}

	accepted2, dups2, err2 := engine2.IngestLeads(ctx, []*model.Lead{lead2})
	if err2 != nil {
		t.Fatalf("unexpected error on engine2 ingest: %v", err2)
	}

	// The database correctly rejected the duplicate, so engine2 must NOT admit it to the dispatch queue!
	if accepted2 != 0 {
		t.Errorf("expected 0 accepted leads in restarted engine, got %d", accepted2)
	}
	if dups2 != 1 {
		t.Errorf("expected 1 duplicate detected by database, got %d", dups2)
	}

	stats2 := engine2.GetStats()
	if stats2.TotalLeads != 0 {
		t.Errorf("expected 0 total leads admitted to restarted engine queue, got %d", stats2.TotalLeads)
	}

	// Verify lead was never stored in engine2 memory
	if _, ok := engine2.GetLead(lead2.ID); ok {
		t.Errorf("duplicate lead must NOT exist in restarted engine memory")
	}
}


func TestLivePostgresRestartIdempotency(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	campID := "camp_live_pg_" + uuid.NewString()[:8]
	camp := &model.Campaign{
		ID:                campID,
		Name:              "Live PG Test",
		AllocatedAgents:   5,
		TargetAbandonRate: 0.025,
	}

	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign to live postgres: %v", err)
	}

	// 1. Initial engine run: ingest lead
	engine1 := queue.NewCampaignEngine(camp, nil, nil, nil)
	engine1.SetRepository(pgRepo)

	lead1 := &model.Lead{
		ID:          "lead_live_1",
		PhoneNumber: "+1 (415) 555-0999",
		Priority:    50,
	}
	accepted1, dups1, err1 := engine1.IngestLeads(ctx, []*model.Lead{lead1})
	if err1 != nil || accepted1 != 1 || dups1 != 0 {
		t.Fatalf("engine1 live postgres ingest failed: accepted=%d, dups=%d, err=%v", accepted1, dups1, err1)
	}

	// 2. Restart engine: new instance with empty in-memory state, connected to same live PostgreSQL
	engine2 := queue.NewCampaignEngine(camp, nil, nil, nil)
	engine2.SetRepository(pgRepo)

	lead2 := &model.Lead{
		ID:          "lead_live_dup",
		PhoneNumber: "14155550999", // Same normalized phone number
		Priority:    90,
	}
	accepted2, dups2, err2 := engine2.IngestLeads(ctx, []*model.Lead{lead2})
	if err2 != nil {
		t.Fatalf("live postgres engine2 ingest error: %v", err2)
	}

	// Live PostgreSQL ON CONFLICT (campaign_id, phone_number) MUST reject the duplicate
	if accepted2 != 0 {
		t.Errorf("live postgres: expected 0 accepted leads on duplicate restart, got %d", accepted2)
	}
	if dups2 != 1 {
		t.Errorf("live postgres: expected 1 duplicate from db conflict, got %d", dups2)
	}

	stats2 := engine2.GetStats()
	if stats2.TotalLeads != 0 {
		t.Errorf("live postgres: expected 0 leads in restarted engine memory, got %d", stats2.TotalLeads)
	}
}

func TestDistributedCompetingWorkersClaiming(t *testing.T) {
	repo := storage.NewMemoryPostgresRepository()
	ctx := context.Background()

	camp := &model.Campaign{
		ID:              "camp_distributed_workers",
		AllocatedAgents: 10,
		Status:          model.CampaignRunning,
	}
	_ = repo.SaveCampaign(ctx, camp)

	const totalLeads = 100
	var leads []*model.Lead
	for i := 0; i < totalLeads; i++ {
		leads = append(leads, &model.Lead{
			ID:          fmt.Sprintf("dist_lead_%03d", i),
			CampaignID:  camp.ID,
			PhoneNumber: fmt.Sprintf("+1415555%04d", i),
			Priority:    i, // Varying priorities
			Status:      model.StatusQueued,
			CreatedAt:   time.Now().UTC(),
		})
	}
	_, _, err := repo.IngestLeadsIdempotent(ctx, leads)
	if err != nil {
		t.Fatalf("failed ingesting test leads: %v", err)
	}

	// Launch 5 concurrent competing workers claiming batches
	const numWorkers = 5
	claimedCh := make(chan *model.Lead, totalLeads)
	errCh := make(chan error, numWorkers)

	for w := 0; w < numWorkers; w++ {
		workerID := fmt.Sprintf("worker-node-%d", w)
		go func(wid string) {
			for {
				batch, err := repo.ClaimPendingLeads(ctx, camp.ID, 5, wid)
				if err != nil {
					errCh <- err
					return
				}
				if len(batch) == 0 {
					return
				}
				for _, l := range batch {
					claimedCh <- l
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(workerID)
	}

	// Collect claimed leads
	claimedMap := make(map[string]int)
	timeout := time.After(3 * time.Second)

collectLoop:
	for len(claimedMap) < totalLeads {
		select {
		case err := <-errCh:
			t.Fatalf("worker error: %v", err)
		case l := <-claimedCh:
			claimedMap[l.ID]++
		case <-timeout:
			break collectLoop
		}
	}

	if len(claimedMap) != totalLeads {
		t.Fatalf("expected all %d leads claimed, got %d unique claims", totalLeads, len(claimedMap))
	}

	for id, count := range claimedMap {
		if count != 1 {
			t.Errorf("lead %s was double-claimed %d times (must be exactly 1)", id, count)
		}
	}
}

func TestServerStartupCampaignRecovery(t *testing.T) {
	repo := storage.NewMemoryPostgresRepository()
	ctx := context.Background()

	camp := &model.Campaign{
		ID:              "camp_startup_recovery",
		Name:            "Startup Recovery Test",
		AllocatedAgents: 10,
		Status:          model.CampaignRunning,
	}
	if err := repo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	leads := []*model.Lead{
		{ID: "rec_lead_1", CampaignID: camp.ID, PhoneNumber: "+14155550001", Priority: 10, Status: model.StatusQueued, CreatedAt: time.Now().UTC()},
		{ID: "rec_lead_2", CampaignID: camp.ID, PhoneNumber: "+14155550002", Priority: 50, Status: model.StatusQueued, CreatedAt: time.Now().UTC()},
		{ID: "rec_lead_3", CampaignID: camp.ID, PhoneNumber: "+14155550003", Priority: 90, Status: model.StatusQueued, CreatedAt: time.Now().UTC()},
	}
	if _, _, err := repo.IngestLeadsIdempotent(ctx, leads); err != nil {
		t.Fatalf("failed ingesting leads: %v", err)
	}

	// Verify repo returns active campaigns and active leads
	camps, err := repo.ListActiveCampaigns(ctx)
	if err != nil || len(camps) != 1 {
		t.Fatalf("expected 1 active campaign, got %d, err: %v", len(camps), err)
	}

	activeLeads, err := repo.GetActiveLeads(ctx, camp.ID)
	if err != nil || len(activeLeads) != 3 {
		t.Fatalf("expected 3 active leads from storage, got %d, err: %v", len(activeLeads), err)
	}

	// Simulate fresh server instance hydrating from storage
	restartedEngine := queue.NewCampaignEngine(camp, nil, nil, nil)
	restartedEngine.SetRepository(repo)
	restartedEngine.HydrateLeads(activeLeads)

	stats := restartedEngine.GetStats()
	if stats.TotalLeads != 3 {
		t.Errorf("expected 3 recovered leads in engine memory, got %d", stats.TotalLeads)
	}

	// Verify highest priority lead is dialed first
	l1, ok1 := restartedEngine.GetLead("rec_lead_3")
	if !ok1 || l1.Priority != 90 {
		t.Errorf("expected highest priority lead rec_lead_3 to be loaded")
	}
}

func TestAtomicTransactionalOutcomePersistence(t *testing.T) {
	repo := storage.NewMemoryPostgresRepository()
	ctx := context.Background()

	camp := &model.Campaign{
		ID:              "camp_tx_test",
		AllocatedAgents: 5,
	}
	_ = repo.SaveCampaign(ctx, camp)

	lead := &model.Lead{
		ID:          "lead_tx_1",
		CampaignID:  camp.ID,
		PhoneNumber: "+14155550099",
		Priority:    10,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = repo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

	claimed, err := repo.ClaimPendingLeads(ctx, camp.ID, 1, "test-worker")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming lead: %v", err)
	}

	attempt := &model.CallAttempt{
		ID:             "attempt_tx_1",
		CampaignID:     camp.ID,
		LeadID:         lead.ID,
		PhoneNumber:    lead.PhoneNumber,
		Status:         model.StatusAnswered,
		RingDurationMs: 120,
		TalkDurationMs: 4500,
		CreatedAt:      time.Now().UTC(),
	}

	// Atomically persist outcome with valid lease token
	err = repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusConnected, "CONNECTED_NORMAL", nil, attempt, 1, claimed[0].LeaseToken)
	if err != nil {
		t.Fatalf("expected atomic outcome persistence to succeed: %v", err)
	}

	// Verify lead status was updated
	activeLeads, err := repo.GetActiveLeads(ctx, camp.ID)
	if err != nil {
		t.Fatalf("failed fetching leads: %v", err)
	}
	// Status was CONNECTED, so should no longer be in QUEUED/DIALING active queue
	for _, l := range activeLeads {
		if l.ID == lead.ID && l.Status != model.StatusConnected {
			t.Errorf("expected lead status to be CONNECTED, got %s", l.Status)
		}
	}
}

func TestTickDistributedClaimingIntegration(t *testing.T) {
	repo := storage.NewMemoryPostgresRepository()
	ctx := context.Background()

	camp := &model.Campaign{
		ID:                "camp_tick_claim_test",
		AllocatedAgents:   5,
		TargetAbandonRate: 0.025,
		Status:            model.CampaignDraft,
	}
	_ = repo.SaveCampaign(ctx, camp)

	// Ingest leads strictly to repository (NOT hydrating engine memory queue)
	leads := []*model.Lead{
		{ID: "tick_lead_1", CampaignID: camp.ID, PhoneNumber: "+14155551111", Priority: 10, Status: model.StatusQueued, CreatedAt: time.Now().UTC()},
		{ID: "tick_lead_2", CampaignID: camp.ID, PhoneNumber: "+14155552222", Priority: 20, Status: model.StatusQueued, CreatedAt: time.Now().UTC()},
	}
	_, _, err := repo.IngestLeadsIdempotent(ctx, leads)
	if err != nil {
		t.Fatalf("failed ingesting test leads: %v", err)
	}

	engine := queue.NewCampaignEngine(camp, nil, nil, nil)
	engine.SetRepository(repo)

	// Engine starts with 0 leads in pendingQueue
	statsBefore := engine.GetStats()
	if statsBefore.TotalLeads != 0 {
		t.Fatalf("expected 0 in-memory leads before tick, got %d", statsBefore.TotalLeads)
	}

	// Start engine: tick() MUST pull from repository via ClaimPendingLeads
	_ = engine.Start()
	time.Sleep(150 * time.Millisecond)
	engine.Stop()

	// Verify leads were claimed from repo and dispatched
	statsAfter := engine.GetStats()
	if statsAfter.DialedCalls < 2 {
		t.Errorf("expected at least 2 dialed calls via distributed claim in tick, got %d", statsAfter.DialedCalls)
	}
}

func TestLivePostgresDistributedClaimingAndLeaseRecovery(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	campID := "camp_dist_live_" + uuid.NewString()[:8]
	camp := &model.Campaign{
		ID:                campID,
		Name:              "Live Distributed Claim Test",
		AllocatedAgents:   10,
		TargetAbandonRate: 0.025,
		Status:            model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	const numLeads = 20
	var leads []*model.Lead
	for i := 0; i < numLeads; i++ {
		leads = append(leads, &model.Lead{
			ID:          fmt.Sprintf("live_claim_%02d", i),
			CampaignID:  camp.ID,
			PhoneNumber: fmt.Sprintf("+1415555%04d", 2000+i),
			Priority:    i,
			Status:      model.StatusQueued,
			CreatedAt:   time.Now().UTC(),
		})
	}
	insertedIDs, _, err := pgRepo.IngestLeadsIdempotent(ctx, leads)
	if err != nil || len(insertedIDs) != numLeads {
		t.Fatalf("failed batch ingesting live leads: %v", err)
	}

	// 1. Genuinely concurrent competing workers against live PostgreSQL: 4 workers simultaneously competing for 20 leads
	const numWorkers = 4
	const batchPerWorker = 5
	var wg sync.WaitGroup
	claimedByWorker := make([][]*model.Lead, numWorkers)
	errs := make([]error, numWorkers)

	startBarrier := make(chan struct{})
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			<-startBarrier
			workerID := fmt.Sprintf("worker-node-%d", workerIdx)
			claimed, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, batchPerWorker, workerID)
			claimedByWorker[workerIdx] = claimed
			errs[workerIdx] = err
		}(w)
	}

	close(startBarrier) // Release all workers simultaneously to compete for rows in PostgreSQL
	wg.Wait()

	totalClaimed := 0
	seen := make(map[string]string)
	for w := 0; w < numWorkers; w++ {
		if errs[w] != nil {
			t.Fatalf("worker %d claim failed: %v", w, errs[w])
		}
		totalClaimed += len(claimedByWorker[w])
		for _, l := range claimedByWorker[w] {
			if prevWorker, duplicate := seen[l.ID]; duplicate {
				t.Fatalf("lead %s was double-claimed by both %s and worker-node-%d on live PostgreSQL!", l.ID, prevWorker, w)
			}
			seen[l.ID] = fmt.Sprintf("worker-node-%d", w)
		}
	}
	if totalClaimed != numLeads {
		t.Fatalf("expected all %d leads to be claimed, got %d", numLeads, totalClaimed)
	}

	// 2. Expired Lease Recovery: Simulate worker crash during call
	campCrashID := "camp_crash_live_" + uuid.NewString()[:8]
	campCrash := &model.Campaign{
		ID:                campCrashID,
		Name:              "Live Crash Recovery Campaign",
		AllocatedAgents:   5,
		TargetAbandonRate: 0.025,
		Status:            model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, campCrash); err != nil {
		t.Fatalf("failed saving crash campaign: %v", err)
	}

	crashedLeadID := "live_claim_crashed"
	crashedLead := &model.Lead{
		ID:          crashedLeadID,
		CampaignID:  campCrash.ID,
		PhoneNumber: "+14155559999",
		Priority:    100, // Highest priority
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{crashedLead})

	// Manually simulate expired lock (> 2 min ago) for worker crash
	_, err = pgRepo.ClaimPendingLeads(ctx, campCrash.ID, 1, "crashed-worker-old")
	if err != nil {
		t.Fatalf("failed initial claim for crash simulation: %v", err)
	}
	_, err = pgRepo.Pool().Exec(ctx, "UPDATE leads SET locked_at = NOW() - INTERVAL '5 minutes' WHERE id = $1", crashedLeadID)
	if err != nil {
		t.Fatalf("failed backdating lock for crash simulation: %v", err)
	}

	// Reclaim expired lease with standard 2m TTL
	reclaimedCount, err := pgRepo.ReclaimExpiredLeases(ctx, campCrash.ID, 2*time.Minute)
	if err != nil {
		t.Fatalf("failed reclaiming expired leases: %v", err)
	}
	if reclaimedCount != 1 {
		t.Errorf("expected 1 expired lease reclaimed, got %d", reclaimedCount)
	}

	// Active worker now claims: must successfully pick up the orphaned lead
	recovered, err := pgRepo.ClaimPendingLeads(ctx, campCrash.ID, 1, "surviving-worker-new")
	if err != nil {
		t.Fatalf("failed claiming recovered lead: %v", err)
	}
	if len(recovered) != 1 || recovered[0].ID != crashedLeadID {
		t.Fatalf("expected crashed lead %s to be reclaimed, got %v", crashedLeadID, recovered)
	}
	if recovered[0].LockedBy != "surviving-worker-new" {
		t.Errorf("expected lock updated to surviving-worker-new, got %s", recovered[0].LockedBy)
	}
}

// -------------------------------------------------------------
// Live PostgreSQL Competing Engines Scheduler Integration Test
// -------------------------------------------------------------

type callbackDialer struct {
	dialFn func(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error)
}

func (d *callbackDialer) Dial(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error) {
	if d.dialFn != nil {
		return d.dialFn(ctx, lead)
	}
	return model.StatusAnswered, 10 * time.Millisecond, nil
}

func TestLivePostgresCompetingEnginesSchedulerDispatch(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	campID := "camp_engine_compete_" + uuid.NewString()[:8]
	camp := &model.Campaign{
		ID:                campID,
		Name:              "Live Competing Engines Scheduler Test",
		AllocatedAgents:   2,
		MaxDialMultiplier: 1.5,
		TargetAbandonRate: 0.025,
		Status:            model.CampaignDraft,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	const numLeads = 24
	var leads []*model.Lead
	for i := 0; i < numLeads; i++ {
		leads = append(leads, &model.Lead{
			ID:          fmt.Sprintf("live_eng_%02d", i),
			CampaignID:  camp.ID,
			PhoneNumber: fmt.Sprintf("+1415556%04d", 3000+i),
			Priority:    i,
			Status:      model.StatusQueued,
			CreatedAt:   time.Now().UTC(),
		})
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, leads)
	if err != nil {
		t.Fatalf("failed ingesting leads: %v", err)
	}

	// Shared dial ledger across both engines
	var dialMu sync.Mutex
	dialCounts := make(map[string]int)

	recordingDialer := &callbackDialer{
		dialFn: func(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error) {
			dialMu.Lock()
			dialCounts[lead.ID]++
			dialMu.Unlock()
			select {
			case <-time.After(25 * time.Millisecond):
			case <-ctx.Done():
			}
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}

	// Instantiate two independent running CampaignEngine instances sharing PostgreSQL
	camp1 := *camp
	camp2 := *camp
	engine1 := queue.NewCampaignEngine(&camp1, nil, nil, recordingDialer)
	engine1.SetRepository(pgRepo)

	engine2 := queue.NewCampaignEngine(&camp2, nil, nil, recordingDialer)
	engine2.SetRepository(pgRepo)

	// Launch both engines simultaneously via Start() -> tick() background dispatch loops
	if err := engine1.Start(); err != nil {
		t.Fatalf("failed starting engine1: %v", err)
	}
	if err := engine2.Start(); err != nil {
		t.Fatalf("failed starting engine2: %v", err)
	}

	if !engine1.IsRunning() || !engine2.IsRunning() {
		t.Fatalf("expected both engines to be actively running")
	}

	// Wait for background tick loops to process all leads
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		dialMu.Lock()
		count := len(dialCounts)
		dialMu.Unlock()
		if count >= numLeads {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)
	engine1.Stop()
	engine2.Stop()

	dialMu.Lock()
	defer dialMu.Unlock()

	if len(dialCounts) != numLeads {
		t.Fatalf("expected all %d leads dialed, got %d", numLeads, len(dialCounts))
	}
	// Assert zero double-dialing across competing scheduler engines
	for leadID, count := range dialCounts {
		if count > 1 {
			t.Errorf("lead %s was dialed %d times (expected exactly 1 dial across engines)", leadID, count)
		}
	}

	stats1 := engine1.GetStats()
	stats2 := engine2.GetStats()
	if stats1.DialedCalls == 0 || stats2.DialedCalls == 0 {
		t.Errorf("expected both competing engines to actively participate in dispatch: engine1 dialed %d, engine2 dialed %d", stats1.DialedCalls, stats2.DialedCalls)
	}
}

func TestLivePostgresDNCPersistenceAndCrossInstanceScreening(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	// 1. Instance 1 registers formatted US number with spaces, parens, and country code
	formattedDNC := fmt.Sprintf("+1 (800) 555-%04d", rand.Intn(10000))
	if err := pgRepo.AddDNCNumbers(ctx, []string{formattedDNC}); err != nil {
		t.Fatalf("failed adding DNC number to live postgres: %v", err)
	}

	// Canonical 10-digit number
	normDNC := dnc.NormalizeNumber(formattedDNC)

	// 2. Cross-Instance Normalization Verification:
	// Both raw formatted, stripped E.164 (+1800...), and local 10-digit formats MUST match in PostgreSQL!
	testQueries := []string{
		formattedDNC,
		"+1" + normDNC,
		normDNC,
		fmt.Sprintf("1%s", normDNC),
	}
	for _, q := range testQueries {
		prohibited, err := pgRepo.IsDNCProhibited(ctx, q)
		if err != nil {
			t.Fatalf("failed querying DNC status for query %q: %v", q, err)
		}
		if !prohibited {
			t.Errorf("expected query %q to be prohibited across instances via live postgres normalization", q)
		}
	}

	// 3. Instance 2 engine without bloom filter screening must drop normalized lead to DLQ
	camp := &model.Campaign{
		ID:              "camp_dnc_cross_" + uuid.NewString()[:8],
		AllocatedAgents: 5,
		Status:          model.CampaignRunning,
	}
	_ = pgRepo.SaveCampaign(ctx, camp)

	var dialedCalls int32
	mockDialer := &callbackDialer{
		dialFn: func(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error) {
			atomic.AddInt32(&dialedCalls, 1)
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}

	engine2 := queue.NewCampaignEngine(camp, nil, nil, mockDialer) // nil bloom filter!
	engine2.SetRepository(pgRepo)

	lead := &model.Lead{
		ID:          "lead_dnc_cross",
		CampaignID:  camp.ID,
		PhoneNumber: normDNC, // Using stripped 10-digit format as ingested by engine
		Priority:    100,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})
	if err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	_ = engine2.Start()
	time.Sleep(200 * time.Millisecond)
	engine2.Stop()

	if atomic.LoadInt32(&dialedCalls) > 0 {
		t.Errorf("expected 0 dials for DNC prohibited number, got %d", dialedCalls)
	}

	dlqLeads, err := pgRepo.GetDeadLetterQueue(ctx, camp.ID)
	if err != nil {
		t.Fatalf("failed fetching DLQ: %v", err)
	}
	found := false
	for _, l := range dlqLeads {
		if l.ID == lead.ID && l.FailureReason == "DNC_PROHIBITED" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected lead %s to be in DLQ with DNC_PROHIBITED reason, got %v", lead.ID, dlqLeads)
	}
}

func TestLivePostgresDurableAttemptStartPersistenceCrashSafety(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	camp := &model.Campaign{
		ID:              "camp_live_crash_" + uuid.NewString()[:8],
		AllocatedAgents: 5,
		Status:          model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	lead := &model.Lead{
		ID:          "lead_live_crash_1",
		CampaignID:  camp.ID,
		PhoneNumber: fmt.Sprintf("+1415555%04d", rand.Intn(10000)),
		Priority:    50,
		Attempts:    0,
		MaxAttempts: 3,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})
	if err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	// 1. Worker reserves lead
	claimed, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-1")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming lead: %v", err)
	}

	// 2. Prior to carrier dial, RecordAttemptStart writes attempts=1 and IN_PROGRESS attempt record durably to PostgreSQL
	attemptID := uuid.NewString()
	err = pgRepo.RecordAttemptStart(ctx, lead.ID, attemptID, 1, claimed[0].LeaseToken)
	if err != nil {
		t.Fatalf("failed recording attempt start on live postgres: %v", err)
	}

	// 3. Verify durability in PostgreSQL before carrier returns: attempts count must already be 1
	var dbAttempts int
	err = pgRepo.Pool().QueryRow(ctx, "SELECT attempts FROM leads WHERE id = $1", lead.ID).Scan(&dbAttempts)
	if err != nil {
		t.Fatalf("failed querying lead attempts from live postgres: %v", err)
	}
	if dbAttempts != 1 {
		t.Fatalf("expected attempts=1 durably preserved in live postgres before dial completion, got %d", dbAttempts)
	}

	var attemptStatus string
	err = pgRepo.Pool().QueryRow(ctx, "SELECT status FROM call_attempts WHERE id = $1", attemptID).Scan(&attemptStatus)
	if err != nil {
		t.Fatalf("failed querying attempt status: %v", err)
	}
	if attemptStatus != "IN_PROGRESS" {
		t.Fatalf("expected call_attempts status IN_PROGRESS, got %s", attemptStatus)
	}

	// 4. Simulate process crash during dial (carrier never returns, instance abruptly dies)
	// Backdate lock to simulate expired lease
	_, err = pgRepo.Pool().Exec(ctx, "UPDATE leads SET locked_at = NOW() - INTERVAL '5 minutes' WHERE id = $1", lead.ID)
	if err != nil {
		t.Fatalf("failed backdating lock: %v", err)
	}

	// 5. Surviving worker instance reclaims expired lease
	reclaimed, err := pgRepo.ReclaimExpiredLeases(ctx, camp.ID, 2*time.Minute)
	if err != nil || reclaimed != 1 {
		t.Fatalf("failed reclaiming expired lease: %v, reclaimed=%d", err, reclaimed)
	}

	// 6. Next worker claims lead: attempts must remain 1 (cannot be evaded by crashing)
	reclaimedLeads, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-2")
	if err != nil || len(reclaimedLeads) != 1 {
		t.Fatalf("failed claiming recovered lead: %v", err)
	}
	if reclaimedLeads[0].Attempts != 1 {
		t.Fatalf("expected recovered lead to retain attempts=1 across process crash, got %d", reclaimedLeads[0].Attempts)
	}
}

// FailingAttemptRepo simulates database failure during attempt-start
type FailingAttemptRepo struct {
	*storage.MemoryPostgresRepository
	shouldFailAttemptStart bool
	shouldFailDNC          bool
}

func (f *FailingAttemptRepo) RecordAttemptStart(ctx context.Context, leadID string, attemptID string, attemptNum int, leaseToken string) error {
	if f.shouldFailAttemptStart {
		return fmt.Errorf("simulated postgres connection deadlock on attempt start")
	}
	return f.MemoryPostgresRepository.RecordAttemptStart(ctx, leadID, attemptID, attemptNum, leaseToken)
}

func (f *FailingAttemptRepo) IsDNCProhibited(ctx context.Context, phone string) (bool, error) {
	if f.shouldFailDNC {
		return false, fmt.Errorf("simulated postgres timeout during dnc check")
	}
	return f.MemoryPostgresRepository.IsDNCProhibited(ctx, phone)
}

func TestAttemptStartFailureAbortsCarrierDial(t *testing.T) {
	memRepo := storage.NewMemoryPostgresRepository()
	failRepo := &FailingAttemptRepo{
		MemoryPostgresRepository: memRepo,
		shouldFailAttemptStart:   true,
	}
	ctx := context.Background()

	camp := &model.Campaign{
		ID:              "camp_abort_dial",
		AllocatedAgents: 5,
		Status:          model.CampaignDraft,
	}
	_ = failRepo.SaveCampaign(ctx, camp)

	lead := &model.Lead{
		ID:          "lead_abort_1",
		CampaignID:  camp.ID,
		PhoneNumber: "+14155554321",
		Priority:    50,
		Attempts:    0,
		MaxAttempts: 3,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = failRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

	var carrierDials int32
	recordingDialer := &callbackDialer{
		dialFn: func(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error) {
			atomic.AddInt32(&carrierDials, 1)
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}

	engine := queue.NewCampaignEngine(camp, nil, nil, recordingDialer)
	engine.SetRepository(failRepo)

	_ = engine.Start()
	time.Sleep(150 * time.Millisecond)
	engine.Stop()

	// Carrier dial MUST NOT have been invoked because attempt-start failed!
	if atomic.LoadInt32(&carrierDials) != 0 {
		t.Fatalf("expected 0 carrier dials when attempt start persistence fails, got %d", carrierDials)
	}

	// Verify lead was safely rescheduled to QUEUED with attempts unchanged
	active, _ := failRepo.GetActiveLeads(ctx, camp.ID)
	if len(active) != 1 || active[0].Attempts != 0 {
		t.Errorf("expected lead attempts to remain 0, got %d", active[0].Attempts)
	}
}

func TestDNCFailClosedOnDatabaseError(t *testing.T) {
	memRepo := storage.NewMemoryPostgresRepository()
	failRepo := &FailingAttemptRepo{
		MemoryPostgresRepository: memRepo,
		shouldFailDNC:            true,
	}
	ctx := context.Background()

	camp := &model.Campaign{
		ID:              "camp_fail_closed_dnc",
		AllocatedAgents: 5,
		Status:          model.CampaignDraft,
	}
	_ = failRepo.SaveCampaign(ctx, camp)

	lead := &model.Lead{
		ID:          "lead_fail_closed_1",
		CampaignID:  camp.ID,
		PhoneNumber: "+14155558888",
		Priority:    50,
		Attempts:    0,
		MaxAttempts: 3,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = failRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

	var carrierDials int32
	recordingDialer := &callbackDialer{
		dialFn: func(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error) {
			atomic.AddInt32(&carrierDials, 1)
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}

	// Nil bloom filter forces engine to check repository
	engine := queue.NewCampaignEngine(camp, nil, nil, recordingDialer)
	engine.SetRepository(failRepo)

	_ = engine.Start()
	time.Sleep(150 * time.Millisecond)
	engine.Stop()

	// Compliance requirement: when DNC status cannot be verified due to DB error, NEVER dial!
	if atomic.LoadInt32(&carrierDials) != 0 {
		t.Fatalf("expected 0 dials when DNC DB check fails (fail-closed violated!), got %d", carrierDials)
	}
}

func TestTimezoneCurfewPersistentReschedule(t *testing.T) {
	repo := storage.NewMemoryPostgresRepository()
	ctx := context.Background()

	camp := &model.Campaign{
		ID:                "camp_curfew_resched_test",
		AllocatedAgents:   5,
		TargetAbandonRate: 0.025,
		Status:            model.CampaignDraft,
	}
	_ = repo.SaveCampaign(ctx, camp)

	tzGov := timezone.NewTimezoneGovernor()
	engine := queue.NewCampaignEngine(camp, nil, tzGov, nil)
	engine.SetRepository(repo)

	lead := &model.Lead{
		ID:          "lead_curfew_persistent",
		CampaignID:  camp.ID,
		PhoneNumber: "+12125550100", // New York
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = repo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

	// Manually claim lead as tick() would
	claimed, err := repo.ClaimPendingLeads(ctx, camp.ID, 1, "test-worker")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming lead: %v", err)
	}

	// Simulate curfew block reschedule via RescheduleLead with valid lease token
	futureTime := time.Now().UTC().Add(8 * time.Hour)
	err = repo.RescheduleLead(ctx, lead.ID, futureTime, claimed[0].LeaseToken)
	if err != nil {
		t.Fatalf("failed rescheduling lead: %v", err)
	}

	// Verify in database: status is QUEUED, lock cleared, attempts = 0
	active, err := repo.GetActiveLeads(ctx, camp.ID)
	if err != nil || len(active) != 1 {
		t.Fatalf("expected 1 active lead, got %v", active)
	}
	if active[0].Status != model.StatusQueued {
		t.Errorf("expected status QUEUED, got %s", active[0].Status)
	}
	if active[0].LockedBy != "" {
		t.Errorf("expected lock cleared, got %s", active[0].LockedBy)
	}
	if active[0].Attempts != 0 {
		t.Errorf("expected 0 attempts consumed by curfew holdoff, got %d", active[0].Attempts)
	}

	// Claiming again right now must return 0 leads because next_attempt_at is in the future
	reclaimed, err := repo.ClaimPendingLeads(ctx, camp.ID, 1, "test-worker-2")
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if len(reclaimed) != 0 {
		t.Errorf("expected 0 leads claimable during holdoff window, got %d", len(reclaimed))
	}
}

func TestAttemptCountingExactnessAndConsistency(t *testing.T) {
	repo := storage.NewMemoryPostgresRepository()
	ctx := context.Background()

	camp := &model.Campaign{
		ID:                "camp_attempts_test",
		AllocatedAgents:   5,
		TargetAbandonRate: 0.025,
		Status:            model.CampaignDraft,
	}
	_ = repo.SaveCampaign(ctx, camp)

	lead := &model.Lead{
		ID:          "lead_attempts_exact",
		CampaignID:  camp.ID,
		PhoneNumber: "+14155557788",
		Status:      model.StatusQueued,
		MaxAttempts: 3,
		Attempts:    0,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = repo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

	// 1. Claiming does NOT increment attempts
	claimed, err := repo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-1")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming: %v", err)
	}
	if claimed[0].Attempts != 0 {
		t.Errorf("expected 0 attempts on claim, got %d", claimed[0].Attempts)
	}

	// 2. Call attempt occurs (dial 1) -> busy -> retry 1 scheduled in the past to simulate elapsed backoff
	pastRetry := time.Now().UTC().Add(-10 * time.Millisecond)
	err = repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusQueued, "BUSY", &pastRetry, nil, 1, claimed[0].LeaseToken)
	if err != nil {
		t.Fatalf("failed recording outcome 1: %v", err)
	}

	// Verify database attempt count is 1
	active, _ := repo.GetActiveLeads(ctx, camp.ID)
	if len(active) != 1 || active[0].Attempts != 1 {
		t.Fatalf("expected DB attempts=1, got %v", active)
	}

	// 3. Dial 2 -> busy -> retry 2
	claimed, _ = repo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-1")
	if len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("expected claim with attempts=1, got %v", claimed)
	}
	err = repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusQueued, "BUSY", &pastRetry, nil, 2, claimed[0].LeaseToken)
	if err != nil {
		t.Fatalf("failed recording outcome 2: %v", err)
	}

	// 4. Dial 3 -> busy -> max attempts exhausted -> DLQ
	claimed, _ = repo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-1")
	if len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("expected claim with attempts=2, got %v", claimed)
	}
	err = repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusDLQ, "EXHAUSTED_MAX_ATTEMPTS", nil, nil, 3, claimed[0].LeaseToken)
	if err != nil {
		t.Fatalf("failed recording DLQ outcome: %v", err)
	}

	// Verify in DLQ: exactly 3 attempts
	dlq, err := repo.GetDeadLetterQueue(ctx, camp.ID)
	if err != nil || len(dlq) != 1 {
		t.Fatalf("expected 1 lead in DLQ, got %v", dlq)
	}
	if dlq[0].Attempts != 3 {
		t.Errorf("expected exactly 3 attempts in DLQ, got %d", dlq[0].Attempts)
	}
}

func TestDNCPersistenceAcrossRestart(t *testing.T) {
	repo := storage.NewMemoryPostgresRepository()
	ctx := context.Background()

	dncList := []string{"+14155550011", "+14155550022", "+14155550033"}
	err := repo.AddDNCNumbers(ctx, dncList)
	if err != nil {
		t.Fatalf("failed adding DNC numbers: %v", err)
	}

	// Simulate restart: load DNC numbers from storage into new Bloom filter
	loaded, err := repo.GetDNCNumbers(ctx)
	if err != nil {
		t.Fatalf("failed loading DNC numbers: %v", err)
	}
	if len(loaded) != len(dncList) {
		t.Fatalf("expected %d loaded numbers, got %d", len(dncList), len(loaded))
	}

	newBloom := dnc.NewBloomFilter(100000, 0.001)
	for _, n := range loaded {
		newBloom.Add(n)
	}

	for _, n := range dncList {
		if !newBloom.Contains(n) {
			t.Errorf("expected DNC filter to contain %s after restart hydration", n)
		}
	}
	if newBloom.Contains("+14155559999") {
		t.Errorf("unexpected number found in DNC filter")
	}
}

func TestRecordAttemptStartAffectedRowsValidation(t *testing.T) {
	ctx := context.Background()
	memRepo := storage.NewMemoryPostgresRepository()

	// 1. Missing lead must fail affected rows check
	err := memRepo.RecordAttemptStart(ctx, "non_existent_lead", uuid.NewString(), 1, "token-1")
	if err == nil {
		t.Fatalf("expected error when recording attempt start for non-existent lead, got nil")
	}

	// 2. Lead in QUEUED state (not DIALING) must fail
	lead := &model.Lead{
		ID:          "lead_queued_only",
		CampaignID:  "camp_test",
		PhoneNumber: "+14155550199",
		Status:      model.StatusQueued,
	}
	_, _, _ = memRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

	err = memRepo.RecordAttemptStart(ctx, lead.ID, uuid.NewString(), 1, "token-1")
	if err == nil {
		t.Fatalf("expected error when recording attempt start for lead not in DIALING state, got nil")
	}

	// 3. Lead transitioned to DIALING with matching lease token and active lease must succeed
	lead.Status = model.StatusDialing
	lead.LeaseToken = "token-valid"
	nowLock := time.Now().UTC()
	lead.LockedAt = &nowLock
	err = memRepo.RecordAttemptStart(ctx, lead.ID, uuid.NewString(), 1, "token-stale")
	if err == nil {
		t.Fatalf("expected error on lease token mismatch, got nil")
	}

	err = memRepo.RecordAttemptStart(ctx, lead.ID, uuid.NewString(), 1, "token-valid")
	if err != nil {
		t.Fatalf("expected success for lead in DIALING state with valid lease token, got: %v", err)
	}
}

func TestLivePostgresRecordAttemptStartAffectedRowsValidation(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	// 1. Missing lead: 0 rows affected -> must rollback and return error
	err = pgRepo.RecordAttemptStart(ctx, "missing_lead_"+uuid.NewString()[:8], uuid.NewString(), 1, "token-1")
	if err == nil {
		t.Fatalf("expected error on live postgres for non-existent lead, got nil")
	}

	// 2. Ingest lead in QUEUED state without claiming: UPDATE affects 0 rows -> must fail
	camp := &model.Campaign{
		ID:              "camp_affected_test_" + uuid.NewString()[:8],
		AllocatedAgents: 1,
		Status:          model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	lead := &model.Lead{
		ID:          "lead_affected_1",
		CampaignID:  camp.ID,
		PhoneNumber: fmt.Sprintf("+1415555%04d", rand.Intn(10000)),
		Priority:    50,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})
	if err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	// Attempt start without claiming (status is QUEUED, not DIALING) must fail
	err = pgRepo.RecordAttemptStart(ctx, lead.ID, uuid.NewString(), 1, "token-1")
	if err == nil {
		t.Fatalf("expected error on live postgres when lead status is QUEUED (not DIALING), got nil")
	}

	// 3. Claim lead into DIALING state: now RecordAttemptStart must affect exactly 1 row and succeed
	claimed, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-affected-1")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming lead: %v", err)
	}

	err = pgRepo.RecordAttemptStart(ctx, lead.ID, uuid.NewString(), 1, claimed[0].LeaseToken)
	if err != nil {
		t.Fatalf("expected RecordAttemptStart to succeed on claimed DIALING lead, got %v", err)
	}
}

// TestLivePostgresDistributedLeaseFencingStaleWorkerRejected verifies distributed fencing token semantics:
// 1. Worker A claims a lead, receiving leaseToken A.
// 2. Lease expires (> 2 minutes) and Worker B reclaims the lead, receiving new distinct leaseToken B.
// 3. Resumed stale Worker A calls RecordAttemptStart() with leaseToken A -> REJECTED (0 rows affected).
// 4. Active Worker B calls RecordAttemptStart() with leaseToken B -> SUCCEEDS (1 row affected).
// 5. Stale Worker A calls RecordCallOutcomeTx() with leaseToken A -> REJECTED.
// 6. Active Worker B calls RecordCallOutcomeTx() with leaseToken B -> SUCCEEDS.
func TestLivePostgresDistributedLeaseFencingStaleWorkerRejected(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	camp := &model.Campaign{
		ID:              "camp_fencing_" + uuid.NewString()[:8],
		AllocatedAgents: 5,
		Status:          model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	lead := &model.Lead{
		ID:          "lead_fencing_" + uuid.NewString()[:8],
		CampaignID:  camp.ID,
		PhoneNumber: fmt.Sprintf("+1415555%04d", rand.Intn(10000)),
		Priority:    100,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})
	if err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	// 1. Worker A claims lead
	claimedA, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-A")
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("worker A failed claiming lead: %v", err)
	}
	leaseTokenA := claimedA[0].LeaseToken
	if leaseTokenA == "" {
		t.Fatalf("expected non-empty leaseToken for claimed lead")
	}

	// 2. Simulate Worker A process pause/delay past 2-minute lease TTL
	_, err = pgRepo.Pool().Exec(ctx,
		"UPDATE leads SET locked_at = NOW() - INTERVAL '3 minutes' WHERE id = $1", lead.ID)
	if err != nil {
		t.Fatalf("failed simulating expired lease: %v", err)
	}

	// 3. Worker B reclaims the expired lead
	claimedB, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-B")
	if err != nil || len(claimedB) != 1 {
		t.Fatalf("worker B failed reclaiming expired lead: %v", err)
	}
	leaseTokenB := claimedB[0].LeaseToken
	if leaseTokenB == "" || leaseTokenB == leaseTokenA {
		t.Fatalf("expected Worker B to receive a new distinct leaseToken, got %s (A was %s)", leaseTokenB, leaseTokenA)
	}

	// 4. Stale Worker A resumes and calls RecordAttemptStart with stale leaseToken A -> MUST FAIL
	err = pgRepo.RecordAttemptStart(ctx, lead.ID, uuid.NewString(), 1, leaseTokenA)
	if err == nil {
		t.Fatalf("expected stale Worker A attempt-start to fail due to lease token fencing, but got nil error")
	}

	// 5. Active Worker B calls RecordAttemptStart with valid leaseToken B -> MUST SUCCEED
	attemptIDB := uuid.NewString()
	err = pgRepo.RecordAttemptStart(ctx, lead.ID, attemptIDB, 1, leaseTokenB)
	if err != nil {
		t.Fatalf("expected active Worker B attempt-start to succeed, got %v", err)
	}

	// 6. Stale Worker A attempts to record outcome with stale leaseToken A -> MUST FAIL
	err = pgRepo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusConnected, "CONNECTED_STALE_A", nil, nil, 1, leaseTokenA)
	if err == nil {
		t.Fatalf("expected stale Worker A outcome persistence to fail due to lease token fencing, but got nil error")
	}

	// 7. Active Worker B records outcome with valid leaseToken B -> MUST SUCCEED
	attemptB := &model.CallAttempt{
		ID:             attemptIDB,
		CampaignID:     camp.ID,
		LeadID:         lead.ID,
		PhoneNumber:    lead.PhoneNumber,
		Status:         model.StatusConnected,
		RingDurationMs: 100,
		TalkDurationMs: 2000,
		CreatedAt:      time.Now().UTC(),
	}
	err = pgRepo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusConnected, "CONNECTED_NORMAL", nil, attemptB, 1, leaseTokenB)
	if err != nil {
		t.Fatalf("expected active Worker B outcome persistence to succeed, got %v", err)
	}

	// 8. Verify final disposition in PostgreSQL matches Worker B's outcome
	var finalStatus, finalReason string
	err = pgRepo.Pool().QueryRow(ctx, "SELECT status, failure_reason FROM leads WHERE id = $1", lead.ID).Scan(&finalStatus, &finalReason)
	if err != nil {
		t.Fatalf("failed querying final lead status: %v", err)
	}
	if finalStatus != "CONNECTED" || finalReason != "CONNECTED_NORMAL" {
		t.Fatalf("expected lead status CONNECTED / CONNECTED_NORMAL from Worker B, got %s / %s", finalStatus, finalReason)
	}
}

// TestQueueLevelStaleWorkerCleanupDoesNotMutateReclaimedLease verifies that when a stale worker's
// attempt-start fails due to lease expiration, its queue-level cleanup does NOT clear Worker B's active lease.
func TestQueueLevelStaleWorkerCleanupDoesNotMutateReclaimedLease(t *testing.T) {
	repo := storage.NewMemoryPostgresRepository()
	ctx := context.Background()

	camp := &model.Campaign{
		ID:                "camp_queue_fencing_mem",
		AllocatedAgents:   5,
		TargetAbandonRate: 0.025,
		Status:            model.CampaignRunning,
	}
	_ = repo.SaveCampaign(ctx, camp)

	lead := &model.Lead{
		ID:          "lead_queue_fencing_mem",
		CampaignID:  camp.ID,
		PhoneNumber: "+14155550188",
		Priority:    100,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = repo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

	// 1. Worker A claims the lead
	claimedA, err := repo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-A")
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("worker A failed claiming lead: %v", err)
	}
	leadA := claimedA[0]
	leaseTokenA := leadA.LeaseToken
	if leaseTokenA == "" {
		t.Fatalf("expected non-empty lease token for Worker A")
	}

	// 2. Simulate lease expiration in repository
	time.Sleep(10 * time.Millisecond)
	reclaimedCount, err := repo.ReclaimExpiredLeases(ctx, camp.ID, 1*time.Millisecond)
	if err != nil || reclaimedCount != 1 {
		t.Fatalf("failed reclaiming expired lease: %v, count=%d", err, reclaimedCount)
	}

	// 3. Worker B claims the reclaimed lead
	claimedB, err := repo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-B")
	if err != nil || len(claimedB) != 1 {
		t.Fatalf("worker B failed claiming reclaimed lead: %v", err)
	}
	leadB := claimedB[0]
	leaseTokenB := leadB.LeaseToken
	if leaseTokenB == "" || leaseTokenB == leaseTokenA {
		t.Fatalf("expected distinct lease token for Worker B, got %s (A=%s)", leaseTokenB, leaseTokenA)
	}

	var carrierDialsA int32
	dialerA := &callbackDialer{
		dialFn: func(ctx context.Context, l *model.Lead) (model.CallStatus, time.Duration, error) {
			atomic.AddInt32(&carrierDialsA, 1)
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}
	engineA := queue.NewCampaignEngine(camp, nil, nil, dialerA)
	engineA.SetRepository(repo)

	// 4. Stale Worker A dispatches leadA: attempt-start fails due to stale token, cleanup runs
	engineA.DispatchLeadDirect(leadA)

	// Carrier dial A must NEVER have been called
	if atomic.LoadInt32(&carrierDialsA) != 0 {
		t.Fatalf("stale Worker A must NOT initiate carrier dial, dialed %d times", carrierDialsA)
	}

	// 5. In repository: Worker B's lease MUST still be active and completely untouched!
	activeLeads, err := repo.GetActiveLeads(ctx, camp.ID)
	if err != nil || len(activeLeads) != 1 {
		t.Fatalf("expected 1 active lead, got %v", activeLeads)
	}
	if activeLeads[0].Status != model.StatusDialing {
		t.Fatalf("Worker A's error handling wiped out Worker B's lease! Status is %s (expected DIALING)", activeLeads[0].Status)
	}
	if activeLeads[0].LockedBy != "worker-node-B" {
		t.Fatalf("expected LockedBy 'worker-node-B', got %s", activeLeads[0].LockedBy)
	}
	if activeLeads[0].LeaseToken != leaseTokenB {
		t.Fatalf("expected LeaseToken %s, got %s", leaseTokenB, activeLeads[0].LeaseToken)
	}

	// 6. Active Worker B dispatches leadB: attempt-start and outcome succeed
	var carrierDialsB int32
	dialerB := &callbackDialer{
		dialFn: func(ctx context.Context, l *model.Lead) (model.CallStatus, time.Duration, error) {
			atomic.AddInt32(&carrierDialsB, 1)
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}
	engineB := queue.NewCampaignEngine(camp, nil, nil, dialerB)
	engineB.SetRepository(repo)

	engineB.DispatchLeadDirect(leadB)

	if atomic.LoadInt32(&carrierDialsB) != 1 {
		t.Fatalf("active Worker B must initiate carrier dial, dialed %d times", carrierDialsB)
	}

	// 7. Verify final disposition is CONNECTED
	finalLeads, _ := repo.GetActiveLeads(ctx, camp.ID)
	if len(finalLeads) != 0 {
		t.Fatalf("expected 0 active queued/dialing leads after completion, got %d", len(finalLeads))
	}
}

// TestLivePostgresRescheduleLeadFencing verifies that RescheduleLead strictly requires a valid non-empty
// lease token and rejects stale tokens with zero rows affected.
func TestLivePostgresRescheduleLeadFencing(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	camp := &model.Campaign{
		ID:              "camp_resched_fence_" + uuid.NewString()[:8],
		AllocatedAgents: 1,
		Status:          model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	lead := &model.Lead{
		ID:          "lead_resched_fence_" + uuid.NewString()[:8],
		CampaignID:  camp.ID,
		PhoneNumber: fmt.Sprintf("+1415555%04d", rand.Intn(10000)),
		Priority:    100,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})
	if err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	// 1. Claim lead -> receives valid leaseToken
	claimed, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-resched-1")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming lead: %v", err)
	}
	leaseToken := claimed[0].LeaseToken
	if leaseToken == "" {
		t.Fatalf("expected non-empty lease token")
	}

	futureRetry := time.Now().UTC().Add(10 * time.Minute)

	// 2. Empty lease token must be rejected up front
	err = pgRepo.RescheduleLead(ctx, lead.ID, futureRetry, "")
	if err == nil {
		t.Fatalf("expected error when rescheduling with empty lease token, got nil")
	}

	// 3. Stale/invalid lease token must fail (0 rows affected)
	err = pgRepo.RescheduleLead(ctx, lead.ID, futureRetry, "stale_token_abc123")
	if err == nil {
		t.Fatalf("expected error when rescheduling with stale lease token, got nil")
	}

	// Verify database row still has DIALING status and active lease
	var curStatus, curToken string
	err = pgRepo.Pool().QueryRow(ctx, "SELECT status, lease_token FROM leads WHERE id = $1", lead.ID).Scan(&curStatus, &curToken)
	if err != nil {
		t.Fatalf("failed reading lead: %v", err)
	}
	if curStatus != "DIALING" || curToken != leaseToken {
		t.Fatalf("lead state corrupted after rejected reschedule: status=%s, token=%s", curStatus, curToken)
	}

	// 4. Reschedule with valid lease token must succeed (1 row affected)
	err = pgRepo.RescheduleLead(ctx, lead.ID, futureRetry, leaseToken)
	if err != nil {
		t.Fatalf("expected reschedule with valid lease token to succeed, got %v", err)
	}

	// Verify database row is returned to QUEUED with lease cleared
	var finalStatus string
	var finalLockedBy, finalToken *string
	err = pgRepo.Pool().QueryRow(ctx, "SELECT status, locked_by, lease_token FROM leads WHERE id = $1", lead.ID).Scan(&finalStatus, &finalLockedBy, &finalToken)
	if err != nil {
		t.Fatalf("failed reading rescheduled lead: %v", err)
	}
	if finalStatus != "QUEUED" || finalLockedBy != nil || finalToken != nil {
		t.Fatalf("expected QUEUED with cleared lease, got status=%s, lockedBy=%v, token=%v", finalStatus, finalLockedBy, finalToken)
	}
}

// TestLivePostgresQueueLevelStaleWorkerCleanupDoesNotMutateReclaimedLease tests the end-to-end
// CampaignEngine queue error-recovery path against real PostgreSQL:
// 1. Worker A claims lead via ClaimPendingLeads -> Token A.
// 2. Database expires lease (locked_at set to 3 minutes ago).
// 3. Worker B reclaims expired lead via ClaimPendingLeads -> Token B.
// 4. Stale Worker A's engine dispatches lead with Token A:
//    - RecordAttemptStart fails due to lease token mismatch.
//    - Worker A detects stale lease and abandons local lead WITHOUT calling RescheduleLead.
// 5. In PostgreSQL: verify Worker B's lease is untouched (status DIALING, lease_token == Token B).
// 6. Active Worker B's engine dispatches lead with Token B: attempt-start and outcome succeed.
// 7. In PostgreSQL: verify final status is CONNECTED with 1 attempt.
func TestLivePostgresQueueLevelStaleWorkerCleanupDoesNotMutateReclaimedLease(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	camp := &model.Campaign{
		ID:              "camp_queue_live_" + uuid.NewString()[:8],
		AllocatedAgents: 5,
		Status:          model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	lead := &model.Lead{
		ID:          "lead_queue_live_" + uuid.NewString()[:8],
		CampaignID:  camp.ID,
		PhoneNumber: fmt.Sprintf("+1415555%04d", rand.Intn(10000)),
		Priority:    100,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})
	if err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	// 1. Worker A claims lead -> Token A
	claimedA, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-A")
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("worker A failed claiming lead: %v", err)
	}
	leadA := claimedA[0]
	leaseTokenA := leadA.LeaseToken
	if leaseTokenA == "" {
		t.Fatalf("expected non-empty leaseToken for Worker A")
	}

	// 2. Simulate lease expiration in PostgreSQL
	_, err = pgRepo.Pool().Exec(ctx,
		"UPDATE leads SET locked_at = NOW() - INTERVAL '3 minutes' WHERE id = $1", lead.ID)
	if err != nil {
		t.Fatalf("failed simulating expired lease: %v", err)
	}

	// 3. Worker B reclaims expired lead -> Token B
	claimedB, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-B")
	if err != nil || len(claimedB) != 1 {
		t.Fatalf("worker B failed reclaiming lead: %v", err)
	}
	leadB := claimedB[0]
	leaseTokenB := leadB.LeaseToken
	if leaseTokenB == "" || leaseTokenB == leaseTokenA {
		t.Fatalf("expected distinct leaseToken for Worker B, got %s (A=%s)", leaseTokenB, leaseTokenA)
	}

	var carrierDialsA int32
	dialerA := &callbackDialer{
		dialFn: func(ctx context.Context, l *model.Lead) (model.CallStatus, time.Duration, error) {
			atomic.AddInt32(&carrierDialsA, 1)
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}
	engineA := queue.NewCampaignEngine(camp, nil, nil, dialerA)
	engineA.SetRepository(pgRepo)

	// 4. Stale Worker A dispatches leadA: attempt-start rejected in PostgreSQL, cleanup runs
	engineA.DispatchLeadDirect(leadA)

	if atomic.LoadInt32(&carrierDialsA) != 0 {
		t.Fatalf("stale Worker A must NOT initiate carrier dial, dialed %d times", carrierDialsA)
	}

	// 5. Query PostgreSQL directly: Worker B's lease MUST still be active and untouched!
	var currentStatus, currentLockedBy, currentLeaseToken string
	err = pgRepo.Pool().QueryRow(ctx,
		"SELECT status, locked_by, lease_token FROM leads WHERE id = $1", lead.ID).
		Scan(&currentStatus, &currentLockedBy, &currentLeaseToken)
	if err != nil {
		t.Fatalf("failed querying lead after stale worker cleanup: %v", err)
	}
	if currentStatus != "DIALING" {
		t.Fatalf("Stale Worker A reset lead status to %s (expected DIALING held by Worker B)!", currentStatus)
	}
	if currentLockedBy != "worker-node-B" {
		t.Fatalf("Stale Worker A corrupted locked_by to %s (expected 'worker-node-B')!", currentLockedBy)
	}
	if currentLeaseToken != leaseTokenB {
		t.Fatalf("Stale Worker A corrupted lease_token to %s (expected '%s')!", currentLeaseToken, leaseTokenB)
	}

	// 6. Active Worker B dispatches leadB: attempt-start and outcome succeed
	var carrierDialsB int32
	dialerB := &callbackDialer{
		dialFn: func(ctx context.Context, l *model.Lead) (model.CallStatus, time.Duration, error) {
			atomic.AddInt32(&carrierDialsB, 1)
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}
	engineB := queue.NewCampaignEngine(camp, nil, nil, dialerB)
	engineB.SetRepository(pgRepo)

	engineB.DispatchLeadDirect(leadB)

	if atomic.LoadInt32(&carrierDialsB) != 1 {
		t.Fatalf("active Worker B must initiate carrier dial, dialed %d times", carrierDialsB)
	}

	// 7. Verify final disposition in PostgreSQL is CONNECTED
	var finalStatus string
	var finalAttempts int
	err = pgRepo.Pool().QueryRow(ctx,
		"SELECT status, attempts FROM leads WHERE id = $1", lead.ID).
		Scan(&finalStatus, &finalAttempts)
	if err != nil {
		t.Fatalf("failed querying final lead status: %v", err)
	}
	if finalStatus != "CONNECTED" || finalAttempts != 1 {
		t.Fatalf("expected final lead status CONNECTED with attempts=1, got %s (attempts=%d)", finalStatus, finalAttempts)
	}
}

// TestMemoryAttemptStartFailsOnExpiredLeaseBeforeReclamation verifies that a worker whose lease has expired (>2m)
// cannot begin dialing, even if no other worker has reclaimed the row yet.
func TestMemoryAttemptStartFailsOnExpiredLeaseBeforeReclamation(t *testing.T) {
	ctx := context.Background()
	repo := storage.NewMemoryPostgresRepository()

	camp := &model.Campaign{
		ID:              "camp_expired_mem",
		AllocatedAgents: 1,
		Status:          model.CampaignRunning,
	}
	_ = repo.SaveCampaign(ctx, camp)

	lead := &model.Lead{
		ID:          "lead_expired_mem",
		CampaignID:  camp.ID,
		PhoneNumber: "+14155550999",
		Priority:    50,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = repo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

	// 1. Worker A claims the lead
	claimed, err := repo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-A")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming lead: %v", err)
	}
	leaseTokenA := claimed[0].LeaseToken

	// 2. Simulate Worker A process pause exceeding 2-minute lease TTL
	// In repository, backdate the lead's locked_at by 3 minutes
	pastTime := time.Now().UTC().Add(-3 * time.Minute)
	lead.LockedAt = &pastTime

	// 3. Worker A resumes and calls RecordAttemptStart: MUST FAIL due to expired lease
	attemptID := uuid.NewString()
	err = repo.RecordAttemptStart(ctx, lead.ID, attemptID, 1, leaseTokenA)
	if err == nil {
		t.Fatalf("expected RecordAttemptStart to fail on expired lease before reclamation, got nil")
	}

	// 4. Refresh locked_at within lease window: RecordAttemptStart now succeeds and atomically refreshes locked_at
	now := time.Now().UTC()
	lead.LockedAt = &now

	validAttemptID := uuid.NewString()
	err = repo.RecordAttemptStart(ctx, lead.ID, validAttemptID, 1, leaseTokenA)
	if err != nil {
		t.Fatalf("expected RecordAttemptStart to succeed on active lease, got %v", err)
	}
}

// TestLivePostgresAttemptStartFailsOnExpiredLeaseBeforeReclamation verifies that in live PostgreSQL,
// an attempt-start update requires locked_at >= NOW() - INTERVAL '2 minutes' and locked_at IS NOT NULL.
// If Worker A's process pauses > 2 minutes and Worker B has NOT reclaimed the row, Worker A's RecordAttemptStart
// MUST FAIL with zero affected rows, preventing Worker A from initiating dialing on an expired lease.
func TestLivePostgresAttemptStartFailsOnExpiredLeaseBeforeReclamation(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	camp := &model.Campaign{
		ID:              "camp_expiry_fencing_" + uuid.NewString()[:8],
		AllocatedAgents: 1,
		Status:          model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	lead := &model.Lead{
		ID:          "lead_expiry_fencing_" + uuid.NewString()[:8],
		CampaignID:  camp.ID,
		PhoneNumber: fmt.Sprintf("+1415555%04d", rand.Intn(10000)),
		Priority:    100,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})
	if err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	// 1. Worker A claims lead -> receives valid leaseToken
	claimed, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-A")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming lead: %v", err)
	}
	leaseTokenA := claimed[0].LeaseToken
	if leaseTokenA == "" {
		t.Fatalf("expected non-empty lease token")
	}

	// 2. Simulate Worker A process pause exceeding 2 minutes:
	// Backdate locked_at by 3 minutes in PostgreSQL, but DO NOT let Worker B reclaim the row!
	_, err = pgRepo.Pool().Exec(ctx,
		"UPDATE leads SET locked_at = NOW() - INTERVAL '3 minutes' WHERE id = $1", lead.ID)
	if err != nil {
		t.Fatalf("failed simulating expired lease in postgres: %v", err)
	}

	// Verify row in PostgreSQL still has DIALING status and Token A stored
	var curStatus, curToken, curWorker string
	err = pgRepo.Pool().QueryRow(ctx,
		"SELECT status, lease_token, locked_by FROM leads WHERE id = $1", lead.ID).
		Scan(&curStatus, &curToken, &curWorker)
	if err != nil {
		t.Fatalf("failed querying lead: %v", err)
	}
	if curStatus != "DIALING" || curToken != leaseTokenA || curWorker != "worker-node-A" {
		t.Fatalf("expected row to still hold Worker A's expired lease, got status=%s, token=%s, worker=%s", curStatus, curToken, curWorker)
	}

	// 3. Worker A resumes and calls RecordAttemptStart() with its leaseToken A:
	// MUST FAIL because locked_at is older than NOW() - INTERVAL '2 minutes'!
	attemptID := uuid.NewString()
	err = pgRepo.RecordAttemptStart(ctx, lead.ID, attemptID, 1, leaseTokenA)
	if err == nil {
		t.Fatalf("expected RecordAttemptStart to fail on expired lease before reclamation, but got nil error")
	}

	// 4. Verify in PostgreSQL that attempt was NOT recorded and attempts counter was NOT incremented
	var dbAttempts int
	err = pgRepo.Pool().QueryRow(ctx, "SELECT attempts FROM leads WHERE id = $1", lead.ID).Scan(&dbAttempts)
	if err != nil {
		t.Fatalf("failed querying attempts: %v", err)
	}
	if dbAttempts != 0 {
		t.Fatalf("expected attempts to remain 0 after rejected attempt-start on expired lease, got %d", dbAttempts)
	}

	var attemptCount int
	err = pgRepo.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM call_attempts WHERE id = $1", attemptID).Scan(&attemptCount)
	if err != nil {
		t.Fatalf("failed querying call_attempts: %v", err)
	}
	if attemptCount != 0 {
		t.Fatalf("expected 0 call attempts inserted for expired lease attempt-start, got %d", attemptCount)
	}

	// 5. Verify that once refreshed within lease window, attempt start succeeds:
	_, err = pgRepo.Pool().Exec(ctx, "UPDATE leads SET locked_at = NOW() WHERE id = $1", lead.ID)
	if err != nil {
		t.Fatalf("failed refreshing locked_at: %v", err)
	}

	validAttemptID := uuid.NewString()
	err = pgRepo.RecordAttemptStart(ctx, lead.ID, validAttemptID, 1, leaseTokenA)
	if err != nil {
		t.Fatalf("expected RecordAttemptStart to succeed on refreshed active lease, got %v", err)
	}

	err = pgRepo.Pool().QueryRow(ctx, "SELECT attempts FROM leads WHERE id = $1", lead.ID).Scan(&dbAttempts)
	if err != nil || dbAttempts != 1 {
		t.Fatalf("expected attempts=1 after valid attempt-start, got %d (err: %v)", dbAttempts, err)
	}
}

// TestLivePostgresAttemptStartLockWaitExpiryRejection verifies that when a transaction waits on a row lock,
// lease expiry is evaluated post-lock-acquisition using clock_timestamp(), not the stale transaction start time.
// If Tx 1 holds the lock and backdates locked_at beyond DefaultLeaseTTL before committing, Worker A's blocked
// RecordAttemptStart MUST be rejected upon acquiring the lock, inserting 0 attempts and incrementing 0 attempts.
func TestLivePostgresAttemptStartLockWaitExpiryRejection(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("skipping live PostgreSQL test: TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pgRepo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to live postgres: %v", err)
	}
	defer pgRepo.Close()

	camp := &model.Campaign{
		ID:              "camp_lock_wait_" + uuid.NewString()[:8],
		AllocatedAgents: 1,
		Status:          model.CampaignRunning,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	lead := &model.Lead{
		ID:          "lead_lock_wait_" + uuid.NewString()[:8],
		CampaignID:  camp.ID,
		PhoneNumber: fmt.Sprintf("+1415555%04d", rand.Intn(10000)),
		Priority:    100,
		Status:      model.StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, err = pgRepo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})
	if err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	// 1. Worker A claims the lead and acquires a valid leaseToken
	claimed, err := pgRepo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-node-A")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed claiming lead: %v", err)
	}
	leaseTokenA := claimed[0].LeaseToken
	if leaseTokenA == "" {
		t.Fatalf("expected non-empty lease token")
	}

	// 2. Tx 1 starts and acquires an exclusive row lock on the lead row
	tx1, err := pgRepo.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)

	var lockedID string
	err = tx1.QueryRow(ctx, "SELECT id FROM leads WHERE id = $1 FOR UPDATE", lead.ID).Scan(&lockedID)
	if err != nil {
		t.Fatalf("failed acquiring row lock in tx1: %v", err)
	}

	// 3. Worker A initiates RecordAttemptStart in a background goroutine.
	// It begins its transaction and attempts SELECT ... FOR UPDATE, which BLOCKS on Tx 1's lock.
	attemptID := uuid.NewString()
	attemptStartDone := make(chan error, 1)
	go func() {
		attemptStartDone <- pgRepo.RecordAttemptStart(ctx, lead.ID, attemptID, 1, leaseTokenA)
	}()

	// Verify Worker A is genuinely blocked waiting on the row lock
	select {
	case earlyErr := <-attemptStartDone:
		t.Fatalf("expected RecordAttemptStart to block on row lock, but completed prematurely: %v", earlyErr)
	case <-time.After(300 * time.Millisecond):
		// Goroutine successfully blocked on Tx 1 row lock
	}

	// 4. While Worker A is waiting for the row lock, Tx 1 backdates locked_at past DefaultLeaseTTL (3 minutes ago)
	_, err = tx1.Exec(ctx, "UPDATE leads SET locked_at = clock_timestamp() - INTERVAL '3 minutes' WHERE id = $1", lead.ID)
	if err != nil {
		t.Fatalf("failed backdating locked_at in tx1: %v", err)
	}

	// 5. Tx 1 commits, releasing the row lock to Worker A
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed committing tx1: %v", err)
	}

	// 6. Worker A unblocks, acquires row lock, evaluates lease validity via clock_timestamp(), and MUST FAIL
	select {
	case startErr := <-attemptStartDone:
		if startErr == nil {
			t.Fatalf("expected RecordAttemptStart to fail due to lock-wait lease expiry, but succeeded!")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for RecordAttemptStart to finish after row lock released")
	}

	// 7. Verify in PostgreSQL that:
	// - lead attempts counter remains 0
	// - no call_attempts row was inserted
	var dbAttempts int
	err = pgRepo.Pool().QueryRow(ctx, "SELECT attempts FROM leads WHERE id = $1", lead.ID).Scan(&dbAttempts)
	if err != nil {
		t.Fatalf("failed querying lead attempts: %v", err)
	}
	if dbAttempts != 0 {
		t.Fatalf("expected attempts to remain 0 after lock-wait expiry rejection, got %d", dbAttempts)
	}

	var attemptCount int
	err = pgRepo.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM call_attempts WHERE id = $1", attemptID).Scan(&attemptCount)
	if err != nil {
		t.Fatalf("failed querying call_attempts count: %v", err)
	}
	if attemptCount != 0 {
		t.Fatalf("expected 0 call_attempts inserted for lock-wait expired attempt-start, got %d", attemptCount)
	}
}
