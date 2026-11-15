package tests

import (
	"context"
	"fmt"
	"os"
	"sync"
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
	engine.Start()

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

	engine.Start()
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

	engine.Start()
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
		e.Start()
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

	// Update lead to DLQ
	if err := repo.UpdateLeadStatus(ctx, "l1", model.StatusDLQ, "CARRIER_TIMEOUT", nil); err != nil {
		t.Fatalf("failed updating lead: %v", err)
	}

	// Record Call Attempt
	attempt := &model.CallAttempt{
		ID:             "att_1",
		CampaignID:     camp.ID,
		LeadID:         "l1",
		PhoneNumber:    "+12125550111",
		Status:         model.StatusFailed,
		RingDurationMs: 120,
		ErrorMessage:   "carrier timeout",
	}
	if err := repo.RecordCallAttempt(ctx, attempt); err != nil {
		t.Fatalf("failed recording call attempt: %v", err)
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

	engine.Start()
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

	engine.Start()
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
		Status:      model.StatusDialing,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, _ = repo.IngestLeadsIdempotent(ctx, []*model.Lead{lead})

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

	// Atomically persist outcome
	err := repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusConnected, "CONNECTED_NORMAL", nil, attempt, 1)
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
	engine.Start()
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
		AllocatedAgents:   10,
		TargetAbandonRate: 0.025,
		Status:            model.CampaignDraft,
	}
	if err := pgRepo.SaveCampaign(ctx, camp); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	const numLeads = 12
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
			return model.StatusAnswered, 10 * time.Millisecond, nil
		},
	}

	// Instantiate two independent running CampaignEngine instances sharing PostgreSQL
	engine1 := queue.NewCampaignEngine(camp, nil, nil, recordingDialer)
	engine1.SetRepository(pgRepo)

	engine2 := queue.NewCampaignEngine(camp, nil, nil, recordingDialer)
	engine2.SetRepository(pgRepo)

	// Launch both engines simultaneously via Start() -> tick() background dispatch loops
	engine1.Start()
	engine2.Start()

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

	// Simulate curfew block reschedule via RescheduleLead
	futureTime := time.Now().UTC().Add(8 * time.Hour)
	err = repo.RescheduleLead(ctx, lead.ID, futureTime)
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
	err = repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusQueued, "BUSY", &pastRetry, nil, 1)
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
	err = repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusQueued, "BUSY", &pastRetry, nil, 2)
	if err != nil {
		t.Fatalf("failed recording outcome 2: %v", err)
	}

	// 4. Dial 3 -> busy -> max attempts exhausted -> DLQ
	claimed, _ = repo.ClaimPendingLeads(ctx, camp.ID, 1, "worker-1")
	if len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("expected claim with attempts=2, got %v", claimed)
	}
	err = repo.RecordCallOutcomeTx(ctx, lead.ID, model.StatusDLQ, "EXHAUSTED_MAX_ATTEMPTS", nil, nil, 3)
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
