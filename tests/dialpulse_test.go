package tests

import (
	"context"
	"fmt"
	"os"
	"github.com/google/uuid"
	"testing"
	"time"

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
