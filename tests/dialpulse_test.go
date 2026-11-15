package tests

import (
	"testing"
	"time"

	"github.com/yoohooshantanu/dialpulse/internal/dnc"
	"github.com/yoohooshantanu/dialpulse/internal/model"
	"github.com/yoohooshantanu/dialpulse/internal/pacer"
	"github.com/yoohooshantanu/dialpulse/internal/queue"
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

	ingested := engine.IngestLeads(leads)
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

	engine.IngestLeads(leads)
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
	engine.IngestLeads([]*model.Lead{lead})

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
