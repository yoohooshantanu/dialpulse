package queue

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/yoohooshantanu/dialpulse/internal/dnc"
	"github.com/yoohooshantanu/dialpulse/internal/logger"
	"github.com/yoohooshantanu/dialpulse/internal/metrics"
	"github.com/yoohooshantanu/dialpulse/internal/model"
	"github.com/yoohooshantanu/dialpulse/internal/pacer"
	"github.com/yoohooshantanu/dialpulse/internal/timezone"
)

type TelephonyDialer interface {
	Dial(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error)
}

// MockTelephonyDialer simulates carrier trunk connections with configurable answer probabilities
type MockTelephonyDialer struct {
	HumanAnswerRate float64
	VoicemailRate   float64
	BusyRate        float64
}

func (m *MockTelephonyDialer) Dial(ctx context.Context, lead *model.Lead) (model.CallStatus, time.Duration, error) {
	// Simulated carrier ring time (10ms - 25ms in test/simulation)
	ringTime := time.Duration(10+rand.Intn(15)) * time.Millisecond
	select {
	case <-time.After(ringTime):
	case <-ctx.Done():
		return model.StatusFailed, 0, ctx.Err()
	}

	roll := rand.Float64()
	if roll < m.HumanAnswerRate {
		return model.StatusAnswered, ringTime, nil
	} else if roll < m.HumanAnswerRate+m.VoicemailRate {
		return model.StatusVoicemail, ringTime, nil
	} else if roll < m.HumanAnswerRate+m.VoicemailRate+m.BusyRate {
		return model.StatusBusy, ringTime, nil
	}
	return model.StatusNoAnswer, ringTime, nil
}

// CampaignEngine orchestrates lead priority dispatching, pacing, and carrier execution
type CampaignEngine struct {
	mu           sync.RWMutex
	campaign     *model.Campaign
	pacer        *pacer.PredictivePacer
	dncFilter    *dnc.BloomFilter
	tzGovernor   *timezone.TimezoneGovernor
	dialer       TelephonyDialer
	leads        map[string]*model.Lead
	pendingQueue []*model.Lead
	activeCalls  int32
	idleAgents   int32
	stopCh       chan struct{}
	stopOnce     sync.Once
	isClosed     bool
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func NewCampaignEngine(
	campaign *model.Campaign,
	dncFilter *dnc.BloomFilter,
	tzGovernor *timezone.TimezoneGovernor,
	dialer TelephonyDialer,
) *CampaignEngine {
	if dialer == nil {
		dialer = &MockTelephonyDialer{
			HumanAnswerRate: 0.20,
			VoicemailRate:   0.15,
			BusyRate:        0.05,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	engine := &CampaignEngine{
		campaign:     campaign,
		pacer:        pacer.NewPredictivePacer(campaign),
		dncFilter:    dncFilter,
		tzGovernor:   tzGovernor,
		dialer:       dialer,
		leads:        make(map[string]*model.Lead),
		pendingQueue: make([]*model.Lead, 0),
		idleAgents:   int32(campaign.AllocatedAgents),
		stopCh:       make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}

	return engine
}

// IngestLeads inserts new prospective contacts into the priority queue
func (e *CampaignEngine) IngestLeads(leads []*model.Lead) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	accepted := 0
	for _, l := range leads {
		if l.ID == "" {
			l.ID = "lead_" + uuid.NewString()[:8]
		}
		l.CampaignID = e.campaign.ID
		l.Status = model.StatusQueued
		if l.MaxAttempts <= 0 {
			l.MaxAttempts = 3
		}
		if l.CreatedAt.IsZero() {
			l.CreatedAt = time.Now().UTC()
		}

		e.leads[l.ID] = l
		e.pendingQueue = append(e.pendingQueue, l)
		accepted++
	}

	// Sort pending queue by priority descending (VIP leads first)
	sort.SliceStable(e.pendingQueue, func(i, j int) bool {
		return e.pendingQueue[i].Priority > e.pendingQueue[j].Priority
	})

	metrics.DefaultMetrics.AddLeads(accepted)
	logger.L.Info("leads_ingested", "campaign_id", e.campaign.ID, "count", accepted)
	return accepted
}

// Start begins the predictive dispatch ticker loop
func (e *CampaignEngine) Start() {
	e.campaign.Status = model.CampaignRunning
	go e.dispatchLoop()
}

func (e *CampaignEngine) dispatchLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.tick()
		}
	}
}

func (e *CampaignEngine) tick() {
	e.mu.Lock()
	if e.campaign.Status != model.CampaignRunning || len(e.pendingQueue) == 0 {
		e.mu.Unlock()
		return
	}

	idle := int(atomic.LoadInt32(&e.idleAgents))
	active := int(atomic.LoadInt32(&e.activeCalls))

	// Ask predictive pacer how many lines to dial
	dialsNeeded := e.pacer.CalculateDialsNeeded(idle, active)
	if dialsNeeded <= 0 {
		e.mu.Unlock()
		return
	}

	// Filter out leads currently holding off due to timezone curfew
	now := time.Now().UTC()
	var eligible []*model.Lead
	var deferred []*model.Lead

	for _, lead := range e.pendingQueue {
		if lead.NextAttemptAt != nil && now.Before(*lead.NextAttemptAt) {
			deferred = append(deferred, lead)
		} else if len(eligible) < dialsNeeded {
			eligible = append(eligible, lead)
		} else {
			deferred = append(deferred, lead)
		}
	}

	e.pendingQueue = deferred
	e.mu.Unlock()

	// Dispatch batch concurrently
	for _, lead := range eligible {
		e.wg.Add(1)
		go e.dispatchLead(lead)
	}
}

func (e *CampaignEngine) dispatchLead(lead *model.Lead) {
	defer e.wg.Done()
	metrics.DefaultMetrics.IncActiveDialer()
	defer metrics.DefaultMetrics.DecActiveDialer()

	// 1. DNC Screening
	if e.dncFilter != nil && e.dncFilter.Contains(lead.PhoneNumber) {
		lead.Status = model.StatusFailed
		metrics.DefaultMetrics.IncDNCHit()
		logger.L.Warn("dial_blocked_dnc", "lead_id", lead.ID, "phone", lead.PhoneNumber)
		return
	}

	// 2. Timezone / Curfew Screening
	if e.tzGovernor != nil {
		if err := e.tzGovernor.CanCall(lead.PhoneNumber); err != nil {
			metrics.DefaultMetrics.IncTimezoneBlock()
			logger.L.Debug("dial_blocked_curfew", "lead_id", lead.ID, "phone", lead.PhoneNumber)
			nextPermissible := e.tzGovernor.NextPermissibleCallTime(lead.PhoneNumber)
			// Return to queue for next valid window with backoff hold
			e.mu.Lock()
			lead.NextAttemptAt = &nextPermissible
			e.pendingQueue = append(e.pendingQueue, lead)
			e.mu.Unlock()
			return
		}
	}

	atomic.AddInt32(&e.activeCalls, 1)
	defer atomic.AddInt32(&e.activeCalls, -1)

	metrics.DefaultMetrics.IncDial()
	lead.Status = model.StatusDialing
	now := time.Now().UTC()
	lead.LastAttemptAt = &now
	lead.Attempts++

	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
	defer cancel()

	status, _, err := e.dialer.Dial(ctx, lead)
	if err != nil {
		lead.Status = model.StatusFailed
		e.pacer.RecordOutcome(model.StatusFailed)
		return
	}

	lead.Status = status
	e.pacer.RecordOutcome(status)

	if status == model.StatusAnswered {
		// Attempt to allocate idle AI agent
		if atomic.AddInt32(&e.idleAgents, -1) >= 0 {
			// Successfully connected
			lead.Status = model.StatusConnected
			metrics.DefaultMetrics.IncConnect()
			logger.L.Info("call_connected", "lead_id", lead.ID, "campaign_id", e.campaign.ID)

			// Simulate AI call handling (30ms - 60ms)
			select {
			case <-time.After(time.Duration(30+rand.Intn(30)) * time.Millisecond):
			case <-e.ctx.Done():
			}
			atomic.AddInt32(&e.idleAgents, 1)
		} else {
			// No agent was available when customer answered -> Abandoned Call!
			atomic.AddInt32(&e.idleAgents, 1)
			lead.Status = model.StatusAbandoned
			metrics.DefaultMetrics.IncAbandon()
			e.pacer.RecordOutcome(model.StatusAbandoned)
			logger.L.Warn("call_abandoned", "lead_id", lead.ID, "campaign_id", e.campaign.ID)
		}
	}
}

// GetStats returns live statistical progress
func (e *CampaignEngine) GetStats() *model.CampaignStats {
	idle := int(atomic.LoadInt32(&e.idleAgents))
	active := int(atomic.LoadInt32(&e.activeCalls))

	stats := e.pacer.GetStats(idle, active)

	e.mu.RLock()
	stats.TotalLeads = int64(len(e.leads))
	e.mu.RUnlock()

	return stats
}

// GetLead retrieves an individual lead's state
func (e *CampaignEngine) GetLead(leadID string) (*model.Lead, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	lead, ok := e.leads[leadID]
	return lead, ok
}

// Stop gracefully terminates campaign execution and drains active workers
func (e *CampaignEngine) Stop() {
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.campaign.Status = model.CampaignPaused
		e.isClosed = true
		e.mu.Unlock()
		e.cancel()
		close(e.stopCh)
		e.wg.Wait()
		logger.L.Info("campaign_stopped", "campaign_id", e.campaign.ID)
	})
}
