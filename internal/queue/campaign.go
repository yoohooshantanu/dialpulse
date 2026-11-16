package queue

import (
	"context"
	"fmt"
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
	"github.com/yoohooshantanu/dialpulse/internal/storage"
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

// CampaignEngine orchestrates lead priority dispatching, predictive pacing, and carrier execution
type CampaignEngine struct {
	mu            sync.RWMutex
	workerID      string
	campaign      *model.Campaign
	pacer         *pacer.PredictivePacer
	dncFilter     *dnc.BloomFilter
	tzGovernor    *timezone.TimezoneGovernor
	dialer        TelephonyDialer
	repo          storage.CampaignRepository
	leads         map[string]*model.Lead
	phoneIndex    map[string]string // In-memory cache for fast deduplication
	pendingQueue  []*model.Lead
	dlq           []*model.Lead // Dead-Letter Queue for unserviceable leads
	activeCalls   int32         // Currently connected or in-conversation calls
	inFlightDials int32         // Outstanding dialing/ringing outbound attempts
	idleAgents    int32
	started       bool
	stopCh        chan struct{}
	stopOnce      sync.Once
	isClosed      bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
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
	workerID := fmt.Sprintf("worker-%s-%s", campaign.ID, uuid.NewString()[:8])

	engine := &CampaignEngine{
		workerID:     workerID,
		campaign:     campaign,
		pacer:        pacer.NewPredictivePacer(campaign),
		dncFilter:    dncFilter,
		tzGovernor:   tzGovernor,
		dialer:       dialer,
		leads:        make(map[string]*model.Lead),
		phoneIndex:   make(map[string]string),
		pendingQueue: make([]*model.Lead, 0),
		dlq:          make([]*model.Lead, 0),
		idleAgents:   int32(campaign.AllocatedAgents),
		stopCh:       make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}

	return engine
}

// WorkerID returns the unique instance worker identifier
func (e *CampaignEngine) WorkerID() string {
	return e.workerID
}

// SetRepository connects a persistent repository (PostgreSQL or Memory replica)
func (e *CampaignEngine) SetRepository(repo storage.CampaignRepository) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.repo = repo
}

// IngestLeads inserts new prospective contacts idempotently.
// When a persistent repository is configured, persistence is strictly synchronous:
// leads are persisted to PostgreSQL before admission into the in-memory dispatch queue.
func (e *CampaignEngine) IngestLeads(ctx context.Context, leads []*model.Lead) (int, int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var toInsert []*model.Lead
	normMap := make(map[string]*model.Lead)
	duplicates := 0

	for _, l := range leads {
		normPhone := dnc.NormalizeNumber(l.PhoneNumber)
		if normPhone == "" {
			continue
		}

		// Idempotency check against in-memory index
		if _, exists := e.phoneIndex[normPhone]; exists {
			duplicates++
			continue
		}
		if _, exists := normMap[normPhone]; exists {
			duplicates++
			continue
		}

		if l.ID == "" {
			l.ID = "lead_" + uuid.NewString()[:8]
		}
		l.CampaignID = e.campaign.ID
		l.PhoneNumber = normPhone
		l.Status = model.StatusQueued
		if l.MaxAttempts <= 0 {
			l.MaxAttempts = 3
		}
		if l.CreatedAt.IsZero() {
			l.CreatedAt = time.Now().UTC()
		}

		normMap[normPhone] = l
		toInsert = append(toInsert, l)
	}

	// Persist to PostgreSQL synchronously first
	admittedMap := normMap
	if e.repo != nil && len(toInsert) > 0 {
		insertedIDs, dbDups, err := e.repo.IngestLeadsIdempotent(ctx, toInsert)
		if err != nil {
			logger.L.Error("lead_ingest_db_failed", "campaign_id", e.campaign.ID, "error", err)
			return 0, 0, fmt.Errorf("database transaction failed: %w", err)
		}
		duplicates += dbDups

		// Authoritative database admission list:
		// Only admit leads whose IDs were actually inserted into PostgreSQL.
		// If the application restarted and an existing duplicate was re-submitted,
		// the database skips it and we DO NOT admit it into the in-memory dispatch queue.
		admittedMap = make(map[string]*model.Lead)
		insertedIDSet := make(map[string]bool, len(insertedIDs))
		for _, id := range insertedIDs {
			insertedIDSet[id] = true
		}

		for normPhone, l := range normMap {
			if insertedIDSet[l.ID] {
				admittedMap[normPhone] = l
			}
		}
	}

	accepted := 0
	for normPhone, l := range admittedMap {
		e.phoneIndex[normPhone] = l.ID
		e.leads[l.ID] = l
		e.pendingQueue = append(e.pendingQueue, l)
		accepted++
	}

	// Sort pending queue by priority descending (VIP leads first)
	sort.SliceStable(e.pendingQueue, func(i, j int) bool {
		return e.pendingQueue[i].Priority > e.pendingQueue[j].Priority
	})

	metrics.DefaultMetrics.AddLeads(accepted)
	logger.L.Info("leads_ingested_idempotent",
		"campaign_id", e.campaign.ID,
		"accepted", accepted,
		"duplicates_skipped", duplicates,
	)
	return accepted, duplicates, nil
}

// Start begins the predictive dispatch ticker loop with strict concurrency locking
func (e *CampaignEngine) Start() error {
	e.mu.Lock()
	if e.isClosed {
		e.mu.Unlock()
		return fmt.Errorf("engine is closed")
	}
	if e.started {
		e.mu.Unlock()
		return nil
	}
	e.started = true
	e.campaign.Status = model.CampaignRunning
	repo := e.repo
	campaignID := e.campaign.ID
	e.mu.Unlock()

	if repo != nil {
		if err := repo.UpdateCampaignStatus(context.Background(), campaignID, model.CampaignRunning); err != nil {
			e.mu.Lock()
			e.started = false
			e.mu.Unlock()
			return fmt.Errorf("failed updating campaign status in repository: %w", err)
		}
	}

	go e.dispatchLoop()
	return nil
}

// IsRunning reports whether the engine's background dispatch loop is actively running
func (e *CampaignEngine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.started && !e.isClosed
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
	if e.isClosed || e.campaign.Status != model.CampaignRunning {
		e.mu.Unlock()
		return
	}

	idle := int(atomic.LoadInt32(&e.idleAgents))
	active := int(atomic.LoadInt32(&e.activeCalls))
	inFlight := int(atomic.LoadInt32(&e.inFlightDials))

	// Ask predictive pacer for gross dials needed based on Little's Law / Erlang-C capacity model
	grossDialsNeeded := e.pacer.CalculateDialsNeeded(idle, active)

	// Deduct outstanding ringing calls to prevent dispatch burst over-subscription
	netDialsNeeded := grossDialsNeeded - inFlight
	if netDialsNeeded <= 0 {
		e.mu.Unlock()
		return
	}

	repo := e.repo
	workerID := e.workerID
	campaignID := e.campaign.ID
	e.mu.Unlock()

	var eligible []*model.Lead

	// 1. Authoritative Distributed Path: When a persistent repository is configured,
	// dynamically claim pending leads directly from PostgreSQL via SELECT FOR UPDATE SKIP LOCKED.
	// This coordinates seamlessly across multiple running server instances with zero collision or double dialing.
	if repo != nil {
		claimCtx, claimCancel := context.WithTimeout(e.ctx, 5*time.Second)
		claimed, err := repo.ClaimPendingLeads(claimCtx, campaignID, netDialsNeeded, workerID)
		claimCancel()
		if err != nil {
			if e.ctx.Err() == nil {
				logger.L.Error("failed_claiming_distributed_leads_in_tick", "campaign_id", campaignID, "error", err)
			}
		} else {
			eligible = claimed
		}
	} else {
		// 2. Pure in-memory standalone mode fallback (for isolated mock harness tests)
		e.mu.Lock()
		now := time.Now().UTC()
		var deferred []*model.Lead
		for _, lead := range e.pendingQueue {
			if lead.NextAttemptAt != nil && now.Before(*lead.NextAttemptAt) {
				deferred = append(deferred, lead)
			} else if len(eligible) < netDialsNeeded {
				eligible = append(eligible, lead)
			} else {
				deferred = append(deferred, lead)
			}
		}
		e.pendingQueue = deferred
		e.mu.Unlock()
	}

	if len(eligible) == 0 {
		return
	}

	e.mu.Lock()
	if e.isClosed {
		e.mu.Unlock()
		return
	}

	for _, lead := range eligible {
		e.leads[lead.ID] = lead
		atomic.AddInt32(&e.inFlightDials, 1)
		e.wg.Add(1)
		go e.dispatchLead(lead)
	}
	e.mu.Unlock()
}


// persistCallOutcome updates lead status and records call attempts in PostgreSQL with atomic transactional persistence and error tracking
func (e *CampaignEngine) persistCallOutcome(ctx context.Context, leadID string, status model.CallStatus, reason string, nextAttempt *time.Time, attempt *model.CallAttempt, attempts int) {
	if e.repo == nil {
		return
	}
	persistCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := e.repo.RecordCallOutcomeTx(persistCtx, leadID, status, reason, nextAttempt, attempt, attempts); err != nil {
		metrics.DefaultMetrics.IncStorageErrors()
		logger.L.Error("failed_recording_call_outcome_tx", "lead_id", leadID, "status", status, "error", err)
	}
}

// HydrateLeads restores leads from storage into the engine's memory structures on server startup or recovery
func (e *CampaignEngine) HydrateLeads(leads []*model.Lead) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now().UTC()
	for _, l := range leads {
		normPhone := dnc.NormalizeNumber(l.PhoneNumber)
		if normPhone == "" {
			normPhone = l.PhoneNumber
		}
		if _, exists := e.phoneIndex[normPhone]; exists {
			continue
		}
		e.phoneIndex[normPhone] = l.ID
		e.leads[l.ID] = l

		// If lead was DIALING from a crashed previous instance with expired lease, reclaim it for queue
		if l.Status == model.StatusDialing {
			if l.LockedAt == nil || now.Sub(*l.LockedAt) > 2*time.Minute {
				l.Status = model.StatusQueued
				l.LockedBy = ""
				l.LockedAt = nil
			}
		}

		if l.Status == model.StatusQueued || l.Status == "" || l.Status == "DRAFT" {
			e.pendingQueue = append(e.pendingQueue, l)
		} else if l.Status == model.StatusDLQ {
			e.dlq = append(e.dlq, l)
		}
	}

	sort.Slice(e.pendingQueue, func(i, j int) bool {
		if e.pendingQueue[i].Priority != e.pendingQueue[j].Priority {
			return e.pendingQueue[i].Priority > e.pendingQueue[j].Priority
		}
		return e.pendingQueue[i].CreatedAt.Before(e.pendingQueue[j].CreatedAt)
	})
}

// ClaimBatchDistributed performs atomic database-level lead claiming using SELECT FOR UPDATE SKIP LOCKED
func (e *CampaignEngine) ClaimBatchDistributed(ctx context.Context, limit int, workerID string) ([]*model.Lead, error) {
	if e.repo == nil {
		return nil, fmt.Errorf("no persistent repository configured for distributed claiming")
	}
	return e.repo.ClaimPendingLeads(ctx, e.campaign.ID, limit, workerID)
}

func (e *CampaignEngine) dispatchLead(lead *model.Lead) {
	defer e.wg.Done()
	defer atomic.AddInt32(&e.inFlightDials, -1)

	metrics.DefaultMetrics.IncActiveDialer()
	defer metrics.DefaultMetrics.DecActiveDialer()

	// 1. DNC Screening
	isDNC := false
	if e.dncFilter != nil && e.dncFilter.Contains(lead.PhoneNumber) {
		isDNC = true
	} else if e.repo != nil {
		dncCtx, dncCancel := context.WithTimeout(e.ctx, 2*time.Second)
		prohibited, err := e.repo.IsDNCProhibited(dncCtx, lead.PhoneNumber)
		dncCancel()
		if err != nil {
			// Fail-closed on database error: NEVER dial when DNC status cannot be verified!
			metrics.DefaultMetrics.IncStorageErrors()
			logger.L.Error("dnc_check_failed_deferring_lead_fail_closed", "lead_id", lead.ID, "phone", lead.PhoneNumber, "error", err)

			// Defer lead with short backoff (e.g. 1 minute) to re-verify compliance later without burning an attempt
			retryTime := time.Now().UTC().Add(1 * time.Minute)
			e.mu.Lock()
			lead.NextAttemptAt = &retryTime
			lead.Status = model.StatusQueued
			lead.LockedBy = ""
			lead.LockedAt = nil
			e.pendingQueue = append(e.pendingQueue, lead)
			e.mu.Unlock()

			// Safely release row in PostgreSQL with bounded context
			reschedCtx, reschedCancel := context.WithTimeout(e.ctx, 2*time.Second)
			if rErr := e.repo.RescheduleLead(reschedCtx, lead.ID, retryTime); rErr != nil {
				logger.L.Error("failed_rescheduling_lead_after_dnc_error", "lead_id", lead.ID, "error", rErr)
				metrics.DefaultMetrics.IncStorageErrors()
			}
			reschedCancel()
			return
		}
		if prohibited {
			isDNC = true
			if e.dncFilter != nil {
				e.dncFilter.Add(lead.PhoneNumber)
			}
		}
	}

	if isDNC {
		e.mu.Lock()
		lead.Status = model.StatusDLQ
		lead.FailureReason = "DNC_PROHIBITED"
		e.dlq = append(e.dlq, lead)
		e.mu.Unlock()

		metrics.DefaultMetrics.IncDNCHit()
		e.pacer.RecordOutcome(model.StatusFailed)
		e.persistCallOutcome(context.Background(), lead.ID, model.StatusDLQ, lead.FailureReason, nil, &model.CallAttempt{
			ID:           uuid.NewString(),
			CampaignID:   e.campaign.ID,
			LeadID:       lead.ID,
			PhoneNumber:  lead.PhoneNumber,
			Status:       model.StatusDLQ,
			ErrorMessage: "DNC_PROHIBITED",
			CreatedAt:    time.Now().UTC(),
		}, lead.Attempts)
		logger.L.Warn("dial_routed_to_dlq_dnc", "lead_id", lead.ID, "phone", lead.PhoneNumber)
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
			lead.Status = model.StatusQueued
			lead.LockedBy = ""
			lead.LockedAt = nil
			e.pendingQueue = append(e.pendingQueue, lead)
			e.mu.Unlock()

			if e.repo != nil {
				reschedCtx, reschedCancel := context.WithTimeout(e.ctx, 2*time.Second)
				if err := e.repo.RescheduleLead(reschedCtx, lead.ID, nextPermissible); err != nil {
					metrics.DefaultMetrics.IncStorageErrors()
					logger.L.Error("failed_rescheduling_timezone_blocked_lead", "lead_id", lead.ID, "error", err)
				}
				reschedCancel()
			}
			return
		}
	}

	atomic.AddInt32(&e.activeCalls, 1)
	defer atomic.AddInt32(&e.activeCalls, -1)

	metrics.DefaultMetrics.IncDial()
	e.mu.Lock()
	lead.Status = model.StatusDialing
	now := time.Now().UTC()
	lead.LastAttemptAt = &now
	lead.Attempts++
	currentAttempts := lead.Attempts
	e.mu.Unlock()

	// Durable attempt-start persistence: write attempt increment and in-progress record to DB before carrier dial
	attemptID := uuid.NewString()
	if e.repo != nil {
		attemptCtx, attemptCancel := context.WithTimeout(e.ctx, 3*time.Second)
		err := e.repo.RecordAttemptStart(attemptCtx, lead.ID, attemptID, currentAttempts)
		attemptCancel()
		if err != nil {
			metrics.DefaultMetrics.IncStorageErrors()
			logger.L.Error("failed_recording_attempt_start_aborting_dial", "lead_id", lead.ID, "attempt_id", attemptID, "error", err)

			// Abort dial: revert in-memory attempt increment and reschedule safely
			e.mu.Lock()
			lead.Attempts--
			retryTime := time.Now().UTC().Add(30 * time.Second)
			lead.NextAttemptAt = &retryTime
			lead.Status = model.StatusQueued
			lead.LockedBy = ""
			lead.LockedAt = nil
			e.pendingQueue = append(e.pendingQueue, lead)
			e.mu.Unlock()

			// Safely release row lock in PostgreSQL with bounded context
			reschedCtx, reschedCancel := context.WithTimeout(e.ctx, 2*time.Second)
			if rErr := e.repo.RescheduleLead(reschedCtx, lead.ID, retryTime); rErr != nil {
				logger.L.Error("failed_rescheduling_lead_after_attempt_start_error", "lead_id", lead.ID, "error", rErr)
				metrics.DefaultMetrics.IncStorageErrors()
			}
			reschedCancel()
			return
		}
	}

	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
	defer cancel()

	status, ringDuration, err := e.dialer.Dial(ctx, lead)

	var finalStatus model.CallStatus
	var talkDuration time.Duration
	var attemptErr string

	if err != nil {
		finalStatus = model.StatusDLQ
		attemptErr = "CARRIER_TRUNK_ERROR: " + err.Error()

		e.mu.Lock()
		lead.Status = finalStatus
		lead.FailureReason = attemptErr
		e.dlq = append(e.dlq, lead)
		e.mu.Unlock()

		e.pacer.RecordOutcome(model.StatusFailed)
		e.persistCallOutcome(context.Background(), lead.ID, finalStatus, attemptErr, nil, &model.CallAttempt{
			ID:             attemptID,
			CampaignID:     e.campaign.ID,
			LeadID:         lead.ID,
			PhoneNumber:    lead.PhoneNumber,
			Status:         finalStatus,
			RingDurationMs: int(ringDuration.Milliseconds()),
			ErrorMessage:   attemptErr,
			CreatedAt:      time.Now().UTC(),
		}, currentAttempts)
		return
	}

	// 3. Retry with Exponential Backoff on Busy or No Answer if attempts remain
	if status == model.StatusBusy || status == model.StatusNoAnswer {
		if lead.Attempts < lead.MaxAttempts {
			// Exponential backoff: base 15m * 2^(attempts-1) with max 4h cap and 10% jitter
			shift := lead.Attempts - 1
			if shift > 5 {
				shift = 5
			}
			delay := (15 * time.Minute) * time.Duration(1<<shift)
			jitter := time.Duration(rand.Int63n(int64(delay / 10)))
			retryTime := time.Now().UTC().Add(delay + jitter)

			finalStatus = model.StatusQueued
			e.mu.Lock()
			lead.NextAttemptAt = &retryTime
			lead.Status = finalStatus
			lead.LockedBy = ""
			lead.LockedAt = nil
			e.pendingQueue = append(e.pendingQueue, lead)
			e.mu.Unlock()

			// Record outcome exactly once
			e.pacer.RecordOutcome(status)
			e.persistCallOutcome(context.Background(), lead.ID, finalStatus, "", &retryTime, &model.CallAttempt{
				ID:             attemptID,
				CampaignID:     e.campaign.ID,
				LeadID:         lead.ID,
				PhoneNumber:    lead.PhoneNumber,
				Status:         status,
				RingDurationMs: int(ringDuration.Milliseconds()),
				CreatedAt:      time.Now().UTC(),
			}, currentAttempts)
			logger.L.Info("lead_retry_scheduled", "lead_id", lead.ID, "attempt", lead.Attempts, "retry_at", retryTime)
			return
		}

		// Attempts exhausted: route to Dead-Letter Queue
		finalStatus = model.StatusDLQ
		attemptErr = "EXHAUSTED_MAX_ATTEMPTS"
		e.mu.Lock()
		lead.Status = finalStatus
		lead.FailureReason = attemptErr
		lead.LockedBy = ""
		lead.LockedAt = nil
		e.dlq = append(e.dlq, lead)
		e.mu.Unlock()

		// Record outcome exactly once
		e.pacer.RecordOutcome(status)
		e.persistCallOutcome(context.Background(), lead.ID, finalStatus, attemptErr, nil, &model.CallAttempt{
			ID:             attemptID,
			CampaignID:     e.campaign.ID,
			LeadID:         lead.ID,
			PhoneNumber:    lead.PhoneNumber,
			Status:         finalStatus,
			RingDurationMs: int(ringDuration.Milliseconds()),
			ErrorMessage:   attemptErr,
			CreatedAt:      time.Now().UTC(),
		}, currentAttempts)
		logger.L.Warn("lead_routed_to_dlq_exhausted", "lead_id", lead.ID, "attempts", lead.Attempts)
		return
	}

	if status == model.StatusVoicemail {
		finalStatus = status
		e.mu.Lock()
		lead.Status = finalStatus
		lead.LockedBy = ""
		lead.LockedAt = nil
		e.mu.Unlock()

		e.pacer.RecordOutcome(status)
		e.persistCallOutcome(context.Background(), lead.ID, finalStatus, "", nil, &model.CallAttempt{
			ID:             attemptID,
			CampaignID:     e.campaign.ID,
			LeadID:         lead.ID,
			PhoneNumber:    lead.PhoneNumber,
			Status:         finalStatus,
			RingDurationMs: int(ringDuration.Milliseconds()),
			CreatedAt:      time.Now().UTC(),
		}, currentAttempts)
		return
	}

	if status == model.StatusAnswered {
		// Attempt to allocate idle AI agent slot
		if atomic.AddInt32(&e.idleAgents, -1) >= 0 {
			// Successfully connected: record CONNECTED as final disposition
			finalStatus = model.StatusConnected
			e.mu.Lock()
			lead.Status = finalStatus
			lead.LockedBy = ""
			lead.LockedAt = nil
			e.mu.Unlock()

			e.pacer.RecordOutcome(model.StatusConnected)
			metrics.DefaultMetrics.IncConnect()
			logger.L.Info("call_connected", "lead_id", lead.ID, "campaign_id", e.campaign.ID)

			// Simulate AI call handling (30ms - 60ms)
			talkDuration = time.Duration(30+rand.Intn(30)) * time.Millisecond
			select {
			case <-time.After(talkDuration):
			case <-e.ctx.Done():
			}
			atomic.AddInt32(&e.idleAgents, 1)

			e.persistCallOutcome(context.Background(), lead.ID, finalStatus, "", nil, &model.CallAttempt{
				ID:             attemptID,
				CampaignID:     e.campaign.ID,
				LeadID:         lead.ID,
				PhoneNumber:    lead.PhoneNumber,
				Status:         finalStatus,
				RingDurationMs: int(ringDuration.Milliseconds()),
				TalkDurationMs: int(talkDuration.Milliseconds()),
				CreatedAt:      time.Now().UTC(),
			}, currentAttempts)
		} else {
			// No agent was available when customer answered -> Abandoned Call!
			atomic.AddInt32(&e.idleAgents, 1)
			finalStatus = model.StatusAbandoned
			attemptErr = "NO_AGENT_AVAILABLE"

			e.mu.Lock()
			lead.Status = finalStatus
			lead.LockedBy = ""
			lead.LockedAt = nil
			e.mu.Unlock()

			e.pacer.RecordOutcome(model.StatusAbandoned)
			metrics.DefaultMetrics.IncAbandon()
			logger.L.Warn("call_abandoned", "lead_id", lead.ID, "campaign_id", e.campaign.ID)

			e.persistCallOutcome(context.Background(), lead.ID, finalStatus, attemptErr, nil, &model.CallAttempt{
				ID:             attemptID,
				CampaignID:     e.campaign.ID,
				LeadID:         lead.ID,
				PhoneNumber:    lead.PhoneNumber,
				Status:         finalStatus,
				RingDurationMs: int(ringDuration.Milliseconds()),
				ErrorMessage:   attemptErr,
				CreatedAt:      time.Now().UTC(),
			}, currentAttempts)
		}
		return
	}

	// Fallback terminal status
	e.mu.Lock()
	lead.Status = status
	lead.LockedBy = ""
	lead.LockedAt = nil
	e.mu.Unlock()
	e.pacer.RecordOutcome(status)
	e.persistCallOutcome(context.Background(), lead.ID, status, "", nil, &model.CallAttempt{
		ID:             attemptID,
		CampaignID:     e.campaign.ID,
		LeadID:         lead.ID,
		PhoneNumber:    lead.PhoneNumber,
		Status:         status,
		RingDurationMs: int(ringDuration.Milliseconds()),
		CreatedAt:      time.Now().UTC(),
	}, currentAttempts)
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

// GetLead retrieves a point-in-time copy of an individual lead's state
func (e *CampaignEngine) GetLead(leadID string) (model.Lead, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	lead, ok := e.leads[leadID]
	if !ok {
		return model.Lead{}, false
	}
	return *lead, true
}

// GetDLQ returns all leads currently routed to the Dead-Letter Queue
func (e *CampaignEngine) GetDLQ() []model.Lead {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]model.Lead, len(e.dlq))
	for i, l := range e.dlq {
		out[i] = *l
	}
	return out
}

// Stop gracefully terminates campaign execution and drains active workers safely
func (e *CampaignEngine) Stop() {
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.campaign.Status = model.CampaignPaused
		e.isClosed = true
		e.started = false
		e.mu.Unlock()

		e.cancel()
		close(e.stopCh)
		e.wg.Wait()
		logger.L.Info("campaign_stopped", "campaign_id", e.campaign.ID)
	})
}
