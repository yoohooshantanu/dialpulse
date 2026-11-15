package pacer

import (
	"math"
	"sync"
	"time"

	"github.com/yoohooshantanu/dialpulse/internal/logger"
	"github.com/yoohooshantanu/dialpulse/internal/model"
)

// PredictivePacer calculates optimal outbound lines to dial balancing agent utilization and legal drop-rate caps
type PredictivePacer struct {
	mu           sync.RWMutex
	campaignID   string
	cfg          *model.Campaign
	dialed       int64
	answered     int64
	connected    int64
	abandoned    int64
	activeAgents int
	activeCalls  int
}

func NewPredictivePacer(campaign *model.Campaign) *PredictivePacer {
	if campaign.TargetAbandonRate <= 0 {
		campaign.TargetAbandonRate = 0.025 // 2.5% FTC legal limit
	}
	if campaign.MaxDialMultiplier <= 0 {
		campaign.MaxDialMultiplier = 3.5
	}
	if campaign.AverageHandleSec <= 0 {
		campaign.AverageHandleSec = 90.0
	}
	if campaign.AverageRingSec <= 0 {
		campaign.AverageRingSec = 15.0
	}
	if campaign.HistoricalAnswerRate <= 0 {
		campaign.HistoricalAnswerRate = 0.20 // default 20%
	}

	return &PredictivePacer{
		campaignID:   campaign.ID,
		cfg:          campaign,
		activeAgents: campaign.AllocatedAgents,
	}
}

// RecordOutcome updates rolling performance metrics from completed calls
func (p *PredictivePacer) RecordOutcome(status model.CallStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dialed++
	switch status {
	case model.StatusAnswered, model.StatusConnected:
		p.answered++
		p.connected++
	case model.StatusAbandoned:
		p.answered++
		p.abandoned++
	}
}

func (p *PredictivePacer) UpdateAgentState(idleAgents, activeCalls int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.activeCalls = activeCalls
}

// CalculateDialsNeeded computes the exact number of leads to dial on this scheduling tick
func (p *PredictivePacer) CalculateDialsNeeded(idleAgents, activeCalls int) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.activeCalls = activeCalls

	if idleAgents <= 0 && activeCalls >= p.cfg.AllocatedAgents {
		// No idle capacity
		return 0
	}

	// 1. Calculate rolling answer rate with Bayesian prior weighting
	answerRate := p.cfg.HistoricalAnswerRate
	if p.dialed > 50 && p.answered > 0 {
		rollingAnswer := float64(p.answered) / float64(p.dialed)
		// 80% weight on rolling historical, 20% prior
		answerRate = (rollingAnswer * 0.8) + (p.cfg.HistoricalAnswerRate * 0.2)
	}
	if answerRate < 0.05 {
		answerRate = 0.05 // Floor at 5% to prevent division runaway
	}

	// 2. Calculate current abandonment rate: Abandoned / Total Answered
	abandonRate := 0.0
	if p.answered > 0 {
		abandonRate = float64(p.abandoned) / float64(p.answered)
	}

	// 3. Estimate agents expected to become free during the ring window:
	// Based on Little's Law / Erlang queue dynamics:
	// Expected Freeing Soon = ActiveCalls * (RingDuration / AHT)
	freeingSoon := float64(activeCalls) * (p.cfg.AverageRingSec / p.cfg.AverageHandleSec)
	effectiveCapacity := float64(idleAgents) + freeingSoon

	// 4. Compute baseline multiplier
	multiplier := effectiveCapacity / answerRate

	// 5. Apply Legal Drop-Rate Governor (Brake Factor)
	// If abandonment is near or exceeds target, aggressively throttle multiplier
	if abandonRate >= p.cfg.TargetAbandonRate*0.9 {
		// Approaching ceiling: throttle multiplier by penalty ratio
		brake := 1.0 - math.Min(1.0, (abandonRate-p.cfg.TargetAbandonRate*0.9)/(p.cfg.TargetAbandonRate*0.3))
		multiplier *= math.Max(0.2, brake)
	}
	if abandonRate >= p.cfg.TargetAbandonRate {
		// Hard safety cap: fall back to progressive mode (1 dial per idle agent)
		multiplier = float64(idleAgents)
	}

	// Enforce maximum multiplier ceiling
	maxAllowedDials := float64(p.cfg.AllocatedAgents) * p.cfg.MaxDialMultiplier
	dials := int(math.Floor(math.Min(multiplier, maxAllowedDials)))

	if dials < 0 {
		dials = 0
	}
	// At least 1 dial if there are idle agents
	if dials == 0 && idleAgents > 0 {
		dials = idleAgents
	}

	logger.L.Debug("pacer_calculated_dials",
		"campaign_id", p.campaignID,
		"idle_agents", idleAgents,
		"active_calls", activeCalls,
		"freeing_soon", freeingSoon,
		"answer_rate", answerRate,
		"abandon_rate", abandonRate,
		"dials_needed", dials,
	)

	return dials
}

// GetStats returns live statistical snapshots for monitoring dashboards
func (p *PredictivePacer) GetStats(idleAgents, activeCalls int) *model.CampaignStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ansRate := p.cfg.HistoricalAnswerRate
	if p.dialed > 0 {
		ansRate = float64(p.answered) / float64(p.dialed)
	}

	abRate := 0.0
	if p.answered > 0 {
		abRate = float64(p.abandoned) / float64(p.answered)
	}

	mult := 1.0
	if p.cfg.AllocatedAgents > 0 {
		mult = float64(activeCalls) / float64(p.cfg.AllocatedAgents)
	}

	return &model.CampaignStats{
		CampaignID:            p.campaignID,
		DialedCalls:           p.dialed,
		AnsweredCalls:         p.answered,
		ConnectedCalls:        p.connected,
		AbandonedCalls:        p.abandoned,
		CurrentAbandonRate:    abRate,
		CurrentDialMultiplier: mult,
		ComputedAnswerRate:    ansRate,
		IdleAgents:            idleAgents,
		ActiveCalls:           activeCalls,
		UpdatedAt:             time.Now().UTC(),
	}
}
