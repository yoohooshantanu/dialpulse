package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yoohooshantanu/dialpulse/internal/model"
)

// CampaignRepository abstracts PostgreSQL persistence for campaigns and leads
type CampaignRepository interface {
	SaveCampaign(ctx context.Context, camp *model.Campaign) error
	GetCampaign(ctx context.Context, id string) (*model.Campaign, error)
	IngestLeadsIdempotent(ctx context.Context, leads []*model.Lead) (inserted int, duplicates int, err error)
	UpdateLeadStatus(ctx context.Context, leadID string, status model.CallStatus, reason string) error
	GetDeadLetterQueue(ctx context.Context, campaignID string) ([]*model.Lead, error)
}

// MemoryPostgresRepository provides an in-memory replica with identical relational semantics and idempotency constraints
type MemoryPostgresRepository struct {
	mu         sync.RWMutex
	campaigns  map[string]*model.Campaign
	leads      map[string]*model.Lead
	phoneIndex map[string]string // Key: campaignID + ":" + phone
	dlq        map[string][]*model.Lead
}

func NewMemoryPostgresRepository() *MemoryPostgresRepository {
	return &MemoryPostgresRepository{
		campaigns:  make(map[string]*model.Campaign),
		leads:      make(map[string]*model.Lead),
		phoneIndex: make(map[string]string),
		dlq:        make(map[string][]*model.Lead),
	}
}

func (r *MemoryPostgresRepository) SaveCampaign(ctx context.Context, camp *model.Campaign) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.campaigns[camp.ID] = camp
	return nil
}

func (r *MemoryPostgresRepository) GetCampaign(ctx context.Context, id string) (*model.Campaign, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.campaigns[id]
	if !ok {
		return nil, model.ErrCampaignNotFound
	}
	return c, nil
}

func (r *MemoryPostgresRepository) IngestLeadsIdempotent(ctx context.Context, leads []*model.Lead) (int, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	inserted := 0
	duplicates := 0

	for _, l := range leads {
		key := fmt.Sprintf("%s:%s", l.CampaignID, l.PhoneNumber)
		if _, exists := r.phoneIndex[key]; exists {
			duplicates++
			continue
		}

		r.phoneIndex[key] = l.ID
		r.leads[l.ID] = l
		inserted++
	}

	return inserted, duplicates, nil
}

func (r *MemoryPostgresRepository) UpdateLeadStatus(ctx context.Context, leadID string, status model.CallStatus, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	lead, ok := r.leads[leadID]
	if !ok {
		return model.ErrLeadNotFound
	}

	lead.Status = status
	lead.FailureReason = reason
	now := time.Now().UTC()
	lead.LastAttemptAt = &now

	if status == model.StatusDLQ || status == model.StatusFailed {
		r.dlq[lead.CampaignID] = append(r.dlq[lead.CampaignID], lead)
	}

	return nil
}

func (r *MemoryPostgresRepository) GetDeadLetterQueue(ctx context.Context, campaignID string) ([]*model.Lead, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dlqLeads := r.dlq[campaignID]
	res := make([]*model.Lead, len(dlqLeads))
	copy(res, dlqLeads)
	return res, nil
}
