package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrCampaignNotFound      = errors.New("campaign not found")
	ErrLeadNotFound          = errors.New("lead not found")
	ErrCampaignNotRunning    = errors.New("campaign is not in running state")
	ErrDNCViolation          = errors.New("phone number is listed in Do-Not-Call registry")
	ErrTimezoneRestricted    = errors.New("recipient local time is outside permitted dialing window (08:00 - 21:00)")
	ErrUnknownTimezone       = errors.New("unknown recipient area code: local timezone cannot be determined")
	ErrStaleLease            = errors.New("stale lease token or invalid lead state: outcome rejected")
	ErrInvalidCampaignConfig = errors.New("invalid campaign configuration")
)

const (
	// -------
	// DefaultLeaseTTL specifies the authoritative lease duration (2 minutes) for a claimed lead in DIALING status.
	// Used consistently across claiming, expiration recovery, and lease fencing validation.
	// -------
	DefaultLeaseTTL = 2 * time.Minute

	// -------
	// TelephonyDialTimeout specifies the strict bounded deadline (10 seconds) for external carrier dialing.
	// Strictly bounded well within DefaultLeaseTTL to guarantee an active dial completes before lease expiry.
	// -------
	TelephonyDialTimeout = 10 * time.Second
)

type CallStatus string

const (
	StatusQueued    CallStatus = "QUEUED"
	StatusDialing   CallStatus = "DIALING"
	StatusRinging   CallStatus = "RINGING"
	StatusAnswered  CallStatus = "ANSWERED"
	StatusConnected CallStatus = "CONNECTED"
	StatusBusy      CallStatus = "BUSY"
	StatusNoAnswer  CallStatus = "NO_ANSWER"
	StatusVoicemail CallStatus = "VOICEMAIL_AMD"
	StatusAbandoned CallStatus = "ABANDONED"
	StatusCompleted CallStatus = "COMPLETED"
	StatusFailed    CallStatus = "FAILED"
	StatusDLQ       CallStatus = "DEAD_LETTER_QUEUE"
)

type CampaignStatus string

const (
	CampaignDraft     CampaignStatus = "DRAFT"
	CampaignRunning   CampaignStatus = "RUNNING"
	CampaignPaused    CampaignStatus = "PAUSED"
	CampaignCompleted CampaignStatus = "COMPLETED"
)

type DialingMode string

const (
	ModePredictive  DialingMode = "PREDICTIVE"
	ModeProgressive DialingMode = "PROGRESSIVE"
	ModePreview     DialingMode = "PREVIEW"
)

type Lead struct {
	ID            string     `json:"id"`
	CampaignID    string     `json:"campaign_id"`
	PhoneNumber   string     `json:"phone_number"`
	Name          string     `json:"name,omitempty"`
	Priority      int        `json:"priority"` // Higher dialed first (e.g., 100 vs 10)
	Status        CallStatus `json:"status"`
	Attempts      int        `json:"attempts"`
	MaxAttempts   int        `json:"max_attempts"`
	FailureReason string     `json:"failure_reason,omitempty"`
	LockedBy      string     `json:"locked_by,omitempty"`
	LockedAt      *time.Time `json:"locked_at,omitempty"`
	LeaseToken    string     `json:"lease_token,omitempty"`
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
}

type CallAttempt struct {
	ID             string     `json:"id"`
	CampaignID     string     `json:"campaign_id"`
	LeadID         string     `json:"lead_id"`
	PhoneNumber    string     `json:"phone_number"`
	Status         CallStatus `json:"status"`
	RingDurationMs int        `json:"ring_duration_ms"`
	TalkDurationMs int        `json:"talk_duration_ms"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

type Campaign struct {
	ID                   string         `json:"id"`
	TenantID             string         `json:"tenant_id"`
	Name                 string         `json:"name"`
	Mode                 DialingMode    `json:"mode"`
	Status               CampaignStatus `json:"status"`
	TargetAbandonRate    float64        `json:"target_abandon_rate"` // e.g., 0.025 (2.5%)
	MaxDialMultiplier    float64        `json:"max_dial_multiplier"` // e.g., 3.5
	AllocatedAgents      int            `json:"allocated_agents"`   // Available AI agent concurrency slots
	AverageHandleSec     float64        `json:"average_handle_sec"` // Expected call length in seconds
	AverageRingSec       float64        `json:"average_ring_sec"`   // Expected ring time in seconds
	HistoricalAnswerRate float64        `json:"historical_answer_rate"` // e.g. 0.20 (20%)
	CreatedAt            time.Time      `json:"created_at"`
}

type CampaignStats struct {
	CampaignID            string    `json:"campaign_id"`
	TotalLeads            int64     `json:"total_leads"`
	DialedCalls           int64     `json:"dialed_calls"`
	AnsweredCalls         int64     `json:"answered_calls"`
	ConnectedCalls        int64     `json:"connected_calls"`
	AbandonedCalls        int64     `json:"abandoned_calls"`
	CurrentAbandonRate    float64   `json:"current_abandon_rate"`
	CurrentDialMultiplier float64   `json:"current_dial_multiplier"`
	ComputedAnswerRate    float64   `json:"computed_answer_rate"`
	IdleAgents            int       `json:"idle_agents"`
	ActiveCalls           int       `json:"active_calls"`
	UpdatedAt             time.Time `json:"updated_at"`
}

// OutboxEvent represents an atomic domain event queued for reliable asynchronous dispatch
type OutboxEvent struct {
	ID            string          `json:"id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
	PublishedAt   *time.Time      `json:"published_at,omitempty"`
}

// DeterministicOutboxEventID generates a deterministic, collision-free event ID
// based on attempt ID and event type, ensuring ON CONFLICT DO NOTHING idempotency.
func DeterministicOutboxEventID(attemptID, eventType string) string {
	return fmt.Sprintf("evt_%s_%s", attemptID, eventType)
}

