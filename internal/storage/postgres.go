package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yoohooshantanu/dialpulse/internal/dnc"
	"github.com/yoohooshantanu/dialpulse/internal/logger"
	"github.com/yoohooshantanu/dialpulse/internal/model"
)

// CampaignRepository abstracts PostgreSQL persistence for campaigns, leads, and call attempts
type CampaignRepository interface {
	SaveCampaign(ctx context.Context, camp *model.Campaign) error
	GetCampaign(ctx context.Context, id string) (*model.Campaign, error)
	ListActiveCampaigns(ctx context.Context) ([]*model.Campaign, error)
	UpdateCampaignStatus(ctx context.Context, campaignID string, status model.CampaignStatus) error
	GetActiveLeads(ctx context.Context, campaignID string) ([]*model.Lead, error)
	IngestLeadsIdempotent(ctx context.Context, leads []*model.Lead) (insertedIDs []string, duplicates int, err error)
	ClaimPendingLeads(ctx context.Context, campaignID string, limit int, workerID string) ([]*model.Lead, error)
	ReclaimExpiredLeases(ctx context.Context, campaignID string, leaseTTL time.Duration) (int64, error)
	RescheduleLead(ctx context.Context, leadID string, nextAttempt time.Time, leaseToken string) error
	RecordCallAttempt(ctx context.Context, attempt *model.CallAttempt) error
	RecordAttemptStart(ctx context.Context, leadID string, attemptID string, attemptNum int, leaseToken string) error
	RecordCallOutcomeTx(ctx context.Context, leadID string, status model.CallStatus, reason string, nextAttempt *time.Time, attempt *model.CallAttempt, attempts int, leaseToken string) error
	GetDeadLetterQueue(ctx context.Context, campaignID string) ([]*model.Lead, error)
	AddDNCNumbers(ctx context.Context, numbers []string) error
	GetDNCNumbers(ctx context.Context) ([]string, error)
	IsDNCProhibited(ctx context.Context, phone string) (bool, error)
	Close()
}

// PostgresRepository implements CampaignRepository with connection pooling and atomic database transactions
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Pool exposes the underlying connection pool for health checks and testing
func (r *PostgresRepository) Pool() *pgxpool.Pool {
	return r.pool
}

func NewPostgresRepositoryFromURL(ctx context.Context, databaseURL string) (*PostgresRepository, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid postgres configuration: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	logger.L.Info("postgres_connected_successfully", "max_conns", config.MaxConns)
	return &PostgresRepository{pool: pool}, nil
}

func (r *PostgresRepository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

func (r *PostgresRepository) SaveCampaign(ctx context.Context, camp *model.Campaign) error {
	query := `
		INSERT INTO campaigns (
			id, tenant_id, name, mode, status, target_abandon_rate,
			max_dial_multiplier, allocated_agents, average_handle_sec,
			average_ring_sec, historical_answer_rate, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			allocated_agents = EXCLUDED.allocated_agents,
			updated_at = NOW();`

	tenantID := camp.TenantID
	if tenantID == "" {
		tenantID = "default"
	}
	createdAt := camp.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	_, err := r.pool.Exec(ctx, query,
		camp.ID,
		tenantID,
		camp.Name,
		string(camp.Mode),
		string(camp.Status),
		camp.TargetAbandonRate,
		camp.MaxDialMultiplier,
		camp.AllocatedAgents,
		camp.AverageHandleSec,
		camp.AverageRingSec,
		camp.HistoricalAnswerRate,
		createdAt,
	)
	return err
}

func (r *PostgresRepository) GetCampaign(ctx context.Context, id string) (*model.Campaign, error) {
	query := `
		SELECT id, tenant_id, name, mode, status, target_abandon_rate,
		       max_dial_multiplier, allocated_agents, average_handle_sec,
		       average_ring_sec, historical_answer_rate, created_at
		FROM campaigns
		WHERE id = $1;`

	row := r.pool.QueryRow(ctx, query, id)
	camp := &model.Campaign{}
	var mode, status string

	err := row.Scan(
		&camp.ID,
		&camp.TenantID,
		&camp.Name,
		&mode,
		&status,
		&camp.TargetAbandonRate,
		&camp.MaxDialMultiplier,
		&camp.AllocatedAgents,
		&camp.AverageHandleSec,
		&camp.AverageRingSec,
		&camp.HistoricalAnswerRate,
		&camp.CreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, model.ErrCampaignNotFound
		}
		return nil, err
	}

	camp.Mode = model.DialingMode(mode)
	camp.Status = model.CampaignStatus(status)
	return camp, nil
}

func (r *PostgresRepository) ListActiveCampaigns(ctx context.Context) ([]*model.Campaign, error) {
	query := `
		SELECT id, tenant_id, name, mode, status, target_abandon_rate,
		       max_dial_multiplier, allocated_agents, average_handle_sec,
		       average_ring_sec, historical_answer_rate, created_at
		FROM campaigns
		WHERE status != 'COMPLETED'
		ORDER BY created_at ASC;`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*model.Campaign
	for rows.Next() {
		camp := &model.Campaign{}
		var mode, status string
		err := rows.Scan(
			&camp.ID,
			&camp.TenantID,
			&camp.Name,
			&mode,
			&status,
			&camp.TargetAbandonRate,
			&camp.MaxDialMultiplier,
			&camp.AllocatedAgents,
			&camp.AverageHandleSec,
			&camp.AverageRingSec,
			&camp.HistoricalAnswerRate,
			&camp.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		camp.Mode = model.DialingMode(mode)
		camp.Status = model.CampaignStatus(status)
		list = append(list, camp)
	}
	return list, nil
}

func (r *PostgresRepository) UpdateCampaignStatus(ctx context.Context, campaignID string, status model.CampaignStatus) error {
	query := `UPDATE campaigns SET status = $1, updated_at = NOW() WHERE id = $2;`
	_, err := r.pool.Exec(ctx, query, string(status), campaignID)
	return err
}

func (r *PostgresRepository) GetActiveLeads(ctx context.Context, campaignID string) ([]*model.Lead, error) {
	query := `
		SELECT id, campaign_id, phone_number, name, priority,
		       status, attempts, max_attempts, failure_reason,
		       locked_by, locked_at, last_attempt_at, next_attempt_at, created_at
		FROM leads
		WHERE campaign_id = $1 AND (
			status IN ('QUEUED', 'DRAFT')
			OR (status = 'DIALING' AND locked_at IS NOT NULL AND locked_at < NOW() - INTERVAL '2 minutes')
		)
		ORDER BY priority DESC, created_at ASC;`

	rows, err := r.pool.Query(ctx, query, campaignID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*model.Lead
	for rows.Next() {
		l := &model.Lead{}
		var status string
		var lockedBy *string
		err := rows.Scan(
			&l.ID,
			&l.CampaignID,
			&l.PhoneNumber,
			&l.Name,
			&l.Priority,
			&status,
			&l.Attempts,
			&l.MaxAttempts,
			&l.FailureReason,
			&lockedBy,
			&l.LockedAt,
			&l.LastAttemptAt,
			&l.NextAttemptAt,
			&l.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		l.Status = model.CallStatus(status)
		if lockedBy != nil {
			l.LockedBy = *lockedBy
		}
		list = append(list, l)
	}
	return list, nil
}

// ClaimPendingLeads executes atomic multi-instance distributed claiming using PostgreSQL SELECT ... FOR UPDATE SKIP LOCKED.
// Multiple dialer instances or worker processes running concurrently will never lock or dial the same lead.
func (r *PostgresRepository) ClaimPendingLeads(ctx context.Context, campaignID string, limit int, workerID string) ([]*model.Lead, error) {
	if limit <= 0 {
		return nil, nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin claiming transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	claimQuery := `
		UPDATE leads
		SET status = 'DIALING',
		    locked_by = $1,
		    locked_at = NOW(),
		    lease_token = md5(random()::text || clock_timestamp()::text)
		WHERE id IN (
			SELECT id FROM leads
			WHERE campaign_id = $2
			  AND (
			    ((status = 'QUEUED' OR status = 'DRAFT') AND (next_attempt_at IS NULL OR next_attempt_at <= NOW()))
			    OR
			    (status = 'DIALING' AND locked_at IS NOT NULL AND locked_at < NOW() - INTERVAL '2 minutes')
			  )
			ORDER BY priority DESC, created_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, campaign_id, phone_number, name, priority,
		          status, attempts, max_attempts, failure_reason,
		          locked_by, locked_at, last_attempt_at, next_attempt_at, created_at, lease_token;`

	rows, err := tx.Query(ctx, claimQuery, workerID, campaignID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed querying claimable leads: %w", err)
	}
	defer rows.Close()

	var claimed []*model.Lead
	for rows.Next() {
		l := &model.Lead{}
		var status string
		var lockedBy *string
		var leaseToken *string
		err := rows.Scan(
			&l.ID,
			&l.CampaignID,
			&l.PhoneNumber,
			&l.Name,
			&l.Priority,
			&status,
			&l.Attempts,
			&l.MaxAttempts,
			&l.FailureReason,
			&lockedBy,
			&l.LockedAt,
			&l.LastAttemptAt,
			&l.NextAttemptAt,
			&l.CreatedAt,
			&leaseToken,
		)
		if err != nil {
			return nil, fmt.Errorf("failed scanning claimed lead: %w", err)
		}
		l.Status = model.CallStatus(status)
		if lockedBy != nil {
			l.LockedBy = *lockedBy
		}
		if leaseToken != nil {
			l.LeaseToken = *leaseToken
		}
		claimed = append(claimed, l)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit claiming transaction: %w", err)
	}

	return claimed, nil
}

// ReclaimExpiredLeases resets DIALING leads with expired leases (> leaseTTL) back to QUEUED for crash recovery
func (r *PostgresRepository) ReclaimExpiredLeases(ctx context.Context, campaignID string, leaseTTL time.Duration) (int64, error) {
	if leaseTTL < 0 {
		leaseTTL = 2 * time.Minute
	}
	intervalSec := float64(leaseTTL.Milliseconds()) / 1000.0
	intervalStr := fmt.Sprintf("%f seconds", intervalSec)

	query := `
		UPDATE leads
		SET status = 'QUEUED',
		    locked_by = NULL,
		    locked_at = NULL,
		    lease_token = NULL
		WHERE campaign_id = $1
		  AND status = 'DIALING'
		  AND locked_at IS NOT NULL
		  AND locked_at < NOW() - $2::interval;`

	tag, err := r.pool.Exec(ctx, query, campaignID, intervalStr)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// RescheduleLead unlocks a lead and returns it to QUEUED with a future next_attempt_at schedule (used for curfew holdoff or retry backoff).
// Enforces leaseToken fencing so a stale worker cannot unlock or reschedule a lead whose lease has been reassigned.
func (r *PostgresRepository) RescheduleLead(ctx context.Context, leadID string, nextAttempt time.Time, leaseToken string) error {
	if leaseToken == "" {
		return fmt.Errorf("reschedule_lead: lease token required")
	}
	query := `
		UPDATE leads
		SET status = 'QUEUED',
		    next_attempt_at = $1,
		    locked_by = NULL,
		    locked_at = NULL,
		    lease_token = NULL
		WHERE id = $2
		  AND status = 'DIALING'
		  AND lease_token = $3
		  AND locked_at IS NOT NULL
		  AND locked_at >= NOW() - INTERVAL '2 minutes';`
	tag, err := r.pool.Exec(ctx, query, nextAttempt, leadID, leaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("reschedule_lead: stale lease token or invalid lead state for %s, got %d affected rows", leadID, tag.RowsAffected())
	}
	return nil
}

// IngestLeadsIdempotent enforces database-level idempotency via ON CONFLICT (campaign_id, phone_number) DO NOTHING RETURNING id.
// Only leads actually inserted into PostgreSQL are returned in insertedIDs, serving as authoritative admission list.
func (r *PostgresRepository) IngestLeadsIdempotent(ctx context.Context, leads []*model.Lead) ([]string, int, error) {
	if len(leads) == 0 {
		return nil, 0, nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	insertQuery := `
		INSERT INTO leads (
			id, campaign_id, phone_number, name, priority,
			status, attempts, max_attempts, failure_reason,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (campaign_id, phone_number) DO NOTHING
		RETURNING id;`

	var insertedIDs []string
	duplicates := 0

	for _, l := range leads {
		createdAt := l.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}

		var insertedID string
		err := tx.QueryRow(ctx, insertQuery,
			l.ID,
			l.CampaignID,
			l.PhoneNumber,
			l.Name,
			l.Priority,
			string(l.Status),
			l.Attempts,
			l.MaxAttempts,
			l.FailureReason,
			createdAt,
		).Scan(&insertedID)
		if err != nil {
			if err == pgx.ErrNoRows {
				duplicates++
				continue
			}
			return nil, 0, fmt.Errorf("failed inserting lead %s: %w", l.ID, err)
		}
		insertedIDs = append(insertedIDs, insertedID)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, fmt.Errorf("failed to commit lead batch: %w", err)
	}

	return insertedIDs, duplicates, nil
}

func (r *PostgresRepository) RecordCallAttempt(ctx context.Context, attempt *model.CallAttempt) error {
	query := `
		INSERT INTO call_attempts (
			id, campaign_id, lead_id, phone_number, status,
			ring_duration_ms, talk_duration_ms, error_message, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`

	createdAt := attempt.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	_, err := r.pool.Exec(ctx, query,
		attempt.ID,
		attempt.CampaignID,
		attempt.LeadID,
		attempt.PhoneNumber,
		string(attempt.Status),
		attempt.RingDurationMs,
		attempt.TalkDurationMs,
		attempt.ErrorMessage,
		createdAt,
	)
	return err
}

// RecordAttemptStart durably persists attempt increment and records an IN_PROGRESS attempt record prior to external carrier dial.
// Uses strict leaseToken fencing to guarantee only the active leaseholder can initiate dialing.
func (r *PostgresRepository) RecordAttemptStart(ctx context.Context, leadID string, attemptID string, attemptNum int, leaseToken string) error {
	if leaseToken == "" {
		return fmt.Errorf("record_attempt_start: lease token required")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed beginning tx for attempt start: %w", err)
	}
	defer tx.Rollback(ctx)

	updateLeadQuery := `
		UPDATE leads
		SET attempts = $1,
		    last_attempt_at = NOW(),
		    locked_at = NOW()
		WHERE id = $2
		  AND status = 'DIALING'
		  AND lease_token = $3
		  AND locked_at IS NOT NULL
		  AND locked_at >= NOW() - INTERVAL '2 minutes';`
	tag1, err := tx.Exec(ctx, updateLeadQuery, attemptNum, leadID, leaseToken)
	if err != nil {
		return fmt.Errorf("failed updating lead attempts in start tx: %w", err)
	}
	if tag1.RowsAffected() != 1 {
		return fmt.Errorf("record_attempt_start: expected 1 row updated for lead %s in DIALING status with active lease, got %d", leadID, tag1.RowsAffected())
	}

	insertAttemptQuery := `
		INSERT INTO call_attempts (
			id, campaign_id, lead_id, phone_number, status, created_at
		) SELECT $1, campaign_id, id, phone_number, 'IN_PROGRESS', NOW()
		FROM leads WHERE id = $2 AND status = 'DIALING' AND lease_token = $3
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			created_at = EXCLUDED.created_at;`
	tag2, err := tx.Exec(ctx, insertAttemptQuery, attemptID, leadID, leaseToken)
	if err != nil {
		return fmt.Errorf("failed inserting in-progress attempt: %w", err)
	}
	if tag2.RowsAffected() != 1 {
		return fmt.Errorf("record_attempt_start: expected 1 row inserted for attempt %s, got %d", attemptID, tag2.RowsAffected())
	}

	return tx.Commit(ctx)
}

// RecordCallOutcomeTx atomically persists lead status and call attempt within a single database transaction with bounded retry and strict lease fencing
func (r *PostgresRepository) RecordCallOutcomeTx(ctx context.Context, leadID string, status model.CallStatus, reason string, nextAttempt *time.Time, attempt *model.CallAttempt, attempts int, leaseToken string) error {
	if leaseToken == "" {
		return fmt.Errorf("record_call_outcome_tx: lease token required")
	}

	var lastErr error
	backoff := 50 * time.Millisecond

	for i := 0; i < 3; i++ {
		err := func() error {
			tx, err := r.pool.Begin(ctx)
			if err != nil {
				return fmt.Errorf("failed to begin tx: %w", err)
			}
			defer tx.Rollback(ctx)

			updateQuery := `
				UPDATE leads
				SET status = $1,
				    failure_reason = $2,
				    next_attempt_at = $3,
				    last_attempt_at = NOW(),
				    attempts = $4,
				    locked_by = NULL,
				    locked_at = NULL,
				    lease_token = NULL
				WHERE id = $5
				  AND status = 'DIALING'
				  AND lease_token = $6
				  AND locked_at IS NOT NULL
				  AND locked_at >= NOW() - INTERVAL '2 minutes';`
			tag, err := tx.Exec(ctx, updateQuery, string(status), reason, nextAttempt, attempts, leadID, leaseToken)
			if err != nil {
				return fmt.Errorf("failed updating lead status in tx: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("record_call_outcome_tx: stale lease token or invalid lead state for %s, got %d affected rows", leadID, tag.RowsAffected())
			}

			if attempt != nil {
				insertAttempt := `
					INSERT INTO call_attempts (
						id, campaign_id, lead_id, phone_number, status,
						ring_duration_ms, talk_duration_ms, error_message, created_at
					) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
					ON CONFLICT (id) DO UPDATE SET
						status = EXCLUDED.status,
						ring_duration_ms = EXCLUDED.ring_duration_ms,
						talk_duration_ms = EXCLUDED.talk_duration_ms,
						error_message = EXCLUDED.error_message;`
				createdAt := attempt.CreatedAt
				if createdAt.IsZero() {
					createdAt = time.Now().UTC()
				}

				if _, err := tx.Exec(ctx, insertAttempt,
					attempt.ID,
					attempt.CampaignID,
					attempt.LeadID,
					attempt.PhoneNumber,
					string(attempt.Status),
					attempt.RingDurationMs,
					attempt.TalkDurationMs,
					attempt.ErrorMessage,
					createdAt,
				); err != nil {
					return fmt.Errorf("failed upserting call attempt in tx: %w", err)
				}
			}

			return tx.Commit(ctx)
		}()
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), "stale lease token") {
			return err
		}
		lastErr = err
		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("record outcome transaction failed after 3 attempts: %w", lastErr)
}

func (r *PostgresRepository) GetDeadLetterQueue(ctx context.Context, campaignID string) ([]*model.Lead, error) {
	query := `
		SELECT id, campaign_id, phone_number, name, priority,
		       status, attempts, max_attempts, failure_reason,
		       last_attempt_at, next_attempt_at, created_at
		FROM leads
		WHERE campaign_id = $1 AND status = 'DEAD_LETTER_QUEUE'
		ORDER BY created_at DESC;`

	rows, err := r.pool.Query(ctx, query, campaignID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dlq []*model.Lead
	for rows.Next() {
		l := &model.Lead{}
		var status string
		err := rows.Scan(
			&l.ID,
			&l.CampaignID,
			&l.PhoneNumber,
			&l.Name,
			&l.Priority,
			&status,
			&l.Attempts,
			&l.MaxAttempts,
			&l.FailureReason,
			&l.LastAttemptAt,
			&l.NextAttemptAt,
			&l.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		l.Status = model.CallStatus(status)
		dlq = append(dlq, l)
	}

	return dlq, nil
}

// AddDNCNumbers persistently saves phone numbers into the PostgreSQL dnc_numbers table,
// normalizing every number to canonical digits format before insertion.
func (r *PostgresRepository) AddDNCNumbers(ctx context.Context, numbers []string) error {
	if len(numbers) == 0 {
		return nil
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx for DNC insert: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `INSERT INTO dnc_numbers (phone_number) VALUES ($1) ON CONFLICT (phone_number) DO NOTHING;`
	for _, num := range numbers {
		norm := dnc.NormalizeNumber(num)
		if norm == "" {
			norm = num
		}
		if _, err := tx.Exec(ctx, query, norm); err != nil {
			return fmt.Errorf("failed inserting dnc number %s: %w", norm, err)
		}
	}
	return tx.Commit(ctx)
}

// GetDNCNumbers retrieves all persistently registered DNC numbers across instances
func (r *PostgresRepository) GetDNCNumbers(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT phone_number FROM dnc_numbers;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var num string
		if err := rows.Scan(&num); err != nil {
			return nil, err
		}
		result = append(result, num)
	}
	return result, nil
}

// IsDNCProhibited checks directly against the authoritative dnc_numbers PostgreSQL table,
// canonicalizing the queried number so formatting variances never cause missed DNC hits.
func (r *PostgresRepository) IsDNCProhibited(ctx context.Context, phone string) (bool, error) {
	norm := dnc.NormalizeNumber(phone)
	if norm == "" {
		norm = phone
	}
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM dnc_numbers WHERE phone_number = $1);`
	err := r.pool.QueryRow(ctx, query, norm).Scan(&exists)
	return exists, err
}

// MemoryPostgresRepository provides an in-memory replica with identical relational semantics and idempotency constraints
type MemoryPostgresRepository struct {
	mu         sync.RWMutex
	campaigns  map[string]*model.Campaign
	leads      map[string]*model.Lead
	phoneIndex map[string]string // Key: campaignID + ":" + phone
	dlq        map[string][]*model.Lead
	attempts   map[string][]*model.CallAttempt
	dncNumbers map[string]bool
}

func NewMemoryPostgresRepository() *MemoryPostgresRepository {
	return &MemoryPostgresRepository{
		campaigns:  make(map[string]*model.Campaign),
		leads:      make(map[string]*model.Lead),
		phoneIndex: make(map[string]string),
		dlq:        make(map[string][]*model.Lead),
		attempts:   make(map[string][]*model.CallAttempt),
		dncNumbers: make(map[string]bool),
	}
}

func (r *MemoryPostgresRepository) Close() {}

func (r *MemoryPostgresRepository) SaveCampaign(ctx context.Context, camp *model.Campaign) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.campaigns[camp.ID] = camp
	return nil
}

func (r *MemoryPostgresRepository) UpdateCampaignStatus(ctx context.Context, campaignID string, status model.CampaignStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.campaigns[campaignID]
	if !ok {
		return model.ErrCampaignNotFound
	}
	c.Status = status
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

func (r *MemoryPostgresRepository) ListActiveCampaigns(ctx context.Context) ([]*model.Campaign, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []*model.Campaign
	for _, c := range r.campaigns {
		if c.Status != model.CampaignCompleted {
			list = append(list, c)
		}
	}
	return list, nil
}

func (r *MemoryPostgresRepository) GetActiveLeads(ctx context.Context, campaignID string) ([]*model.Lead, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []*model.Lead
	for _, l := range r.leads {
		if l.CampaignID == campaignID && (l.Status == model.StatusQueued || l.Status == model.StatusDialing || l.Status == "") {
			list = append(list, l)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Priority != list[j].Priority {
			return list[i].Priority > list[j].Priority
		}
		return list[i].CreatedAt.Before(list[j].CreatedAt)
	})
	res := make([]*model.Lead, len(list))
	for i, l := range list {
		clone := *l
		res[i] = &clone
	}
	return res, nil
}

func (r *MemoryPostgresRepository) ClaimPendingLeads(ctx context.Context, campaignID string, limit int, workerID string) ([]*model.Lead, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if limit <= 0 {
		return nil, nil
	}

	now := time.Now().UTC()
	const leaseTTL = 2 * time.Minute
	var candidates []*model.Lead
	for _, l := range r.leads {
		if l.CampaignID == campaignID {
			isQueued := (l.Status == model.StatusQueued || l.Status == "" || l.Status == "DRAFT") &&
				(l.NextAttemptAt == nil || now.After(*l.NextAttemptAt) || now.Equal(*l.NextAttemptAt))
			isExpiredLease := l.Status == model.StatusDialing && l.LockedAt != nil && now.Sub(*l.LockedAt) > leaseTTL

			if isQueued || isExpiredLease {
				candidates = append(candidates, l)
			}
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	for _, l := range candidates {
		l.Status = model.StatusDialing
		l.LockedBy = workerID
		l.LockedAt = &now
		l.LeaseToken = uuid.NewString()
	}

	res := make([]*model.Lead, len(candidates))
	for i, l := range candidates {
		clone := *l
		res[i] = &clone
	}
	return res, nil
}

func (r *MemoryPostgresRepository) RescheduleLead(ctx context.Context, leadID string, nextAttempt time.Time, leaseToken string) error {
	if leaseToken == "" {
		return fmt.Errorf("reschedule_lead: lease token required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	lead, ok := r.leads[leadID]
	if !ok {
		return model.ErrLeadNotFound
	}
	if lead.Status != model.StatusDialing {
		return fmt.Errorf("reschedule_lead: lead %s is not in DIALING status (status=%s)", leadID, lead.Status)
	}
	if lead.LeaseToken != leaseToken {
		return fmt.Errorf("reschedule_lead: stale lease token for lead %s: current=%s, given=%s", leadID, lead.LeaseToken, leaseToken)
	}
	now := time.Now().UTC()
	if lead.LockedAt == nil || now.Sub(*lead.LockedAt) > 2*time.Minute {
		return fmt.Errorf("reschedule_lead: stale lease token or invalid lead state for %s, got 0 affected rows (lease expired)", leadID)
	}
	lead.Status = model.StatusQueued
	lead.NextAttemptAt = &nextAttempt
	lead.LockedBy = ""
	lead.LockedAt = nil
	lead.LeaseToken = ""
	return nil
}

func (r *MemoryPostgresRepository) ReclaimExpiredLeases(ctx context.Context, campaignID string, leaseTTL time.Duration) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if leaseTTL < 0 {
		leaseTTL = 2 * time.Minute
	}
	now := time.Now().UTC()
	var count int64

	for _, l := range r.leads {
		if l.CampaignID == campaignID && l.Status == model.StatusDialing && l.LockedAt != nil {
			if now.Sub(*l.LockedAt) >= leaseTTL {
				l.Status = model.StatusQueued
				l.LockedBy = ""
				l.LockedAt = nil
				l.LeaseToken = ""
				count++
			}
		}
	}
	return count, nil
}

func (r *MemoryPostgresRepository) IngestLeadsIdempotent(ctx context.Context, leads []*model.Lead) ([]string, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var insertedIDs []string
	duplicates := 0

	for _, l := range leads {
		key := fmt.Sprintf("%s:%s", l.CampaignID, l.PhoneNumber)
		if _, exists := r.phoneIndex[key]; exists {
			duplicates++
			continue
		}

		r.phoneIndex[key] = l.ID
		r.leads[l.ID] = l
		insertedIDs = append(insertedIDs, l.ID)
	}

	return insertedIDs, duplicates, nil
}

func (r *MemoryPostgresRepository) RecordCallAttempt(ctx context.Context, attempt *model.CallAttempt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts[attempt.CampaignID] = append(r.attempts[attempt.CampaignID], attempt)
	return nil
}

func (r *MemoryPostgresRepository) RecordAttemptStart(ctx context.Context, leadID string, attemptID string, attemptNum int, leaseToken string) error {
	if leaseToken == "" {
		return fmt.Errorf("record_attempt_start: lease token required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	lead, exists := r.leads[leadID]
	if !exists {
		return fmt.Errorf("record_attempt_start: expected 1 row updated for lead %s in DIALING status with active lease, got 0", leadID)
	}
	if lead.Status != model.StatusDialing {
		return fmt.Errorf("record_attempt_start: lead %s is not in DIALING status (status=%s)", leadID, lead.Status)
	}
	if lead.LeaseToken != leaseToken {
		return fmt.Errorf("record_attempt_start: lease token mismatch for lead %s: current=%s, given=%s", leadID, lead.LeaseToken, leaseToken)
	}
	now := time.Now().UTC()
	if lead.LockedAt == nil || now.Sub(*lead.LockedAt) > 2*time.Minute {
		return fmt.Errorf("record_attempt_start: expected 1 row updated for lead %s in DIALING status with active lease, got 0 (lease expired)", leadID)
	}
	lead.Attempts = attemptNum
	lead.LastAttemptAt = &now
	lead.LockedAt = &now // Atomic lease refresh on attempt start

	r.attempts[lead.CampaignID] = append(r.attempts[lead.CampaignID], &model.CallAttempt{
		ID:          attemptID,
		CampaignID:  lead.CampaignID,
		LeadID:      lead.ID,
		PhoneNumber: lead.PhoneNumber,
		Status:      "IN_PROGRESS",
		CreatedAt:   now,
	})
	return nil
}

func (r *MemoryPostgresRepository) RecordCallOutcomeTx(ctx context.Context, leadID string, status model.CallStatus, reason string, nextAttempt *time.Time, attempt *model.CallAttempt, attempts int, leaseToken string) error {
	if leaseToken == "" {
		return fmt.Errorf("record_call_outcome_tx: lease token required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	lead, ok := r.leads[leadID]
	if !ok {
		return model.ErrLeadNotFound
	}
	if lead.Status != model.StatusDialing {
		return fmt.Errorf("record_call_outcome_tx: lead %s is not in DIALING status (status=%s)", leadID, lead.Status)
	}
	if lead.LeaseToken != leaseToken {
		return fmt.Errorf("record_call_outcome_tx: stale lease token for lead %s: current=%s, given=%s", leadID, lead.LeaseToken, leaseToken)
	}
	now := time.Now().UTC()
	if lead.LockedAt == nil || now.Sub(*lead.LockedAt) > 2*time.Minute {
		return fmt.Errorf("record_call_outcome_tx: stale lease token or invalid lead state for %s, got 0 affected rows (lease expired)", leadID)
	}

	lead.Status = status
	lead.FailureReason = reason
	lead.NextAttemptAt = nextAttempt
	lead.Attempts = attempts
	lead.LockedBy = ""
	lead.LockedAt = nil
	lead.LeaseToken = ""
	lead.LastAttemptAt = &now

	if status == model.StatusDLQ || status == model.StatusFailed {
		r.dlq[lead.CampaignID] = append(r.dlq[lead.CampaignID], lead)
	}

	if attempt != nil {
		updated := false
		for i, a := range r.attempts[attempt.CampaignID] {
			if a.ID == attempt.ID {
				r.attempts[attempt.CampaignID][i] = attempt
				updated = true
				break
			}
		}
		if !updated {
			r.attempts[attempt.CampaignID] = append(r.attempts[attempt.CampaignID], attempt)
		}
	}

	return nil
}

func (r *MemoryPostgresRepository) AddDNCNumbers(ctx context.Context, numbers []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range numbers {
		norm := dnc.NormalizeNumber(n)
		if norm == "" {
			norm = n
		}
		r.dncNumbers[norm] = true
	}
	return nil
}

func (r *MemoryPostgresRepository) GetDNCNumbers(ctx context.Context) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var res []string
	for n := range r.dncNumbers {
		res = append(res, n)
	}
	return res, nil
}

func (r *MemoryPostgresRepository) IsDNCProhibited(ctx context.Context, phone string) (bool, error) {
	norm := dnc.NormalizeNumber(phone)
	if norm == "" {
		norm = phone
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.dncNumbers[norm], nil
}

func (r *MemoryPostgresRepository) GetDeadLetterQueue(ctx context.Context, campaignID string) ([]*model.Lead, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dlqLeads := r.dlq[campaignID]
	res := make([]*model.Lead, len(dlqLeads))
	copy(res, dlqLeads)
	return res, nil
}
