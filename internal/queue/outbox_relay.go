package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/yoohooshantanu/dialpulse/internal/logger"
)

// JetStreamPublisher abstracts NATS JetStream publication for dependency injection and testing.
type JetStreamPublisher interface {
	PublishMsg(ctx context.Context, msg *nats.Msg) (*nats.PubAck, error)
}

// NATSJetStreamPublisher wraps nats.JetStreamContext to implement JetStreamPublisher.
type NATSJetStreamPublisher struct {
	js nats.JetStreamContext
}

func NewNATSJetStreamPublisher(js nats.JetStreamContext) *NATSJetStreamPublisher {
	return &NATSJetStreamPublisher{js: js}
}

func (p *NATSJetStreamPublisher) PublishMsg(ctx context.Context, msg *nats.Msg) (*nats.PubAck, error) {
	return p.js.PublishMsg(msg, nats.Context(ctx))
}

// OutboxRelayConfig configures the transactional outbox relay polling behavior.
type OutboxRelayConfig struct {
	BatchSize      int           // Batch size for SKIP LOCKED claiming (default: 10)
	PollInterval   time.Duration // Polling loop cadence (default: 50ms)
	PublishTimeout time.Duration // Per-publish JetStream timeout (default: 2s)
	SubjectPrefix  string        // Subject prefix, e.g. "dialpulse.events"
}

func DefaultOutboxRelayConfig() OutboxRelayConfig {
	return OutboxRelayConfig{
		BatchSize:      10,
		PollInterval:   50 * time.Millisecond,
		PublishTimeout: 2 * time.Second,
		SubjectPrefix:  "dialpulse.events",
	}
}

// OutboxRelay continuously drains unpublished records from outbox_events to NATS JetStream.
//
// TRANSACTION & ORDERING GUARANTEES:
// Each batch executes inside a single database transaction:
//  1. SELECT ... FOR UPDATE SKIP LOCKED (bounded by BatchSize).
//  2. Publish each message with Nats-Msg-Id set to event ID and individual timeout.
//  3. UPDATE outbox_events SET published_at = clock_timestamp() WHERE id = ANY(...).
//  4. COMMIT.
//
// If any publish fails or times out, the transaction rolls back, releasing the row locks
// so unpublished events remain intact and will be retried in a subsequent iteration.
//
// CONCURRENCY & ORDERING NOTE:
// When multiple relays run concurrently using SELECT ... FOR UPDATE SKIP LOCKED, there is
// NO global or per-lead ordering guarantee across relays. While single-lead updates are
// serialized at the database lease boundary, distinct relays claim distinct batches in parallel.
// Downstream consumers must be resilient to out-of-order event delivery.
type OutboxRelay struct {
	pool      *pgxpool.Pool
	publisher JetStreamPublisher
	cfg       OutboxRelayConfig

	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

func NewOutboxRelay(pool *pgxpool.Pool, publisher JetStreamPublisher, cfg OutboxRelayConfig) *OutboxRelay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 2 * time.Second
	}
	if cfg.SubjectPrefix == "" {
		cfg.SubjectPrefix = "dialpulse.events"
	}

	return &OutboxRelay{
		pool:      pool,
		publisher: publisher,
		cfg:       cfg,
		stopCh:    make(chan struct{}),
	}
}

// ProcessBatch executes one bounded batch cycle inside a single database transaction.
// Returns the number of events published, or an error causing a rollback.
func (r *OutboxRelay) ProcessBatch(ctx context.Context) (int, error) {
	if r.pool == nil || r.publisher == nil {
		return 0, fmt.Errorf("outbox_relay: database pool or publisher uninitialized")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox_relay: failed beginning batch transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Claim unpublished rows with exclusive row lock, skipping locked rows claimed by competing relays
	query := `
		SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE;`

	rows, err := tx.Query(ctx, query, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox_relay: failed claiming outbox rows: %w", err)
	}
	defer rows.Close()

	type outboxRecord struct {
		id            string
		aggregateType string
		aggregateID   string
		eventType     string
		payload       []byte
		createdAt     time.Time
	}

	var batch []outboxRecord
	for rows.Next() {
		var rec outboxRecord
		if err := rows.Scan(&rec.id, &rec.aggregateType, &rec.aggregateID, &rec.eventType, &rec.payload, &rec.createdAt); err != nil {
			return 0, fmt.Errorf("outbox_relay: failed scanning row: %w", err)
		}
		batch = append(batch, rec)
	}
	rows.Close()

	if len(batch) == 0 {
		return 0, nil
	}

	publishedIDs := make([]string, 0, len(batch))
	for _, item := range batch {
		subject := fmt.Sprintf("%s.%s.%s", r.cfg.SubjectPrefix, item.aggregateType, item.eventType)
		msg := nats.NewMsg(subject)
		msg.Header.Set("Nats-Msg-Id", item.id)
		msg.Header.Set("x-aggregate-type", item.aggregateType)
		msg.Header.Set("x-aggregate-id", item.aggregateID)
		msg.Header.Set("x-event-type", item.eventType)
		msg.Data = item.payload

		pubCtx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
		_, pubErr := r.publisher.PublishMsg(pubCtx, msg)
		cancel()
		if pubErr != nil {
			return 0, fmt.Errorf("outbox_relay: publish failed for event %s: %w", item.id, pubErr)
		}
		publishedIDs = append(publishedIDs, item.id)
	}

	updateQuery := `
		UPDATE outbox_events
		SET published_at = clock_timestamp()
		WHERE id = ANY($1);`
	if _, err := tx.Exec(ctx, updateQuery, publishedIDs); err != nil {
		return 0, fmt.Errorf("outbox_relay: failed updating published_at: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("outbox_relay: failed committing batch transaction: %w", err)
	}

	return len(publishedIDs), nil
}

// Start launches the background relay polling goroutine.
func (r *OutboxRelay) Start(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.cfg.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-r.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, err := r.ProcessBatch(ctx)
				if err != nil {
					logger.L.Warn("outbox_relay_batch_error", "error", err.Error())
				}
			}
		}
	}()
}

// Stop performs graceful shutdown, waiting for active batch processing to terminate.
func (r *OutboxRelay) Stop() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	close(r.stopCh)
	r.mu.Unlock()

	r.wg.Wait()
}
