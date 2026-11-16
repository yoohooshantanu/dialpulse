package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/yoohooshantanu/dialpulse/internal/logger"
	"github.com/yoohooshantanu/dialpulse/internal/model"
)

// ConsumerTxHandler abstracts business side-effects that execute inside the same
// transaction as the consumer's processed_events idempotency record.
type ConsumerTxHandler interface {
	HandleEventTx(ctx context.Context, tx pgx.Tx, event *model.OutboxEvent) error
}

// CampaignOutcomeAggregator demonstrates atomic aggregate side-effects by incrementing
// per-campaign outcome counters within the consumer's idempotency transaction.
type CampaignOutcomeAggregator struct{}

func NewCampaignOutcomeAggregator() *CampaignOutcomeAggregator {
	return &CampaignOutcomeAggregator{}
}

func (h *CampaignOutcomeAggregator) HandleEventTx(ctx context.Context, tx pgx.Tx, event *model.OutboxEvent) error {
	var payload struct {
		CampaignID string `json:"campaign_id"`
		Status     string `json:"status"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("invalid outcome payload: %w", err)
	}
	if payload.CampaignID == "" || payload.Status == "" {
		return fmt.Errorf("outcome payload missing campaign_id or status")
	}

	query := `
		INSERT INTO campaign_outcomes (campaign_id, status, count, last_updated_at)
		VALUES ($1, $2, 1, clock_timestamp())
		ON CONFLICT (campaign_id, status) DO UPDATE SET
			count = campaign_outcomes.count + 1,
			last_updated_at = clock_timestamp();`
	_, err := tx.Exec(ctx, query, payload.CampaignID, payload.Status)
	return err
}

// OutboxConsumerConfig configures durable pull consumer retry, backoff, and DLQ routing.
type OutboxConsumerConfig struct {
	ConsumerName     string        // Durable consumer name, e.g. "dialpulse_outcome_worker"
	StreamName       string        // JetStream stream, e.g. "DIALPULSE_EVENTS"
	SubjectFilter    string        // JetStream subject filter, e.g. "dialpulse.events.>"
	DLQSubjectPrefix string        // DLQ subject prefix, e.g. "dialpulse.dlq"
	MaxDeliveries    int           // Maximum delivery attempts before poison message is routed to DLQ (default: 3)
	AckWait          time.Duration // JetStream AckWait (default: 10s, must be > HandlerTimeout)
	HandlerTimeout   time.Duration // Execution timeout for handler (default: 2s)
	BaseBackoff      time.Duration // Initial retry delay for NakWithDelay (default: 20ms)
	MaxBackoff       time.Duration // Ceiling for exponential backoff (default: 1s)
}

func DefaultOutboxConsumerConfig() OutboxConsumerConfig {
	return OutboxConsumerConfig{
		ConsumerName:     "dialpulse_outcome_worker",
		StreamName:       "DIALPULSE_EVENTS",
		SubjectFilter:    "dialpulse.events.>",
		DLQSubjectPrefix: "dialpulse.dlq",
		MaxDeliveries:    3,
		AckWait:          10 * time.Second,
		HandlerTimeout:   2 * time.Second,
		BaseBackoff:      20 * time.Millisecond,
		MaxBackoff:       1 * time.Second,
	}
}

// OutboxConsumer consumes messages from NATS JetStream, enforcing at-least-once delivery
// with atomic database-level idempotency and poison-message dead letter routing.
//
// TRANSACTION & IDEMPOTENCY GUARANTEES:
// For each message, both the processed_events record and the handler's business DB mutation
// are committed in the SAME PostgreSQL transaction.
// If the event was previously recorded in processed_events (e.g. duplicate redelivery after crash),
// the transaction commits without re-executing handler logic, preserving an idempotent effect.
//
// DLQ CRASH LIMITATION:
// If a process crashes after publishing a poison message to the DLQ subject but before acknowledging
// (Ack) the original message in JetStream, the message will be redelivered upon restart and may produce
// a duplicate DLQ entry. Downstream DLQ consumers must treat DLQ events as at-least-once.
type OutboxConsumer struct {
	pool    *pgxpool.Pool
	js      nats.JetStreamContext
	handler ConsumerTxHandler
	cfg     OutboxConsumerConfig

	sub    *nats.Subscription
	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

func NewOutboxConsumer(pool *pgxpool.Pool, js nats.JetStreamContext, handler ConsumerTxHandler, cfg OutboxConsumerConfig) *OutboxConsumer {
	if cfg.MaxDeliveries <= 0 {
		cfg.MaxDeliveries = 3
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 10 * time.Second
	}
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = 2 * time.Second
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 20 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 1 * time.Second
	}
	if cfg.DLQSubjectPrefix == "" {
		cfg.DLQSubjectPrefix = "dialpulse.dlq"
	}

	return &OutboxConsumer{
		pool:    pool,
		js:      js,
		handler: handler,
		cfg:     cfg,
		stopCh:  make(chan struct{}),
	}
}

// ProcessMessage processes a single NATS message with atomic idempotency and DLQ routing.
func (c *OutboxConsumer) ProcessMessage(ctx context.Context, msg *nats.Msg) error {
	meta, err := msg.Metadata()
	if err != nil {
		meta = &nats.MsgMetadata{NumDelivered: 1}
	}

	eventID := msg.Header.Get("Nats-Msg-Id")
	if eventID == "" {
		eventID = string(msg.Data)
	}

	var event model.OutboxEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		event = model.OutboxEvent{
			ID:            eventID,
			AggregateType: msg.Header.Get("x-aggregate-type"),
			AggregateID:   msg.Header.Get("x-aggregate-id"),
			EventType:     msg.Header.Get("x-event-type"),
			Payload:       msg.Data,
		}
	}
	if event.ID == "" {
		event.ID = eventID
	}
	if event.AggregateType == "" {
		event.AggregateType = "lead"
	}

	// Step 1: Open a single database transaction for idempotency record + handler mutation
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		_ = msg.NakWithDelay(c.cfg.BaseBackoff)
		return fmt.Errorf("outbox_consumer: failed beginning tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Step 2: Attempt inserting into processed_events to establish idempotency boundary
	insertQuery := `
		INSERT INTO processed_events (event_id, consumer_name, processed_at)
		VALUES ($1, $2, clock_timestamp())
		ON CONFLICT (event_id, consumer_name) DO NOTHING;`
	tag, err := tx.Exec(ctx, insertQuery, event.ID, c.cfg.ConsumerName)
	if err != nil {
		_ = msg.NakWithDelay(c.cfg.BaseBackoff)
		return fmt.Errorf("outbox_consumer: insert processed_events failed: %w", err)
	}

	// Idempotency Gate: If row was not inserted, this event was already processed
	if tag.RowsAffected() == 0 {
		_ = msg.Ack()
		return nil
	}

	// Step 3: Execute handler inside the SAME transaction, passing event.ID for idempotency
	handlerCtx, cancel := context.WithTimeout(ctx, c.cfg.HandlerTimeout)
	handlerErr := c.handler.HandleEventTx(handlerCtx, tx, &event)
	cancel()

	if handlerErr != nil {
		// Rollback both handler modifications and processed_events insertion
		_ = tx.Rollback(ctx)

		// Check delivery count using JetStream metadata NumDelivered
		if int(meta.NumDelivered) >= c.cfg.MaxDeliveries {
			// Poison message exceeded max deliveries: route to DLQ
			dlqSubject := fmt.Sprintf("%s.%s", c.cfg.DLQSubjectPrefix, event.AggregateType)
			dlqMsg := nats.NewMsg(dlqSubject)
			dlqMsg.Header.Set("Nats-Msg-Id", fmt.Sprintf("dlq_%s", event.ID))
			dlqMsg.Header.Set("x-original-event-id", event.ID)
			dlqMsg.Header.Set("x-dlq-error", handlerErr.Error())
			dlqMsg.Header.Set("x-delivery-count", fmt.Sprintf("%d", meta.NumDelivered))
			dlqMsg.Data = msg.Data

			var pubErr error
			if c.js != nil {
				_, pubErr = c.js.PublishMsg(dlqMsg)
				if pubErr != nil {
					// Fall back to core NATS connection if no JetStream stream is mapped to DLQ subject
					pubErr = c.js.Conn().PublishMsg(dlqMsg)
				}
			}

			if pubErr != nil {
				_ = msg.NakWithDelay(c.cfg.BaseBackoff)
				return fmt.Errorf("outbox_consumer: failed routing poison message to DLQ: %w", pubErr)
			}

			// Positively acknowledge original message from primary stream after routing to DLQ
			_ = msg.Ack()
			return nil
		}

		// Under max deliveries: calculate exponential backoff delay and NAK
		backoff := c.cfg.BaseBackoff * (1 << (meta.NumDelivered - 1))
		if backoff > c.cfg.MaxBackoff {
			backoff = c.cfg.MaxBackoff
		}
		_ = msg.NakWithDelay(backoff)
		return handlerErr
	}

	// Step 4: Atomically commit handler business mutation and processed_events record
	if err := tx.Commit(ctx); err != nil {
		_ = msg.NakWithDelay(c.cfg.BaseBackoff)
		return fmt.Errorf("outbox_consumer: failed committing tx: %w", err)
	}

	// Step 5: Acknowledge message delivery to JetStream
	_ = msg.Ack()
	return nil
}

// StartPullConsumer initializes a durable pull subscription and continuously processes batches.
func (c *OutboxConsumer) StartPullConsumer(ctx context.Context) error {
	sub, err := c.js.PullSubscribe(
		c.cfg.SubjectFilter,
		c.cfg.ConsumerName,
		nats.AckWait(c.cfg.AckWait),
		nats.MaxDeliver(c.cfg.MaxDeliveries),
	)
	if err != nil {
		return fmt.Errorf("outbox_consumer: failed creating pull subscription: %w", err)
	}
	c.sub = sub

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.stopCh:
				return
			case <-ctx.Done():
				return
			default:
				// Pull messages in bounded batches with short timeout
				msgs, err := sub.Fetch(5, nats.MaxWait(200*time.Millisecond))
				if err != nil {
					if err == nats.ErrTimeout {
						continue
					}
					select {
					case <-c.stopCh:
						return
					case <-time.After(50 * time.Millisecond):
						continue
					}
				}

				for _, msg := range msgs {
					if err := c.ProcessMessage(ctx, msg); err != nil {
						logger.L.Warn("outbox_consumer_msg_error", "error", err.Error())
					}
				}
			}
		}
	}()

	return nil
}

// Stop terminates the pull consumer cleanly.
func (c *OutboxConsumer) Stop() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.stopCh)
	c.mu.Unlock()

	c.wg.Wait()
}
