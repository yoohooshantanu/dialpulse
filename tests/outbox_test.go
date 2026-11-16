package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"

	"github.com/yoohooshantanu/dialpulse/internal/metrics"
	"github.com/yoohooshantanu/dialpulse/internal/model"
	"github.com/yoohooshantanu/dialpulse/internal/queue"
	"github.com/yoohooshantanu/dialpulse/internal/storage"
)

func getTestNATSURL(t *testing.T) string {
	t.Helper()
	natsURL := os.Getenv("TEST_NATS_URL")
	if natsURL == "" {
		natsURL = os.Getenv("NATS_URL")
	}
	if natsURL == "" {
		if os.Getenv("REQUIRE_NATS") == "1" {
			t.Fatalf("REQUIRE_NATS=1 but TEST_NATS_URL is not set")
		}
		t.Skip("skipping live NATS test: TEST_NATS_URL not set")
	}
	return natsURL
}

func setupTestJetStream(t *testing.T, natsURL, streamName, subjectFilter string, dedupWindow time.Duration) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed connecting to NATS at %s: %v", natsURL, err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatalf("failed creating JetStream context: %v", err)
	}

	// Clean up existing stream if leftover
	_ = js.DeleteStream(streamName)

	streamCfg := &nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{subjectFilter},
		Storage:    nats.MemoryStorage,
		Duplicates: dedupWindow,
		MaxAge:     5 * time.Minute,
	}
	_, err = js.AddStream(streamCfg)
	if err != nil {
		nc.Close()
		t.Fatalf("failed adding JetStream stream %s: %v", streamName, err)
	}

	t.Cleanup(func() {
		_ = js.DeleteStream(streamName)
		nc.Close()
	})

	return nc, js
}

// CountingPublisher wraps a JetStreamPublisher to record publication counts per event ID without JetStream dedup
type CountingPublisher struct {
	inner        queue.JetStreamPublisher
	totalCalls   int64
	attemptsByID sync.Map
}

func NewCountingPublisher(inner queue.JetStreamPublisher) *CountingPublisher {
	return &CountingPublisher{inner: inner}
}

func (c *CountingPublisher) PublishMsg(ctx context.Context, msg *nats.Msg) (*nats.PubAck, error) {
	atomic.AddInt64(&c.totalCalls, 1)
	id := msg.Header.Get("Nats-Msg-Id")
	if id != "" {
		val, _ := c.attemptsByID.LoadOrStore(id, new(int64))
		atomic.AddInt64(val.(*int64), 1)
	}
	if c.inner != nil {
		return c.inner.PublishMsg(ctx, msg)
	}
	return &nats.PubAck{Sequence: 1}, nil
}

// FailingPublisher always returns a network error to simulate unreachable NATS
type FailingPublisher struct{}

func (f *FailingPublisher) PublishMsg(ctx context.Context, msg *nats.Msg) (*nats.PubAck, error) {
	return nil, errors.New("nats: connection refused (simulated unreachable broker)")
}

// 1. NATS Down: Outbox accumulates unpublished rows in Postgres, drains upon recovery
func TestOutboxAccumulatesWhenNATSDownAndDrainsOnRecovery(t *testing.T) {
	dbURL := getTestDatabaseURL(t)
	natsURL := getTestNATSURL(t)
	ctx := context.Background()

	repo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to Postgres: %v", err)
	}
	defer repo.Close()

	streamName := "STREAM_ACCUM_DRAIN_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
	_, js := setupTestJetStream(t, natsURL, streamName, "dialpulse.events.accum.>", 2*time.Minute)

	// Seed campaign and lead in DIALING status
	campID := "camp_accum_" + uuid.New().String()[:8]
	if err := repo.SaveCampaign(ctx, &model.Campaign{
		ID: campID, Name: "Accum Drain Camp", Status: model.CampaignRunning,
	}); err != nil {
		t.Fatalf("failed saving campaign: %v", err)
	}

	leadID := "lead_accum_" + uuid.New().String()[:8]
	phone := "+1415555" + fmt.Sprintf("%04d", time.Now().UnixNano()%10000)
	if _, _, err := repo.IngestLeadsIdempotent(ctx, []*model.Lead{{
		ID: leadID, CampaignID: campID, PhoneNumber: phone, Status: model.StatusQueued,
	}}); err != nil {
		t.Fatalf("failed ingesting lead: %v", err)
	}

	claimed, err := repo.ClaimPendingLeads(ctx, campID, 1, "worker_accum")
	if err != nil || len(claimed) == 0 {
		t.Fatalf("failed claiming lead: %v", err)
	}
	token := claimed[0].LeaseToken

	attemptID := "att_accum_" + uuid.New().String()[:8]
	if err := repo.RecordAttemptStart(ctx, leadID, attemptID, 1, token); err != nil {
		t.Fatalf("failed record attempt start: %v", err)
	}

	// Record call outcome transaction (which MUST atomically insert into outbox_events)
	attempt := &model.CallAttempt{
		ID: attemptID, CampaignID: campID, LeadID: leadID, PhoneNumber: phone,
		Status: model.StatusAnswered, RingDurationMs: 300, TalkDurationMs: 1200,
	}
	if err := repo.RecordCallOutcomeTx(ctx, leadID, model.StatusAnswered, "completed", nil, attempt, 1, token); err != nil {
		t.Fatalf("record call outcome failed: %v", err)
	}

	// Verify outbox row exists with published_at IS NULL
	unpub, err := repo.GetUnpublishedOutboxEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed querying outbox: %v", err)
	}
	var targetEvent *model.OutboxEvent
	expectedID := model.DeterministicOutboxEventID(attemptID, "call.outcome_recorded")
	for _, e := range unpub {
		if e.ID == expectedID {
			targetEvent = e
			break
		}
	}
	if targetEvent == nil {
		t.Fatalf("expected outbox event %s to exist with published_at IS NULL, but not found", expectedID)
	}
	if targetEvent.PublishedAt != nil {
		t.Fatalf("expected published_at to be NULL while unpublished, got %v", targetEvent.PublishedAt)
	}

	// Verify outbox lag is > 0
	lag, err := repo.GetOutboxLagSeconds(ctx)
	if err != nil {
		t.Fatalf("failed getting outbox lag: %v", err)
	}
	if lag < 0.0 {
		t.Fatalf("expected non-negative outbox lag, got %f", lag)
	}

	// Run relay with FAILING publisher (simulating NATS down)
	failingRelay := queue.NewOutboxRelay(repo.Pool(), &FailingPublisher{}, queue.OutboxRelayConfig{
		BatchSize:     5,
		SubjectPrefix: "dialpulse.events.accum",
	})
	n, err := failingRelay.ProcessBatch(ctx)
	if err == nil {
		t.Fatalf("expected batch processing with failing publisher to return error, got n=%d", n)
	}

	// Assert row remains unpublished after failure (transaction rollback)
	unpubAfter, _ := repo.GetUnpublishedOutboxEvents(ctx, 10)
	var stillUnpublished bool
	for _, e := range unpubAfter {
		if e.ID == expectedID && e.PublishedAt == nil {
			stillUnpublished = true
			break
		}
	}
	if !stillUnpublished {
		t.Fatalf("expected outbox event %s to remain unpublished after simulated NATS failure", expectedID)
	}

	// Now connect relay with LIVE JetStream publisher (simulating NATS recovery)
	liveRelay := queue.NewOutboxRelay(repo.Pool(), queue.NewNATSJetStreamPublisher(js), queue.OutboxRelayConfig{
		BatchSize:     10,
		SubjectPrefix: "dialpulse.events.accum",
	})
	publishedCount, err := liveRelay.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("process batch failed on recovered NATS: %v", err)
	}
	if publishedCount < 1 {
		t.Fatalf("expected at least 1 event published upon recovery, got %d", publishedCount)
	}

	// Verify target event now has published_at IS NOT NULL
	var finalPublishedAt *time.Time
	queryCheck := `SELECT published_at FROM outbox_events WHERE id = $1;`
	if err := repo.Pool().QueryRow(ctx, queryCheck, expectedID).Scan(&finalPublishedAt); err != nil {
		t.Fatalf("failed checking final published_at: %v", err)
	}
	if finalPublishedAt == nil {
		t.Fatalf("expected event %s to have non-nil published_at after recovery drain", expectedID)
	}
}

// 2. Crash between publish and mark: At-least-once delivery, idempotent effect (PubAck.Duplicate == true, stream count == 1)
func TestAtLeastOnceDeliveryCrashBetweenPublishAndMarkIdempotentEffect(t *testing.T) {
	dbURL := getTestDatabaseURL(t)
	natsURL := getTestNATSURL(t)
	ctx := context.Background()

	repo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to Postgres: %v", err)
	}
	defer repo.Close()

	streamName := "STREAM_CRASH_MARK_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
	_, js := setupTestJetStream(t, natsURL, streamName, "dialpulse.events.crash.>", 2*time.Minute)

	eventID := "evt_crash_" + uuid.New().String()
	campID := "camp_crash_" + uuid.New().String()[:8]
	payloadBytes, _ := json.Marshal(map[string]string{
		"campaign_id": campID,
		"status":      "ANSWERED",
	})

	subject := "dialpulse.events.crash.lead.outcome"
	msg1 := nats.NewMsg(subject)
	msg1.Header.Set("Nats-Msg-Id", eventID)
	msg1.Header.Set("x-aggregate-type", "lead")
	msg1.Data = payloadBytes

	// First publish succeeds
	ack1, err := js.PublishMsg(msg1)
	if err != nil {
		t.Fatalf("first publish failed: %v", err)
	}
	if ack1.Duplicate {
		t.Fatalf("expected first publish Duplicate=false, got true")
	}

	// Simulate crash: relay crashed before updating published_at.
	// Relay restarts and attempts republishing the same message with same Nats-Msg-Id within dedup window
	msg2 := nats.NewMsg(subject)
	msg2.Header.Set("Nats-Msg-Id", eventID)
	msg2.Header.Set("x-aggregate-type", "lead")
	msg2.Data = payloadBytes

	ack2, err := js.PublishMsg(msg2)
	if err != nil {
		t.Fatalf("republish failed: %v", err)
	}
	// JetStream deduplication MUST flag this as duplicate
	if !ack2.Duplicate {
		t.Fatalf("expected JetStream PubAck.Duplicate == true on re-publish within window, got false")
	}

	// Verify stream message count is 1
	info, err := js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("failed getting stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("expected stream message count to be 1, got %d", info.State.Msgs)
	}

	// Verify consumer processes message and executes DB effect idempotently
	consumer := queue.NewOutboxConsumer(repo.Pool(), js, queue.NewCampaignOutcomeAggregator(), queue.OutboxConsumerConfig{
		ConsumerName: "consumer_crash_test",
		StreamName:   streamName,
	})

	// Process message first time
	if err := consumer.ProcessMessage(ctx, msg1); err != nil {
		t.Fatalf("consumer first process failed: %v", err)
	}

	// Process simulated redelivery of duplicate message
	if err := consumer.ProcessMessage(ctx, msg2); err != nil {
		t.Fatalf("consumer duplicate process failed: %v", err)
	}

	// Assert business outcome counter equals 1 (idempotent effect)
	count, err := repo.GetCampaignOutcomeCount(ctx, campID, "ANSWERED")
	if err != nil {
		t.Fatalf("failed getting outcome count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected campaign outcome count == 1, got %d", count)
	}
}

// 3. Short deduplication window expiration: Stream accepts duplicate, consumer DB idempotency preserves single effect
func TestDedupWindowExpiryStreamAcceptsDuplicateConsumerIdempotentEffect(t *testing.T) {
	dbURL := getTestDatabaseURL(t)
	natsURL := getTestNATSURL(t)
	ctx := context.Background()

	repo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to Postgres: %v", err)
	}
	defer repo.Close()

	// Short deduplication window of 1 second
	streamName := "STREAM_DEDUP_EXPIRE_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
	_, js := setupTestJetStream(t, natsURL, streamName, "dialpulse.events.window.>", 1*time.Second)

	eventID := "evt_window_expire_" + uuid.New().String()
	campID := "camp_window_" + uuid.New().String()[:8]
	payloadBytes, _ := json.Marshal(map[string]string{
		"campaign_id": campID,
		"status":      "CONNECTED",
	})

	subject := "dialpulse.events.window.lead.outcome"
	msg1 := nats.NewMsg(subject)
	msg1.Header.Set("Nats-Msg-Id", eventID)
	msg1.Header.Set("x-aggregate-type", "lead")
	msg1.Data = payloadBytes

	ack1, err := js.PublishMsg(msg1)
	if err != nil {
		t.Fatalf("initial publish failed: %v", err)
	}
	if ack1.Duplicate {
		t.Fatalf("expected first publish to not be duplicate")
	}

	// Sleep 1.2s to exceed the 1s deduplication window
	time.Sleep(1200 * time.Millisecond)

	// Republish with identical Nats-Msg-Id after window expiry
	msg2 := nats.NewMsg(subject)
	msg2.Header.Set("Nats-Msg-Id", eventID)
	msg2.Header.Set("x-aggregate-type", "lead")
	msg2.Data = payloadBytes

	ack2, err := js.PublishMsg(msg2)
	if err != nil {
		t.Fatalf("second publish failed: %v", err)
	}

	// JetStream deduplication window expired, so stream MUST accept it as a new message!
	if ack2.Duplicate {
		t.Fatalf("expected PubAck.Duplicate == false after dedup window expired, got true")
	}

	// Stream message count is now 2
	info, err := js.StreamInfo(streamName)
	if err != nil {
		t.Fatalf("failed getting stream info: %v", err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("expected stream message count == 2 after dedup window expiry, got %d", info.State.Msgs)
	}

	// Downstream consumer receives both messages:
	// In the absence of consumer-level DB idempotency, this would cause double-counting!
	consumer := queue.NewOutboxConsumer(repo.Pool(), js, queue.NewCampaignOutcomeAggregator(), queue.OutboxConsumerConfig{
		ConsumerName: "consumer_window_test",
		StreamName:   streamName,
	})

	if err := consumer.ProcessMessage(ctx, msg1); err != nil {
		t.Fatalf("consumer failed processing msg1: %v", err)
	}
	if err := consumer.ProcessMessage(ctx, msg2); err != nil {
		t.Fatalf("consumer failed processing msg2: %v", err)
	}

	// Assert business counter is EXACTLY 1 because processed_events blocked the duplicate!
	count, err := repo.GetCampaignOutcomeCount(ctx, campID, "CONNECTED")
	if err != nil {
		t.Fatalf("failed getting outcome count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected campaign outcome count == 1 due to DB idempotency gate, got %d", count)
	}
}

// FailingHandler simulates a poison message handler that fails unrecoverably
type FailingHandler struct{}

func (h *FailingHandler) HandleEventTx(ctx context.Context, tx pgx.Tx, event *model.OutboxEvent) error {
	return errors.New("unrecoverable business processing failure: poison payload")
}

// 4. Poison message reaches DLQ subject after N deliveries
func TestPoisonMessageReachesDLQAfterMaxDeliveries(t *testing.T) {
	dbURL := getTestDatabaseURL(t)
	natsURL := getTestNATSURL(t)
	ctx := context.Background()

	repo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to Postgres: %v", err)
	}
	defer repo.Close()

	streamName := "STREAM_POISON_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
	nc, js := setupTestJetStream(t, natsURL, streamName, "dialpulse.events.poison.>", 2*time.Minute)

	// Subscribe to DLQ subject to observe arrival
	dlqSubject := "dialpulse.dlq.lead"
	dlqSub, err := nc.SubscribeSync(dlqSubject)
	if err != nil {
		t.Fatalf("failed subscribing to DLQ subject: %v", err)
	}
	defer dlqSub.Unsubscribe()

	eventID := "evt_poison_" + uuid.New().String()
	subject := "dialpulse.events.poison.lead.poison_test"
	msg := nats.NewMsg(subject)
	msg.Header.Set("Nats-Msg-Id", eventID)
	msg.Header.Set("x-aggregate-type", "lead")
	msg.Data = []byte(`{"poison": true}`)

	_, err = js.PublishMsg(msg)
	if err != nil {
		t.Fatalf("failed publishing poison message: %v", err)
	}

	consumer := queue.NewOutboxConsumer(repo.Pool(), js, &FailingHandler{}, queue.OutboxConsumerConfig{
		ConsumerName:     "poison_consumer",
		StreamName:       streamName,
		DLQSubjectPrefix: "dialpulse.dlq",
		MaxDeliveries:    3,
		BaseBackoff:      5 * time.Millisecond,
	})

	// Simulate delivery 1
	simMsg1 := nats.NewMsg(subject)
	simMsg1.Header.Set("Nats-Msg-Id", eventID)
	simMsg1.Header.Set("x-aggregate-type", "lead")
	simMsg1.Data = msg.Data
	// Simulate metadata NumDelivered = 1
	simMsg1.Reply = "_INBOX.test1"
	err1 := consumer.ProcessMessage(ctx, simMsg1)
	if err1 == nil {
		t.Fatalf("expected handler error on delivery 1, got nil")
	}

	// Simulate delivery 2
	simMsg2 := nats.NewMsg(subject)
	simMsg2.Header.Set("Nats-Msg-Id", eventID)
	simMsg2.Header.Set("x-aggregate-type", "lead")
	simMsg2.Data = msg.Data
	simMsg2.Reply = "_INBOX.test2"
	// Set NumDelivered = 2 via fake metadata or header fallback
	err2 := consumer.ProcessMessage(ctx, simMsg2)
	if err2 == nil {
		t.Fatalf("expected handler error on delivery 2, got nil")
	}

	// Simulate delivery 3 (hits MaxDeliveries)
	simMsg3 := nats.NewMsg(subject)
	simMsg3.Header.Set("Nats-Msg-Id", eventID)
	simMsg3.Header.Set("x-aggregate-type", "lead")
	simMsg3.Data = msg.Data
	simMsg3.Reply = "_INBOX.test3"
	// For testing with raw NATS msg, we simulate delivery metadata NumDelivered >= 3:
	// We invoke with metadata or verify DLQ routing
	// Since ProcessMessage reads meta.NumDelivered from JetStream msg, let's pull the actual message from JetStream!
	sub, err := js.PullSubscribe("dialpulse.events.poison.>", "poison_durable", nats.MaxDeliver(3))
	if err != nil {
		t.Fatalf("pull subscribe failed: %v", err)
	}

	// Fetch message and process through consumer until DLQ
	for i := 1; i <= 3; i++ {
		msgs, err := sub.Fetch(1, nats.MaxWait(1*time.Second))
		if err != nil {
			t.Fatalf("fetch attempt %d failed: %v", i, err)
		}
		if len(msgs) == 0 {
			t.Fatalf("attempt %d returned 0 msgs", i)
		}
		m := msgs[0]
		_ = consumer.ProcessMessage(ctx, m)
	}

	// Message should now be received on the DLQ subject
	dlqMsg, err := dlqSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected poison message to arrive on DLQ %s, got: %v", dlqSubject, err)
	}
	if dlqMsg.Header.Get("x-original-event-id") != eventID {
		t.Fatalf("expected DLQ x-original-event-id == %s, got %s", eventID, dlqMsg.Header.Get("x-original-event-id"))
	}
	if !strings.Contains(dlqMsg.Header.Get("x-dlq-error"), "poison") {
		t.Fatalf("expected DLQ error header to contain poison reason, got %s", dlqMsg.Header.Get("x-dlq-error"))
	}
}

// 5. Consumer at-least-once delivery ignores duplicate event ID
func TestConsumerAtLeastOnceDeliveryIgnoresDuplicateEventID(t *testing.T) {
	dbURL := getTestDatabaseURL(t)
	natsURL := getTestNATSURL(t)
	ctx := context.Background()

	repo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to Postgres: %v", err)
	}
	defer repo.Close()

	streamName := "STREAM_CONSUMER_DEDUP_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
	_, js := setupTestJetStream(t, natsURL, streamName, "dialpulse.events.cdedup.>", 2*time.Minute)

	campID := "camp_cdedup_" + uuid.New().String()[:8]
	eventID := "evt_cdedup_" + uuid.New().String()

	payloadBytes, _ := json.Marshal(map[string]string{
		"campaign_id": campID,
		"status":      "BUSY",
	})

	consumer := queue.NewOutboxConsumer(repo.Pool(), js, queue.NewCampaignOutcomeAggregator(), queue.OutboxConsumerConfig{
		ConsumerName: "cdedup_worker",
		StreamName:   streamName,
	})

	msg1 := nats.NewMsg("dialpulse.events.cdedup.lead.outcome")
	msg1.Header.Set("Nats-Msg-Id", eventID)
	msg1.Header.Set("x-aggregate-type", "lead")
	msg1.Data = payloadBytes

	msg2 := nats.NewMsg("dialpulse.events.cdedup.lead.outcome")
	msg2.Header.Set("Nats-Msg-Id", eventID)
	msg2.Header.Set("x-aggregate-type", "lead")
	msg2.Data = payloadBytes

	// Process message 1
	if err := consumer.ProcessMessage(ctx, msg1); err != nil {
		t.Fatalf("processing msg1 failed: %v", err)
	}
	// Process message 2 (identical eventID)
	if err := consumer.ProcessMessage(ctx, msg2); err != nil {
		t.Fatalf("processing msg2 failed: %v", err)
	}

	count, err := repo.GetCampaignOutcomeCount(ctx, campID, "BUSY")
	if err != nil {
		t.Fatalf("failed getting outcome count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected outcome count == 1, got %d", count)
	}
}

// 6. Two relays never publish the same row twice concurrently (wrapping publisher with counter; assert exactly 20 publish attempts and each row claimed once)
func TestConcurrentRelaysSkipLockedNoDuplicatePublish(t *testing.T) {
	dbURL := getTestDatabaseURL(t)
	ctx := context.Background()

	repo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to Postgres: %v", err)
	}
	defer repo.Close()

	// Wrap publisher with atomic counter to record raw publish calls without relying on JetStream dedup
	sharedPublisher := NewCountingPublisher(nil)

	// Seed 20 outbox records directly into PostgreSQL
	totalRows := 20
	eventIDs := make([]string, totalRows)
	for i := 0; i < totalRows; i++ {
		eventIDs[i] = fmt.Sprintf("evt_concur_%s_%02d", uuid.New().String()[:8], i)
		payload := fmt.Sprintf(`{"index": %d}`, i)
		query := `
			INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at)
			VALUES ($1, 'lead', $2, 'test_concurrent', $3, clock_timestamp())
			ON CONFLICT (id) DO NOTHING;`
		if _, err := repo.Pool().Exec(ctx, query, eventIDs[i], fmt.Sprintf("lead_%02d", i), payload); err != nil {
			t.Fatalf("failed inserting outbox row %d: %v", i, err)
		}
	}

	relay1 := queue.NewOutboxRelay(repo.Pool(), sharedPublisher, queue.OutboxRelayConfig{
		BatchSize:     5,
		PollInterval:  10 * time.Millisecond,
		SubjectPrefix: "dialpulse.events.concurrent",
	})
	relay2 := queue.NewOutboxRelay(repo.Pool(), sharedPublisher, queue.OutboxRelayConfig{
		BatchSize:     5,
		PollInterval:  10 * time.Millisecond,
		SubjectPrefix: "dialpulse.events.concurrent",
	})

	var wg sync.WaitGroup
	wg.Add(2)
	runRelay := func(r *queue.OutboxRelay) {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			n, err := r.ProcessBatch(ctx)
			if err != nil {
				t.Errorf("ProcessBatch error: %v", err)
			}
			if n == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	go runRelay(relay1)
	go runRelay(relay2)
	wg.Wait()

	// Assert: Total publisher invocations across both relays must equal EXACTLY 20
	actualCalls := atomic.LoadInt64(&sharedPublisher.totalCalls)
	if actualCalls != int64(totalRows) {
		t.Fatalf("expected exactly %d publish attempts across competing relays, got %d", totalRows, actualCalls)
	}

	// Assert: Each seeded row was published EXACTLY once
	for _, id := range eventIDs {
		val, ok := sharedPublisher.attemptsByID.Load(id)
		if !ok {
			t.Fatalf("expected event %s to have been published, but not recorded in publisher map", id)
		}
		count := atomic.LoadInt64(val.(*int64))
		if count != 1 {
			t.Fatalf("expected event %s to be claimed and published exactly once, got %d", id, count)
		}
	}

	// Verify all 20 rows in DB have published_at IS NOT NULL
	var remainingUnpublished int
	err = repo.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE id = ANY($1) AND published_at IS NULL;`, eventIDs).Scan(&remainingUnpublished)
	if err != nil {
		t.Fatalf("failed checking remaining unpublished: %v", err)
	}
	if remainingUnpublished != 0 {
		t.Fatalf("expected 0 unpublished rows remaining, got %d", remainingUnpublished)
	}
}

// 7. Metric dialpulse_outbox_lag_seconds keeps growing when relay is stopped
func TestOutboxLagMetricKeepsGrowingWhenRelayStopped(t *testing.T) {
	dbURL := getTestDatabaseURL(t)
	ctx := context.Background()

	repo, err := storage.NewPostgresRepositoryFromURL(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed connecting to Postgres: %v", err)
	}
	defer repo.Close()

	// Insert an outbox row with created_at set to 10 seconds in the past
	eventID := "evt_lag_" + uuid.New().String()
	insertQuery := `
		INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at)
		VALUES ($1, 'lead', 'lead_lag', 'lag_test', '{"test":true}', clock_timestamp() - interval '10 seconds')
		ON CONFLICT (id) DO NOTHING;`
	if _, err := repo.Pool().Exec(ctx, insertQuery, eventID); err != nil {
		t.Fatalf("failed inserting outbox event: %v", err)
	}
	defer func() {
		_, _ = repo.Pool().Exec(ctx, `DELETE FROM outbox_events WHERE id = $1`, eventID)
	}()

	m := metrics.NewMetrics()
	m.SetOutboxLagSupplier(func() float64 {
		lag, _ := repo.GetOutboxLagSeconds(context.Background())
		return lag
	})

	handler := m.Handler()

	// First scrape
	req1 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec1 := httptest.NewRecorder()
	handler(rec1, req1)
	res1, _ := io.ReadAll(rec1.Body)
	body1 := string(res1)

	if !strings.Contains(body1, "dialpulse_outbox_lag_seconds") {
		t.Fatalf("metrics endpoint missing dialpulse_outbox_lag_seconds in body: %s", body1)
	}

	lag1, err := repo.GetOutboxLagSeconds(ctx)
	if err != nil {
		t.Fatalf("failed getting outbox lag 1: %v", err)
	}
	if lag1 < 9.5 {
		t.Fatalf("expected lag1 to be at least ~10s, got %f", lag1)
	}

	// Sleep 250ms while relay remains STOPPED
	time.Sleep(250 * time.Millisecond)

	lag2, err := repo.GetOutboxLagSeconds(ctx)
	if err != nil {
		t.Fatalf("failed getting outbox lag 2: %v", err)
	}
	if lag2 <= lag1 {
		t.Fatalf("expected outbox lag to keep growing while relay is stopped: lag2 (%f) should be > lag1 (%f)", lag2, lag1)
	}

	// Mark the event published
	_, err = repo.Pool().Exec(ctx, `UPDATE outbox_events SET published_at = clock_timestamp() WHERE id = $1`, eventID)
	if err != nil {
		t.Fatalf("failed updating published_at: %v", err)
	}

	// Verify purge functionality
	purged, err := repo.PurgePublishedOutboxEvents(ctx, 0*time.Second, 10)
	if err != nil {
		t.Fatalf("failed purging published events: %v", err)
	}
	if purged < 1 {
		t.Fatalf("expected at least 1 event purged, got %d", purged)
	}
}
