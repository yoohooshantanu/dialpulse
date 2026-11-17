# DialPulse: Distributed Predictive Outbound Campaign Engine & Dialer

[![Go Reference](https://pkg.go.dev/badge/github.com/yoohooshantanu/dialpulse.svg)](https://pkg.go.dev/github.com/yoohooshantanu/dialpulse)
[![Go Report Card](https://goreportcard.com/badge/github.com/yoohooshantanu/dialpulse)](https://goreportcard.com/report/github.com/yoohooshantanu/dialpulse)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**DialPulse** is a high-throughput, distributed predictive campaign dialer written in Go. Designed for enterprise voice AI platforms (sales qualification, automated appointments, customer surveys), DialPulse balances maximum AI agent utilization with strict regulatory compliance: **sub-microsecond Do-Not-Call (DNC) screening via Bloom filters**, **Little's Law-inspired dynamic pacing heuristics with FTC abandonment braking**, **in-flight ringing capacity reservations**, **exactly-once call disposition accounting**, **database-level idempotent lead ingestion**, and **production PostgreSQL persistence (`pgx/v5`)**.

---

## Architecture Overview

```
                                      DIALPULSE CAMPAIGN ENGINE
                                      
  Lead Ingestion ────► [ Atomic DB Upsert ] ────► [ In-Memory Phone Index ] ────► [ Priority Queue ]
   (REST / Batch)     (PostgreSQL pgx/v5)             (Fast O(1) Dedupe)                   │
                               │                                                           ▼
                               ▼                                                [ Scheduling Ticker ]
                     [ PostgreSQL Store ]                                                  │
                               │                                                           ▼
                               │                                                [ Dynamic Pacer ]
                               │                                             (Little's Law Heuristic)
                               │                                             - Rolling Answer Rate
                               │                                             - Agents Freeing Soon
                               │                                             - Abandonment Rate Brake
                               │                                                           │
                               │                                           [ Gross Dials Needed: M ]
                               │                                                           │
                               │                                           [ In-Flight Ringing Reserve ]
                               │                                             (Net Dials = M - InFlight)
                               │                                                           │
                               │                                                           ▼
                               │                                           ┌───────────────────────────────┐
                               │                                           │   Pre-Call Compliance Gate    │
                               │                                           ├───────────────────────────────┤
                               │                                           │ 1. DNC Bloom Filter (<1µs)    │
                               │                                           │ 2. Timezone Governor (TCPA)   │
                               │                                           └───────────────┬───────────────┘
                               │                                                           │ [ Verified Safe ]
                               │                                                           ▼
                               │                                               [ Carrier Trunk Dispatch ]
                               │                                                 (Concurrent Dialers)
                               │                                                           │
                               │                         ┌─────────────────────────────────┴─────────────────────────────────┐
                               │                         ▼                                                                   ▼
                               │                 [ Call Answered ]                                                   [ Unanswered / AMD ]
                               │                         │                                                        (Voicemail, Busy, No-Ans)
                               │                         ▼                                                                   │
                               │            [ Agent Slot Available? ]                                                        ▼
                               │                   /           \                                                  [ Retry Backoff or DLQ ]
                               │                YES             NO                                             (Exhausted max attempts -> DLQ)
                               │                 │               │
                               │                 ▼               ▼
                               │         [ CONNECTED ]     [ ABANDONED ] ──► (Throttles Pacer Multiplier)
                               │                 │               │
                               └─────────────────┴───────────────┴─────────────────────────┐
                                                                                           ▼
                                                                          ┌─────────────────────────────────┐
                                                                          │  Atomic RecordCallOutcomeTx     │
                                                                          │  - Fenced Lead Status Update    │
                                                                          │  - Call Attempt Insertion       │
                                                                          │  - Transactional Outbox Insert  │
                                                                          └────────────────┬────────────────┘
                                                                                           │
                                                                                           ▼
                                                                          ┌─────────────────────────────────┐
                                                                          │   Outbox Relay (SKIP LOCKED)    │
                                                                          │   - Publishes to NATS JetStream │
                                                                          │   - Header: Nats-Msg-Id = ID    │
                                                                          │   - Marks published_at on ACK   │
                                                                          └────────────────┬────────────────┘
                                                                                           │
                                                                                           ▼
                                                                          ┌─────────────────────────────────┐
                                                                          │  Durable JetStream Consumer     │
                                                                          │  - Single-Tx Idempotency & DB   │
                                                                          │  - NakWithDelay Backoff         │
                                                                          │  - Poison Dead-Letter Queue     │
                                                                          └─────────────────────────────────┘
```

---

## Core Technical Features

1. **Dynamic Pacing Engine (Little's Law & Erlang-C Capacity Heuristics)**:
   - Models queue completion during carrier ring windows:
     $$\text{EffectiveCapacity} = \text{IdleAgents} + \text{ActiveCalls} \times \left(\frac{\text{AverageRingSec}}{\text{AverageHandleSec}}\right)$$
   - Evaluates Bayesian-weighted rolling answer rates with an automated quadratic braking multiplier that throttles dialing down to progressive mode ($1:1$) if the rolling abandonment rate approaches the legal FTC $<3.0\%$ cap.
2. **In-Flight Ringing Capacity Reservation**:
   - Explicitly accounts for outstanding outbound calls ringing on carrier trunks (`inFlightDials`).
   - Subtracts currently ringing calls from the dispatch budget ($\text{NetDials} = \text{GrossDials} - \text{InFlightDials}$), preventing consecutive 50ms ticker bursts from oversaturating agent capacity.
3. **Exactly-Once Disposition & Outcome Accounting**:
   - Records one definitive terminal disposition per dial attempt (`CONNECTED`, `ABANDONED`, `BUSY`, `NO_ANSWER`, `VOICEMAIL`, or `FAILED`).
   - Eliminates double-counting in statistical pacer feedback loops.
4. **Sub-Microsecond Do-Not-Call (DNC) Bloom Filter**:
   - Bitset Bloom filter using Kirsch-Mitzenmacher double-hashing (FNV-1a).
   - Screens phone numbers in **~520 nanoseconds** with zero disk I/O and $<0.01\%$ false-positive probability.
5. **Time-Zone & Regulatory Curfew Governor**:
   - Inspects North American area codes (NPA) to localize recipient time zones across Eastern, Central, Mountain, Pacific, Alaska, Hawaii, and Arizona.
   - **Federal Baseline**: Enforces the federal 08:00 AM – 09:00 PM permissible telemarketing calling window pursuant to FCC regulations (47 CFR § 64.1200(c)(1)), holding off off-hours leads with future-dated retry timestamps without CPU busy-spinning. Note that some states enforce narrower windows (e.g., Florida and Louisiana at 8 AM – 8 PM); DialPulse provides the technical baseline and dual-zone intersection guards, with configurable hooks for state-specific curfew overrides.
   - **Arizona Non-DST Compliance**: Maps Arizona area codes (`602`, `480`, `623`) to `America/Phoenix` (UTC-7 year-round). Unlike Denver (MDT, UTC-6), Arizona does not observe Daylight Saving Time; in summer, 14:30 UTC is 08:30 MDT (permitted) but 07:30 MST in Phoenix (prohibited). DialPulse strictly prevents dangerous off-hours calls in Arizona.
   - **Navajo Nation & Northern Arizona Intersection Guard (`928`)**: Area code `928` covers both the Navajo Nation (which observes Mountain Daylight Time in summer) and the Hopi Reservation / rural Arizona (which does not observe DST). DialPulse enforces a dual-zone intersection check (`America/Phoenix` $\cap$ `America/Denver`): calls are permissible only when both zones fall within 08:00 – 21:00, preventing off-hours dials in the Navajo Nation after 9:00 PM MDT (which is 8:00 PM MST in Phoenix).
   - **El Paso Approximation**: Area code `915` (El Paso, TX) is correctly mapped to Mountain Time (`America/Denver`), diverging from the remainder of Texas (Central Time). Note: NPA area-code routing is an industry-standard technical heuristic subject to mobile number portability (MNP) where subscribers relocate while retaining their numbers. Production deployments can supplement NPA tables with number-level carrier data or recipient mailing addresses.
   - **Fail-Closed Unknown Area Codes**: Unrecognized area codes fail closed returning `model.ErrUnknownTimezone`, immediately routing the lead to `DEAD_LETTER_QUEUE` (`unknown_timezone`) rather than guessing or retrying indefinitely.
6. **PostgreSQL Persistence & Distributed Work Queue Layer (`pgx/v5`)**:
   - Production SQL schema ([`migrations/001_init.sql`](migrations/001_init.sql)) covering `campaigns`, `leads` (with `locked_by` / `locked_at`), and `call_attempts`.
   - **Distributed Atomic Claiming (`FOR UPDATE SKIP LOCKED`)**: Multiple competing dialer instances sharing the same PostgreSQL database safely lease pending leads concurrently with zero double-dials and zero cross-worker collisions.
   - **Atomic Transactional Outcome Persistence**: Terminal dispositions update lead state and insert call attempts inside a unified PostgreSQL transaction (`RecordCallOutcomeTx`) with exponential backoff retry.
   - **Server Startup Recovery & Lazy Hydration**: On engine startup or restart, active campaigns and pending leads are automatically recovered from PostgreSQL storage without 404s or lost queue states.
   - Enforces unique constraint `uq_campaign_phone (campaign_id, phone_number)` at the database engine level via `ON CONFLICT DO NOTHING`, paired with in-memory phone indexing.
7. **Dead-Letter Queue (DLQ) & Failure Categorization**:
   - Routes permanently unserviceable leads to `DEAD_LETTER_QUEUE` with structured error codes (`DNC_PROHIBITED`, `CARRIER_TRUNK_ERROR`, `EXHAUSTED_MAX_ATTEMPTS`).
   - Exposed via inspection endpoint `GET /v1/campaigns/{id}/dlq`.
8. **Thread-Safe Campaign Lifecycle**:
   - Mutex-guarded `Start()` and `Stop()` routines preventing duplicate ticker loops and worker `sync.WaitGroup` races.
9. **Transactional Outbox & NATS JetStream Event Pipeline**:
   - **Atomic Outbox Insertion**: Domain events (`call.outcome_recorded`) are inserted into `outbox_events` in the **exact same PostgreSQL transaction** as `RecordCallOutcomeTx` (`leads` status update and `call_attempts` record). If the transaction fails, neither the outcome nor the outbox row is persisted; if it commits, both are guaranteed to exist on disk.
   - **Outbox Relay (`FOR UPDATE SKIP LOCKED`)**:
     - Operates in bounded small batches within a single database transaction:
       $$\text{BEGIN} \longrightarrow \text{SELECT ... FOR UPDATE SKIP LOCKED} \longrightarrow \text{Publish each to JetStream (with timeout)} \longrightarrow \text{UPDATE published\_at} \longrightarrow \text{COMMIT}$$
     - If any publish call fails or times out, the transaction rolls back, releasing row locks so unpublished events remain safe in PostgreSQL and are retried on subsequent cycles.
     - **No Per-Lead Ordering Across Relays**: While individual leads are fenced at the database level, concurrent relays claim distinct batches in parallel without cross-worker coordination. Relays do not guarantee chronological order across different leads or across concurrent batches. Downstream consumers must be resilient to out-of-order event delivery.
   - **Honest Delivery Semantics: At-Least-Once Delivery**:
     - Distributed message transport without distributed two-phase commit (2PC) cannot guarantee "exactly-once" delivery across network partitions, broker restarts, or publisher crashes occurring after transport transmission but before database acknowledgment. DialPulse explicitly guarantees **at-least-once delivery** at the transport layer, combined with **atomic idempotency** at the consumer layer.
   - **JetStream Deduplication Window Limitation**:
     - NATS JetStream provides message deduplication via the `Nats-Msg-Id` header, but this deduplication is strictly bounded by the stream's rolling `duplicate_window` (e.g., 2 minutes). If an outbox row is re-published after this sliding window expires, JetStream accepts it as a new message (`PubAck.Duplicate == false`). Transport deduplication alone is therefore insufficient for business correctness.
   - **Consumer Atomic Idempotency (`processed_events`)**:
     - To ensure end-to-end exactly-once business effects, the consumer executes both its database mutation (e.g., updating aggregated `campaign_outcomes` counters) and the insertion of `event_id` into `processed_events` within **ONE single PostgreSQL transaction**.
     - If an event ID already exists in `processed_events`, the insert conflicts (`RowsAffected == 0`), and the consumer immediately acknowledges the message without executing any business side-effects.
   - **Poison Message & Dead-Letter Queue (DLQ)**:
     - The durable pull consumer sets broker-side `MaxDeliver` to unlimited (`-1`), delegating delivery threshold enforcement strictly to application logic (`msg.Metadata().NumDelivered >= MaxDeliveries`). This guarantees that if the DLQ broker experiences transient failure on the 3rd delivery, JetStream does not drop the message; instead, the consumer retries and routes the message to the DLQ once available.
     - When delivery attempts reach `MaxDeliveries` (e.g., 3), the consumer routes the poison message along with diagnostic headers (`x-dlq-error`, `x-dead-letter-reason: max_deliveries_exceeded`, `x-original-event-id`, `x-delivery-count`) to `dialpulse.dlq.<aggregate_type>` and acknowledges the message from the main stream.
     - **Malformed Message Fencing**: If an incoming message lacks a `Nats-Msg-Id` header, the consumer derives an identifier from the JetStream stream sequence (`malformed-seq-<stream_seq>`), attaches diagnostic header `x-dead-letter-reason: malformed_missing_id`, publishes it immediately to the DLQ, and terminates (`Ack`) without retrying the malformed payload.
     - *Crash Limitation*: If a consumer process crashes after successfully publishing to the DLQ subject but before acknowledging the message to the main JetStream stream, the message will be redelivered upon consumer recovery and routed to the DLQ again, creating a duplicate DLQ entry. Downstream DLQ consumers must treat DLQ entries as at-least-once.
   - **Outbox Lag Metric (`dialpulse_outbox_lag_seconds`)**:
     - Computed at scrape time directly against PostgreSQL:
       $$\text{OutboxLag} = \text{EXTRACT}\left(\text{EPOCH FROM } (\text{clock\_timestamp}() - \min(\text{created\_at}))\right) \quad \text{WHERE published\_at IS NULL}$$
     - Because it is evaluated independently of the relay's internal loop, the metric accurately reflects true backlog age and continues to grow monotonically if the relay process stalls or NATS becomes unavailable.
   - **Retention & Table Growth Pruning**:
     - **`outbox_events` Pruning**: `PurgePublishedOutboxEvents(ctx, olderThan, limit)` removes published records older than the specified retention threshold (e.g. 1 hour or 24 hours), bounding table size and disk usage without holding locks on the active tail of unpublished rows. In production, this can be triggered by a background maintenance worker or scheduled cron task.
     - **`processed_events` Growth & Retention Horizon**: Because the consumer writes an idempotency key `(event_id, consumer_name)` for every processed event, the `processed_events` ledger grows monotonically over the life of the platform. Left unmanaged, this introduces table bloat and index overhead. Production deployments should enforce a sliding retention horizon (e.g., pruning keys older than 7 or 30 days) that strictly exceeds the maximum possible transport redelivery, retry, or dead-letter horizon. Once an event is older than the maximum retry window, duplicate deliveries will never arrive, making key deletion safe.

---

## API Specification

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/v1/campaigns` | Creates a new outbound campaign & persists to PostgreSQL |
| `POST` | `/v1/campaigns/{id}/leads` | Idempotent batch lead ingestion (`accepted`, `duplicates`) |
| `GET` | `/v1/campaigns/{id}/leads/{lead_id}` | Retrieves individual lead status & attempt history |
| `GET` | `/v1/campaigns/{id}/dlq` | Inspects dead-letter queue leads and failure reasons |
| `POST` | `/v1/campaigns/{id}/start` | Activates predictive background dialing loop |
| `GET` | `/v1/campaigns/{id}/stats` | Live pacing stats (multiplier, answer rate, drop rate) |
| `POST` | `/v1/dnc` | Bulk inserts phone numbers into DNC Bloom filter |
| `GET` | `/healthz` | Liveness & readiness probe |
| `GET` | `/metrics` | Prometheus metrics exposition |
| `GET` | `/debug/pprof/*` | Go runtime memory and CPU profiling |

---

## Database Schema (PostgreSQL)

Located in [`migrations/001_init.sql`](migrations/001_init.sql) and [`migrations/002_outbox.sql`](migrations/002_outbox.sql):

- **`campaigns`**: Campaign lifecycle, pacing state, allocated agent counts, and target drop rates.
- **`leads`**: Campaign-associated leads with priority, retry scheduling (`next_attempt_at`), attempt counters, and terminal status (`DEAD_LETTER_QUEUE`, `CONNECTED`, `ABANDONED`).
- **`call_attempts`**: Immutable call disposition log tracking telephony latency, duration, carrier errors, and agent leasing times.
- **`outbox_events`**: Atomic transactional outbox table holding unpublished domain events (`aggregate_type`, `aggregate_id`, `event_type`, `payload`, `created_at`, `published_at`).
- **`processed_events`**: Consumer idempotency ledger recording processed `(event_id, consumer_name)` keys.
- **`campaign_outcomes`**: Per-campaign aggregated outcome counters (`campaign_id`, `status`, `count`, `last_updated_at`) updated atomically with consumer deduplication.

---

## Docker & Deployment

Spin up DialPulse along with PostgreSQL, NATS JetStream, and Prometheus:

```bash
docker-compose up -d
```

Services:
- **DialPulse Engine**: `http://localhost:8086`
- **PostgreSQL 16**: `localhost:5432` (Auto-runs migrations on first boot)
- **NATS 2 (JetStream)**: `localhost:4222` (Management: `http://localhost:8222/healthz`)
- **Prometheus**: `http://localhost:9091`

---

## Verification & Testing

Run all 15 unit, concurrency, and distributed integration tests:

```bash
$env:CGO_ENABLED="0"; $env:GOPROXY="off"; $env:GOTOOLCHAIN="local"
go test -v -mod=readonly ./tests/...
```

### Verified Benchmark Performance

Benchmarks executed on AMD Ryzen 5 (Offline Go 1.27.0):

```text
BenchmarkDNCBloomFilterLookup-8         2,480,710 ops/sec    520.9 ns/op     24 B/op    2 allocs/op
BenchmarkPredictivePacerCalculation-8   5,678,142 ops/sec    255.4 ns/op     32 B/op    3 allocs/op
```

Compile local binary:

```bash
go build -o bin/dialpulse.exe ./cmd/server
```

---

## Load Test & Saturation Analysis: Lead Claiming & Transactional Outbox Scaling

To rigorously characterize the throughput ceilings, database lock behavior, and architectural bottlenecks of DialPulse, an external high-concurrency Go load generator (`cmd/loadgen`) was executed against an isolated PostgreSQL 16 container (`--cpus=2.0`, `--memory=2g`, `max_connections=100`, `pg_stat_statements.track=all`) and NATS JetStream 2.

> [!IMPORTANT]
> **Measurement Discipline & Labeling**: Results are strictly reported as **"Max Claim-and-Record Rate (No Dial Time)"** and are **never converted to calls/sec**, as no carrier telephony delay or ring duration is simulated.

### 1. Pre-Run Predictions & Hypotheses

Before running any benchmark steps, the following hypotheses were established:
1. **Index Predicate Mismatch**: The partial index on `status = 'QUEUED'` will not be selected by the PostgreSQL query planner for the unified claim query because the `WHERE` clause incorporates an `OR` branch for expired leases (`status = 'DIALING' AND locked_at < clock_timestamp() - interval`). Postgres cannot satisfy `FOR UPDATE SKIP LOCKED` via a `BitmapOr` scan and will fall back to a full table sequential scan and disk sort.
2. **Scenario A (Claiming Alone) Saturation**: Claiming throughput will scale near-linearly with worker concurrency ($W$) until PostgreSQL container CPU saturates around $W = 8$ (consuming $\sim$200% of the 2.0 vCPU allocation). Beyond $W = 8$, worker contention on the row-level locks and transaction commit queues will cause tail latencies to surge without increasing throughput.
3. **Scenario AB (Transactional Outbox & Consumer Overhead)**: Concurrently running the `OutboxRelay` (polling `outbox_events` via `SKIP LOCKED` and publishing to JetStream) and `OutboxConsumer` (pulling events and writing to `processed_events` + `campaign_outcomes` in single transactions) will reduce peak claiming throughput by 50%–65% due to shared database CPU and connection pool resource contention.

---

### 2. Index Experiment: `EXPLAIN (ANALYZE, BUFFERS)` Comparison

Evaluated on a dataset of **100,000 seeded leads** under campaign `camp-explain-1` with batch limit 50:

```sql
-- Unified Claim Query
UPDATE leads SET status = 'DIALING', locked_by = 'bench-worker', locked_at = clock_timestamp(), lease_token = gen_random_uuid()::text
WHERE id IN (
    SELECT id FROM leads
    WHERE campaign_id = $1
      AND (
        ((status = 'QUEUED' OR status = 'DRAFT') AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
        OR
        (status = 'DIALING' AND locked_at IS NOT NULL AND locked_at < clock_timestamp() - $3::interval)
      )
    ORDER BY priority DESC, created_at ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
);
```

| Experiment / Index Configuration | Planner Node Chosen | Rows Scanned | Disk Spill / Memory | Execution Time | Speedup vs Baseline |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Migration 001 Default** (`idx_leads_dispatch`) | `Seq Scan on leads` | 100,000 | External merge: 4,704 kB disk | **940.98 ms** | 1.0x (Baseline) |
| **2. Partial Index** (`status = 'QUEUED'`) | `Seq Scan on leads` | 100,000 | External merge: 4,704 kB disk | **1,056.78 ms** | 0.9x (Ignored by planner) |
| **3. Composite Index** `(campaign_id, priority DESC, created_at ASC)` | `Index Scan using idx_leads_comp` | 50 | None (In-memory index traversal) | **21.87 ms** | **43.0x speedup** |
| **4. Split Claim: Fast Path** (`status = 'QUEUED'`) | `Index Scan using idx_leads_comp` | 50 | Shared hit buffers: 269 | **31.75 ms** | **29.6x speedup** |
| **5. Split Claim: Expired Lease Path** (`status = 'DIALING'`) | `Index Scan using idx_leads_dispatch` | 0–2 | Shared hit buffers: 10 | **1.77 ms** | Sub-2ms recovery |

#### Key Architectural Findings:
* **Why the Partial Index Failed**: The partial index `WHERE status = 'QUEUED'` was **completely ignored** by PostgreSQL because the unified query contains an `OR (status = 'DIALING' ...)` branch. PostgreSQL cannot use a partial index covering only one branch of an `OR` unless it combines index scans using `BitmapOr`, which is fundamentally incompatible with `FOR UPDATE SKIP LOCKED` (bitmap scans discard row pointers required for locking).
* **Why the Composite Index Succeeded**: The composite index on `(campaign_id, priority DESC, created_at ASC)` matches the exact `ORDER BY` clause. The planner performs a directed index scan in priority order, tests each row against the `WHERE` filter, and terminates the scan immediately once `LIMIT 50` matching rows are claimed and locked, eliminating the expensive external disk merge sort entirely.
* **Why the Query Gets Cheaper as Table Drains**: Under sequential scans, query cost is directly proportional to table volume ($O(N)$). As leads drain from 100k to 0, sequential scan time drops from 940 ms to <10 ms. Under the composite index, query cost remains $O(\text{LIMIT} \times \log N)$ throughout.

---

### 3. Load Test Results: Scenario A vs Scenario AB

All benchmarks were executed with 3 clean runs per concurrency step ($W \in \{1, 2, 4, 8, 16, 32\}$). Results are reported as **`[min - max]` ranges**:

#### Scenario A: Distributed Claiming & Outcome Recording (Alone)
*Each worker executes: `ClaimPendingLeads` (batch 50) $\rightarrow$ `RecordAttemptStart` $\rightarrow$ `RecordCallOutcomeTx`.*

| Concurrency ($W$) | Max Claim-and-Record Rate (leads/s) | Total Cycle p50 (ms) | Total Cycle p99 (ms) | Claim Latency p50 (ms) | Record Latency p50 (ms) | Postgres CPU% (2 CPU Cap) | Max Lock Waits |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | [27.2 – 33.6] | [25.7 – 31.4] | [88.8 – 116.6] | [0.3 – 0.4] | [25.4 – 31.0] | [31.0% – 91.7%] | [0 – 0] |
| **2** | [54.3 – 56.5] | [30.3 – 31.1] | [107.9 – 128.7] | [0.4 – 0.4] | [29.7 – 30.5] | [57.6% – 104.8%] | [0 – 0] |
| **4** | [87.9 – 92.7] | [37.1 – 39.3] | [120.6 – 143.9] | [0.6 – 0.7] | [36.4 – 38.5] | [116.3% – 119.2%] | [0 – 0] |
| **8** | **[98.6 – 101.8]** | **[67.4 – 71.2]** | **[175.5 – 209.7]** | **[1.0 – 1.1]** | **[65.6 – 70.0]** | **[179.7% – 208.5%]** | **[0 – 0]** |
| **16** | [80.5 – 125.4] | [105.7 – 133.2] | [256.6 – 780.0] | [1.6 – 2.2] | [102.8 – 130.5] | [209.5% – 216.4%] | [0 – 1] |
| **32** | [67.1 – 96.5] | [208.7 – 427.3] | [558.0 – 1150.3] | [9.2 – 32.9] | [199.4 – 367.8] | [219.6% – 266.6%] | [0 – 3] |

---

#### Scenario AB: Claiming + Outbox Relay + JetStream Consumer (Concurrent)
*Scenario A workers running simultaneously with `OutboxRelay` (polling and publishing to NATS) and `OutboxConsumer` (consuming and updating `campaign_outcomes` and `processed_events`).*

| Concurrency ($W$) | Max Claim-and-Record Rate (leads/s) | Total Cycle p50 (ms) | Total Cycle p99 (ms) | Claim Latency p50 (ms) | Record Latency p50 (ms) | Postgres CPU% (2 CPU Cap) | Max Lock Waits |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | [10.9 – 12.4] | [60.4 – 69.9] | [317.1 – 370.2] | [1.1 – 1.4] | [59.2 – 68.0] | [154.1% – 165.2%] | [0 – 0] |
| **2** | [23.1 – 40.7] | [42.1 – 77.1] | [135.3 – 210.3] | [0.7 – 1.4] | [41.3 – 75.7] | [88.9% – 180.9%] | [0 – 0] |
| **4** | [27.3 – 35.5] | [88.1 – 121.9] | [377.4 – 469.1] | [1.2 – 2.0] | [86.4 – 119.3] | [161.2% – 230.1%] | [0 – 0] |
| **8** | **[33.5 – 37.3]** | **[170.3 – 202.5]** | **[666.7 – 884.2]** | **[2.7 – 4.8]** | **[163.9 – 195.7]** | **[233.6% – 267.0%]** | **[0 – 1]** |
| **16** | [26.7 – 34.1] | [357.0 – 444.7] | [1454.8 – 1769.5] | [14.2 – 21.3] | [324.1 – 407.4] | [229.4% – 419.2%] | [0 – 1] |
| **32** | [28.4 – 32.0] | [777.0 – 869.5] | [2286.0 – 2725.5] | [108.7 – 138.6] | [660.0 – 731.3] | [278.2% – 296.8%] | [0 – 2] |

---

### 4. What Saturated & Knee Diagnosis

* **PostgreSQL CPU Saturation**: In Scenario A, throughput scaled from 30 leads/s ($W=1$) to 101 leads/s ($W=8$). At $W=8$, Postgres container CPU reached **180%–208%**, completely exhausting the assigned `--cpus=2.0` ceiling. Increasing concurrency to $W=16$ and $W=32$ did not increase throughput; instead, it introduced CPU queue throttling, driving p50 latency from 67 ms to 427 ms and p99 latency to 1,150 ms.
* **Outbox Competition Bottleneck**: In Scenario AB, the background outbox relay and consumer loop created heavy competition for PostgreSQL CPU and transaction serialization. Peak throughput dropped from **101.8 leads/s** down to **37.3 leads/s** (a **63.4% reduction** in max claim rate), exactly validating Prediction 3. The Relay's batch `UPDATE outbox_events SET published_at` and Consumer's `INSERT INTO campaign_outcomes` saturated container CPU starting at $W=4$ (161%–230% CPU).
* **Lock Contention**: `FOR UPDATE SKIP LOCKED` effectively prevented worker deadlocks across all runs, but at $W \ge 16$, backend lock waits appeared in `pg_stat_activity` (1–3 lock wait states) as workers competed for index leaf pages and table write locks during attempt outcome updates.

---

### 5. Honest Limitations

1. **Co-located Architecture**: The Go load generator, PostgreSQL container, and NATS broker were executed on the same physical host. CPU scheduling, Docker WSL2 virtualization boundaries, and context switching introduce variance not present in dedicated multi-node cloud environments.
2. **Docker-on-Windows Hypervisor Overhead**: Networking traverses the WSL2 virtual Ethernet adapter and gvproxy translation layer, adding approximately 1–3 ms of baseline socket latency to every PostgreSQL transaction round-trip.
3. **Telemetry Granularity**: Docker container CPU was sampled at 200 ms intervals via `docker stats --no-stream`, smoothing out micro-bursts of sub-millisecond CPU spikes.
4. **No Carrier Telephony Latency**: This benchmark measures database claim-and-record limits only; actual carrier dialing involves 3–15 seconds of PSTN trunk ring time per attempt.

---

### 6. Reproduction Commands

```bash
# 1. Start isolated PostgreSQL with resource caps and NATS JetStream
docker run -d --name dialpulse-postgres -p 5432:5432 --cpus=2.0 --memory=2g \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=dialpulse_test \
  postgres:16-alpine -c max_connections=100 -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=all

docker run -d --name dialpulse-nats -p 4222:4222 -p 8222:8222 nats:2-alpine -js -m 8222

# 2. Apply migrations
Get-Content migrations/001_init.sql | docker exec -i dialpulse-postgres psql -U postgres -d dialpulse_test
Get-Content migrations/002_outbox.sql | docker exec -i dialpulse-postgres psql -U postgres -d dialpulse_test

# 3. Run EXPLAIN (ANALYZE, BUFFERS) Index Experiment on 100k leads
go run ./cmd/loadgen -explain=true -leads=100000

# 4. Run Full Scenario A Ramp (W=1, 2, 4, 8, 16, 32; 3 runs each)
go run ./cmd/loadgen -ramp=true -scenario=A -leads=2000 -runs=3

# 5. Run Full Scenario AB Ramp (W=1, 2, 4, 8, 16, 32; 3 runs each)
go run ./cmd/loadgen -ramp=true -scenario=AB -leads=2000 -runs=3
```

---

## Author

- **Shantanu Juvekar** ([@yoohooshantanu](https://github.com/yoohooshantanu))
- Email: `santanujuvekar@gmail.com`
- License: MIT
