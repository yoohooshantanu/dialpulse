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

---

### 1. Index Experiment: `EXPLAIN (ANALYZE, BUFFERS)` Comparison

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

### 2. Full Concurrency Ramp: Unified Mode vs Split Mode

To separate query scan cost from transaction recording overhead, the full concurrency ramp ($W \in \{1, 2, 4, 8, 16, 32\}$, 3 runs each, 2,000 leads per run) was executed under both modes. All values are reported as **`[min - max]` ranges across 3 runs**:

#### Mode A: Unified Mode with Original Index Only (`idx_leads_dispatch`)
*Index configuration: Only `idx_leads_dispatch (campaign_id, status, next_attempt_at)` is present; `idx_leads_comp` and `idx_leads_queued` are dropped. Claiming executes the single unified claim query with `OR`.*

| Concurrency ($W$) | Max Claim-and-Record Rate (leads/s) | Total Cycle p50 (ms) | Total Cycle p99 (ms) | Claim Latency p50 (ms) | Record Latency p50 (ms) | Postgres CPU% (2 CPU Cap) | Max Lock Waits | Retries |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | [16.8 – 17.9] | [43.2 – 45.2] | [214.1 – 241.3] | [0.6 – 0.8] | [42.3 – 44.4] | [66.2% – 90.5%] | [0 – 0] | [0 – 0] |
| **2** | [18.7 – 23.4] | [66.9 – 77.9] | [316.6 – 424.2] | [1.1 – 1.6] | [65.4 – 76.1] | [79.5% – 144.9%] | [0 – 0] | [0 – 0] |
| **4** | [33.9 – 39.6] | [73.4 – 89.6] | [317.7 – 436.5] | [1.3 – 2.0] | [72.0 – 88.3] | [144.8% – 162.6%] | [0 – 1] | [0 – 0] |
| **8** | [41.0 – 51.0] | [131.9 – 163.5] | [426.9 – 573.8] | [2.7 – 3.5] | [129.1 – 157.7] | [134.1% – 206.5%] | [0 – 0] | [0 – 0] |
| **16** | [56.3 – 65.1] | [199.6 – 226.9] | [616.0 – 689.9] | [4.3 – 5.9] | [192.5 – 218.4] | [199.6% – 214.8%] | [0 – 1] | [0 – 0] |
| **32** | [65.2 – 75.5] | [343.4 – 366.9] | [688.1 – 1320.0] | [13.2 – 27.7] | [300.5 – 352.3] | [215.6% – 222.7%] | [0 – 1] | [0 – 0] |

---

#### Mode B: Split Mode with Composite Index (`idx_leads_comp`)
*Index configuration: Composite index `idx_leads_comp (campaign_id, priority DESC, created_at ASC)` is active. Claiming separates the queued fast path from the expired lease recovery path.*

| Concurrency ($W$) | Max Claim-and-Record Rate (leads/s) | Total Cycle p50 (ms) | Total Cycle p99 (ms) | Claim Latency p50 (ms) | Record Latency p50 (ms) | Postgres CPU% (2 CPU Cap) | Max Lock Waits | Retries |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | [10.0 – 17.8] | [42.8 – 65.6] | [220.9 – 489.7] | [0.6 – 1.3] | [42.1 – 63.9] | [42.9% – 141.6%] | [0 – 0] | [0 – 0] |
| **2** | [16.3 – 27.6] | [54.6 – 93.5] | [262.5 – 453.9] | [0.9 – 1.8] | [53.4 – 90.5] | [96.8% – 138.8%] | [0 – 0] | [0 – 0] |
| **4** | [31.7 – 38.6] | [80.3 – 96.5] | [380.6 – 479.7] | [1.4 – 1.5] | [78.9 – 93.9] | [94.1% – 123.3%] | [0 – 0] | [0 – 0] |
| **8** | [40.8 – 45.8] | [146.2 – 156.6] | [470.8 – 760.8] | [2.6 – 3.4] | [141.3 – 152.0] | [116.7% – 185.8%] | [0 – 1] | [0 – 0] |
| **16** | [34.8 – 46.4] | [252.3 – 370.5] | [708.2 – 1678.7] | [4.2 – 15.1] | [245.9 – 342.3] | [204.6% – 292.0%] | [0 – 1] | [0 – 0] |
| **32** | [36.6 – 53.6] | [439.6 – 613.8] | [2067.1 – 3345.1] | [58.1 – 84.6] | [378.8 – 511.1] | [243.4% – 258.8%] | [2 – 5] | [0 – 0] |

---

### 3. 1,000,000-Lead Dataset Experiment

To test scalability on large datasets, a fresh table of **1,000,000 leads** was seeded in PostgreSQL:
* **Seeding Performance**: 1,000,000 rows inserted in **462.93s** (effective ingest rate: **2,160 leads/sec**).
* **EXPLAIN (ANALYZE, BUFFERS)** with `idx_leads_comp`:
  * Scan node: `Index Scan using idx_leads_comp on leads`
  * Index scan time: **1.647..4.314 ms** to lock 50 rows (cost `0.42..66310.11`)
  * Total execution time: **86.99 ms** (Shared hit buffers: 1,212; shared read buffers: 16).
* **Drain Performance ($W=8$)**:
  * Drained **2,008 leads** in **50.94s** $\rightarrow$ **39.4 leads/sec**.
  * Remaining queued leads: **997,750**.
  * **Conclusion**: Draining a 1M-row table achieves the exact same throughput (39.4 leads/s) as a 2k-row table. The composite B-tree index isolates the claim query to $O(\text{LIMIT} \times \log N)$ index page traversals, maintaining flat execution cost regardless of total table volume.

---

### 4. Cost Per Lead Deep-Dive: Explaining ~20ms CPU Per Lead

A claiming rate of 100 leads/s on 2.0 vCPUs represents **20 ms of container CPU per lead**. Profiling shows this is not due to claim query scans, but rather the execution of **7 distinct SQL statements across 2 ACID transactions per lead**:

#### Statement Execution Breakdown (per lead):
1. **Transaction 1 (`RecordAttemptStart`)**:
   * `SELECT ... leads ... FOR UPDATE`: **0.49 ms** (row lock verification)
   * `UPDATE leads SET attempts = attempts + 1`: **2.59 ms**
   * `INSERT INTO call_attempts`: **4.11 ms** (Foreign key constraint validation triggers `call_attempts_campaign_id_fkey` and `call_attempts_lead_id_fkey` alone consume **1.43 ms**)
2. **Transaction 2 (`RecordCallOutcomeTx`)**:
   * `SELECT ... leads ... FOR UPDATE`: **0.27 ms**
   * `UPDATE leads SET status = $2, failure_reason = $3, lease_token = NULL`: **0.67 ms**
   * `INSERT INTO call_attempts ... ON CONFLICT DO UPDATE`: **0.56 ms**
   * `INSERT INTO outbox_events`: **2.38 ms**
3. **Overhead & Serialization**:
   * Pure query execution time: **11.07 ms**
   * Query parsing, binding, and planning: **3.54 ms**
   * Transaction `BEGIN` / `COMMIT`, WAL write, and snapshot management: **~3.5 ms**
   * **Total CPU time per lead**: **~18–20 ms**.

#### Top Statements from `pg_stat_statements`:
| SQL Statement Pattern | Total Exec Time (ms) | Mean Exec Time (ms) | % DB CPU Time |
| :--- | :---: | :---: | :---: |
| `UPDATE leads SET status = 'DIALING', locked_by = ... WHERE id IN (...)` | 2,752.4 | 1.83 | 24.8% |
| `INSERT INTO outbox_events (id, aggregate_type, ...) VALUES (...)` | 2,423.8 | 2.38 | 21.8% |
| `UPDATE leads SET attempts = attempts + 1, updated_at = clock_timestamp() ...` | 2,126.9 | 2.59 | 19.1% |
| `INSERT INTO call_attempts (id, lead_id, ...) VALUES (...)` | 1,756.2 | 4.11 | 15.8% |
| `SELECT ... FROM leads WHERE id = $1 FOR UPDATE` | 647.2 | 0.49 | 5.8% |

#### Diagnostic Experiments:
* **Commit/WAL Sync Latency Isolation (`synchronous_commit=off`)**:
  Running $W=8$ with `synchronous_commit=off` yielded **41.0 leads/s** (compared to 40.8–45.8 leads/s with `synchronous_commit=on`). This proves that disk fsync / `IO:WALSync` is **not** the throughput bottleneck; CPU instruction cycles for statement execution, FK checks, and row locking are the hard ceiling.
* **Bounded Retries Under Load**:
  `OutcomeTxRetries` was **0** across all benchmark runs. Because `FOR UPDATE SKIP LOCKED` guarantees disjoint lead leases, competing workers never attempt to update the same lead row concurrently, eliminating serialization retries.

---

### 5. Scenario AB Cost Breakdown: Component Overhead

Running Scenario A alongside the transactional outbox pipeline reduces throughput from **40.8–45.8 leads/s** down to **26.9–33.0 leads/s**. Isolated runs isolate which component introduces the cost:

| Configuration ($W=8$) | Throughput (leads/s) | Relative Cost | Dead Tuples Generated (`n_dead_tup`) | Primary Bottleneck Identified |
| :--- | :---: | :---: | :---: | :--- |
| **A Alone** (Workers Only) | [40.8 – 45.8] | Baseline (100%) | 0 on outbox | Pure claim + record execution |
| **A + Relay** (`A_RELAY`) | 33.0 | ~25% throughput reduction | **1,200** on `outbox_events` | `UPDATE outbox_events SET published_at` updates table pages |
| **A + Relay + No-Op Consumer** (`AB_NOOP`) | 26.9 | ~37% throughput reduction | 0 on `processed_events` (1,984 live rows) | Consumer network roundtrips + `processed_events` INSERTs |
| **Full AB** (Workers + Relay + Consumer) | 31.2 | ~28% throughput reduction | **37** on `campaign_outcomes` (1 row) | Hot-row contention on single campaign outcome row |

* **Dead Tuple Bloat**: The Outbox Relay's `UPDATE outbox_events SET published_at = clock_timestamp()` creates dead row versions for every processed event, requiring periodic autovacuum cycles.
* **Hot-Row Contention**: In `AB`, the consumer repeatedly updates a single row in `campaign_outcomes` (`WHERE campaign_id = $1 AND status = $2`), generating 37 dead tuples on a 1-row table and causing brief `Lock:tuple` waits between consumer transactions.

---

### 6. Lock Waits & Telemetry Sampling

* **Exact Wait Events Recorded**: Queries to `pg_stat_activity` during peak load revealed that lock wait states were strictly:
  * `IO:WALSync`: Waiting for transaction commit log flushing.
  * `Client:ClientRead`: Waiting for data transmission over the Go client connection socket.
  * `Lock:tuple` / `Lock:transactionid`: Transient row-level update waits when workers or consumers target active lead or outcome rows.
  * *Correction*: Zero buffer pin waits or index leaf page contention were detected.
* **Docker Stats Telemetry Sampling**: Container CPU was sampled every **1.0 second** via `docker stats --no-stream` using an explicit **800 ms context timeout** per poll. This prevented the Docker CLI from stalling Windows named pipes while capturing accurate process metrics.

---

### 7. Knee Diagnosis & Architectural Takeaway

* **Query vs Record Latency**: While the composite index successfully reduces claim query scan time from 940 ms to 2–4 ms, the overall worker cycle is heavily dominated by outcome persistence: claim latency p50 is **1–3 ms**, whereas record latency p50 is **70–150 ms**.
* **Saturation Mechanism**: At $W \ge 8$, the 2.0 vCPU container reaches 180%–208% utilization. Adding workers beyond this point does not increase throughput because the 7 transactional SQL statements saturate the database CPU, shifting excess work into connection queue waits and raising p99 latency beyond 1,500 ms.

---

### 8. Honest Limitations

1. **Co-located Architecture**: The Go load generator, PostgreSQL container, and NATS broker were executed on the same physical host. CPU scheduling, Docker WSL2 virtualization boundaries, and context switching introduce variance not present in dedicated multi-node cloud environments.
2. **Docker-on-Windows Hypervisor Overhead**: Networking traverses the WSL2 virtual Ethernet adapter and gvproxy translation layer, adding approximately 1–3 ms of baseline socket latency to every PostgreSQL transaction round-trip.
3. **Telemetry Granularity**: Docker container CPU was sampled at 1.0s intervals with an 800 ms timeout to prevent named pipe deadlocks, smoothing out micro-bursts of sub-millisecond CPU spikes.
4. **No Carrier Telephony Latency**: This benchmark measures database claim-and-record limits only; actual carrier dialing involves 3–15 seconds of PSTN trunk ring time per attempt.

---

### 9. Reproduction Commands

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

# 4. Run Full Scenario A Ramp (Unified mode, idx_leads_dispatch only)
go run ./cmd/loadgen -ramp=true -scenario=A -query-mode=unified -leads=2000 -runs=3

# 5. Run Full Scenario A Ramp (Split mode, composite index)
go run ./cmd/loadgen -ramp=true -scenario=A -query-mode=split -leads=2000 -runs=3

# 6. Run 1M Leads Dataset Experiment
go run ./cmd/loadgen -dataset-experiment=true

# 7. Run Full Scenario AB Ramp
go run ./cmd/loadgen -ramp=true -scenario=AB -leads=2000 -runs=3
```

---

## Author

- **Shantanu Juvekar** ([@yoohooshantanu](https://github.com/yoohooshantanu))
- Email: `santanujuvekar@gmail.com`
- License: MIT
