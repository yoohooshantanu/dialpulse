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
>
> **Loadgen Binary Provenance**:
> - The initial ramp benchmarks in Section 2 (Mode A vs Mode B) were generated with `loadgen` v1.
> - The 1M-lead scaling tests, single-round-trip CTE benchmark, back-to-back $W=8$ ablation (A, A_RELAY, AB_NOOP, AB), and cgroup/ping telemetry were executed using `loadgen` v2 (which directly scrapes `/sys/fs/cgroup/` usage/throttling and executes ping benchmarks).

---

### Database Table & Index DDL (`\d leads`)

The exact schema for `leads` across benchmark modes:

```text
                                      Table "public.leads"
     Column      |           Type           | Collation | Nullable |           Default           
-----------------+--------------------------+-----------+----------+-----------------------------
 id              | character varying(64)    |           | not null | 
 campaign_id     | character varying(64)    |           | not null | 
 phone_number    | character varying(32)    |           | not null | 
 name            | character varying(255)   |           |          | 
 priority        | integer                  |           | not null | 0
 status          | character varying(32)    |           | not null | 'QUEUED'::character varying
 attempts        | integer                  |           | not null | 0
 max_attempts    | integer                  |           | not null | 3
 failure_reason  | character varying(255)   |           |          | 
 locked_by       | character varying(64)    |           |          | 
 locked_at       | timestamp with time zone |           |          | 
 lease_token     | character varying(64)    |           |          | 
 last_attempt_at | timestamp with time zone |           |          | 
 next_attempt_at | timestamp with time zone |           |          | 
 created_at      | timestamp with time zone |           | not null | now()
```

#### Exact Index DDL:
- **Mode A (Migration 001 Default)**:
  ```sql
  -- Base tables only use idx_leads_dispatch
  CREATE INDEX idx_leads_dispatch ON leads (campaign_id, status, priority DESC, next_attempt_at);
  ALTER TABLE leads ADD CONSTRAINT leads_pkey PRIMARY KEY (id);
  ALTER TABLE leads ADD CONSTRAINT uq_campaign_phone UNIQUE (campaign_id, phone_number);
  ```
- **Mode B (Composite Index)**:
  ```sql
  -- Adds composite index matching claim ORDER BY
  CREATE INDEX idx_leads_comp ON leads (campaign_id, priority DESC, created_at ASC);
  ```
- **Partial Index Mode**:
  ```sql
  -- Partial index restricted strictly to active uncalled leads
  CREATE INDEX idx_leads_partial_queued ON leads (campaign_id, priority DESC, created_at ASC) WHERE status = 'QUEUED';
  ```

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
* **Why the Partial Index Failed on Unified Query**: The partial index `WHERE status = 'QUEUED'` was **completely ignored** by PostgreSQL because the unified query contains an `OR (status = 'DIALING' ...)` branch. PostgreSQL cannot use a partial index covering only one branch of an `OR` unless it combines index scans using `BitmapOr`, which is fundamentally incompatible with `FOR UPDATE SKIP LOCKED` (bitmap scans discard row pointers required for locking).
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

### 3. 1,000,000-Lead Dataset Experiment & Scale Comparison

To evaluate behavior at massive scale, a table of **1,000,000 leads** was evaluated:
* **Seeding Performance**: 1,000,000 rows inserted in **317.77s** via PostgreSQL COPY protocol (**3,147 leads/sec**).
* **Partly Drained Table Setup**: Drained **20,769 leads** into `COMPLETED` status (leaving 979,200 `QUEUED` rows).

#### `EXPLAIN (ANALYZE, BUFFERS)` on Partly Drained 1M-Lead Table:

1. **Unified Query with Default Index (`idx_leads_dispatch`)**:
   ```text
   Limit (cost=9712.31..9717.81 rows=50) (actual time=9144.849..9154.081 rows=50)
     Buffers: shared hit=1008 read=14448 dirtied=10 written=4786, temp read=365 written=586
     -> Incremental Sort (cost=9712.31..109611.97 rows=999400)
          Sort Method: external merge  Average Disk: 4672kB  Peak Disk: 4672kB
          -> Index Scan using idx_leads_dispatch on leads (cost=0.42..63919.93 rows=999400)
   Planning Time: 19.729 ms | Execution Time: 9168.650 ms (~9.17 seconds!)
   ```
   *Insight*: Because `idx_leads_dispatch` does not presort by `created_at ASC`, PostgreSQL performs an **external disk merge sort spilling 4.67 MB to temp files**, generating heavy disk I/O (`IO:BufFileWrite`) and reading 14,448 buffer pages (~115 MB).

2. **Split Fast Path with Composite Index (`idx_leads_comp`)**:
   ```text
   Limit (cost=0.42..4.30 rows=50) (actual time=25.489..25.951 rows=50)
     Buffers: shared hit=563 read=5
     -> LockRows (actual time=25.408..25.803 rows=50)
          -> Index Scan using idx_leads_comp on leads (cost=0.42..66016.11 rows=979133)
               Filter: (((status)::text = 'QUEUED'::text) AND ...)
               Rows Removed by Filter: 2130
   Planning Time: 13.405 ms | Execution Time: 27.620 ms
   ```
   *Insight*: The composite index avoids the disk spill, but because `status` is not in the index, the scan must traverse past completed rows at the head of the index: **2,130 rows removed by filter** and 563 buffer hits.

3. **Split Fast Path with Partial Index (`idx_leads_partial_queued`)**:
   ```text
   Limit (cost=0.42..4.19 rows=50) (actual time=3.072..6.264 rows=50)
     Buffers: shared hit=110 read=3
     -> LockRows (actual time=2.912..5.978 rows=50)
          -> Index Scan using idx_leads_partial_queued on leads (cost=0.42..65332.41 rows=999167)
               Filter: (((status)::text = 'QUEUED'::text) AND ...)
               Rows Removed by Filter: 0 (Zero Dead Rows Checked!)
   Planning Time: 29.788 ms | Execution Time: 8.397 ms
   ```
   *Insight*: The partial index contains **only** `status = 'QUEUED'` rows. As leads are marked `DIALING` or `COMPLETED`, they are removed from the index. Buffer hits drop from 563 down to **110** (5x reduction), zero rows are filtered, and execution completes in **8.40 ms**.

#### Why Split Mode with Composite Index is Slower than Unified at $W \ge 16$:
- In Mode B (`idx_leads_comp`), all 16 to 32 concurrent workers scan the exact same B-tree index in identical order `(campaign_id, priority DESC, created_at ASC)`.
- Because `idx_leads_comp` is an unconstrained composite index, completed leads remain in the index.
- At $W=32$, workers at the tail of the concurrent wave scan through thousands of non-QUEUED rows at the head of the index, fetching table heap pages for each row to evaluate `status = 'QUEUED'`.
- This creates intense buffer page contention on the head index leaf pages and heap blocks, causing claim latency to spike from **2.6 ms at $W=8$** up to **58.1–84.6 ms at $W=32$** (reducing throughput from 45.8 down to 36.6 leads/s).
- **The Remedy**: A partial index `WHERE status = 'QUEUED'` completely resolves this contention by pruning rows from the B-tree as soon as they are claimed.

---

### 4. Cost Per Lead Accounting & Latency Decomposition

A claiming rate of ~100 leads/s on 2.0 vCPUs represents **20 ms of container CPU per lead**. We broke down the wall-time and CPU budget using direct ping benchmarking, cgroup telemetry, and a single-round-trip CTE prototype:

#### 1. Baseline Network Round-Trip Benchmarks (1,000 samples `SELECT 1`):
* **Host to Docker across WSL2/gvproxy**:
  * `mean = 7.08 ms`, `p50 = 4.70 ms`, `p90 = 13.15 ms`, `p95 = 19.34 ms`, `p99 = 33.81 ms`
* **Inside Docker Bridge Network (Linux)**:
  * `mean = 1.93 ms`, `p50 = 1.17 ms`, `p90 = 4.24 ms`, `p95 = 6.78 ms`, `p99 = 12.75 ms`
  * *Finding*: Running the load generator outside Docker across the Windows WSL2 hypervisor boundary adds **3.5–4.7 ms of socket round-trip latency** to every individual SQL statement.

#### 2. Wall Time Budget Decomposition per Lead Cycle ($W=1$, p50 = 54.6 ms):
A single lead lifecycle in standard mode executes **7–9 sequential network round-trips** over TCP:
1. `repo.ClaimPendingLeads`: `BEGIN` + `UPDATE leads ... FOR UPDATE SKIP LOCKED` + `COMMIT` (3 round-trips) $\approx 14.1\text{ ms}$
2. `repo.RecordAttemptStart`: `SELECT ... FOR UPDATE` + `INSERT INTO call_attempts` + `UPDATE leads` (3 round-trips) $\approx 14.1\text{ ms}$
3. `repo.RecordCallOutcomeTx`: `BEGIN` + `SELECT ... FOR UPDATE` + `UPDATE leads` + `INSERT INTO outbox_events` + `COMMIT` (3 round-trips) $\approx 14.1\text{ ms}$
- **Total Network Flight Time**: $9 \times 4.70\text{ ms (p50 ping)} = \mathbf{42.30\text{ ms}}$
- **Statement Execution Time (`pg_stat_statements`)**: **10.07 ms**
- **Query Planning & Parsing**: **1.52 ms**
- **WAL Flush / Buffer Management**: **~2.80 ms**
- **Sum of Parts**: $42.30 + 10.07 + 1.52 \approx \mathbf{53.89\text{ ms}}$, matching the observed total cycle p50 (**54.6 ms**) within 1.3%.

#### 3. Single-Round-Trip CTE Prototype (`-one-round-trip=true`):
To prove that network round trips dominate caller latency, we implemented a single-round-trip CTE transaction collapsing attempt recording, lead disposition update, and outbox event publishing into 1 network round-trip:
* **Standard Multi-Round-Trip Path**: Record p50 = **44.0 ms**, Total p50 = **46.8 ms**, Throughput = **17.8 leads/s**
* **Single-Round-Trip CTE Path**: Record p50 = **14.5 ms**, Total p50 = **15.8 ms**, Throughput = **35.3 leads/s** (Nearly **2x throughput** at $W=1$).
* *pg_stat_statements*: CTE execution time inside Postgres was **2.64 ms**. Subtracting 2.64 ms from 14.5 ms leaves 11.8 ms, which exactly reflects 1 WSL2 network ping (~4.7 ms) + commit WAL sync (~3 ms) + pool acquire.

#### 4. Container Cgroup CPU & Throttling:
* Directly scraped from `/sys/fs/cgroup/cpuacct/cpuacct.usage` and `/sys/fs/cgroup/cpu/cpu.stat`:
  * `nr_throttled = 0`
  * `throttled_time = 0`
  * *Conclusion*: At no point was the PostgreSQL container throttled by the Linux CFS scheduler. Bottlenecks were network socket serialization at low concurrency and database CPU core saturation (180%–208%) at peak concurrency.

---

### 5. Scenario AB Cost Breakdown: Back-to-Back Ablation ($W=8$, 3 Runs Each)

Executed back-to-back at $W=8$ (3 runs each, 2,000 leads per run). Values reported as **`[min - max]` ranges across 3 runs**:

| Configuration ($W=8$) | Max Claim-and-Record Rate (leads/s) | Total Cycle p50 (ms) | Claim p50 (ms) | Record p50 (ms) | Cgroup CPU% (2 CPU Cap) | Dead Tuples (`n_dead_tup`) | Autovacuum Runs | Primary Bottleneck |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **A Alone** (Workers Only) | [37.4 – 41.7] | [153.8 – 171.7] | [2.5 – 3.1] | [143.0 – 162.1] | [91.7% – 109.8%] | 0 on outbox | 0 on outbox | Baseline worker claim & record |
| **A + Relay** (`A_RELAY`) | [31.1 – 32.6] | [205.9 – 217.1] | [3.2 – 4.4] | [196.3 – 207.4] | [107.3% – 108.3%] | 647–857 on `outbox_events` | 4–7 on `outbox_events` | Relay `UPDATE outbox_events SET published_at` page churn |
| **A + Relay + NoOp Consumer** (`AB_NOOP`) | [29.6 – 41.5] | [172.0 – 221.3] | [3.1 – 4.1] | [165.1 – 209.1] | [145.2% – 187.3%] | 0 on `campaign_outcomes` | 0 on `campaign_outcomes` | NATS pull consumption + `processed_events` INSERTs |
| **Full AB** (Workers + Relay + Consumer) | [29.2 – 32.1] | [222.6 – 238.6] | [4.3 – 7.1] | [211.7 – 231.3] | [156.2% – 171.9%] | 25–71 on `campaign_outcomes` | 1–5 on `campaign_outcomes` | Hot-row contention on single `campaign_outcomes` row |

#### Component Overhead Insights:
1. **Relay Overhead (`A_RELAY`)**: Draining unpublished events introduces `UPDATE outbox_events SET published_at = clock_timestamp()`. Each update dirties a page, creating dead tuples and triggering 4–7 autovacuum passes, consuming ~15% database CPU and lowering throughput from ~40 to ~32 leads/s.
2. **Consumer Overhead (`AB_NOOP`)**: Pulling batches from NATS and recording idempotency keys (`INSERT INTO processed_events`) increases CPU utilization from ~108% to ~160%.
3. **Hot-Row Contention (`AB`)**: In full `AB`, the consumer aggregates outcome statistics into `campaign_outcomes` (`UPDATE campaign_outcomes SET answered = answered + 1 WHERE campaign_id = $1`). Because all 2,000 events target the exact same row for `camp-load-1`, transactions serialize on row locks, producing 25–71 dead row versions on a 1-row table and triggering 1–5 autovacuums.

---

### 6. Lock Waits & Telemetry Sampling

* **Exact Wait Events Recorded (`pg_stat_activity`)**:
  * `IO:WALSync`: Flushing transaction commit logs to disk.
  * `IO:BufFileWrite`: External merge sort disk spilling during unindexed 1M-lead queries.
  * `Client:ClientRead`: Waiting for data transmission over the Go client TCP socket.
  * `Lock:tuple` / `Lock:transactionid`: Transient row-level update waits when workers or consumers target active lead or outcome rows.
  * *Finding*: Buffer pin waits and B-tree leaf lock contention were strictly confined to unindexed scans or high-concurrency composite index scans with non-QUEUED rows.
* **Telemetry Sampling Integrity**:
  * `docker stats`: Tracked with explicit counter (`dockerStatsAttempted` vs `dockerStatsTaken`). With the 800 ms context timeout, 100% of samples (50/50 per step) completed without stalling Windows named pipes.
  * Direct container cgroups scraping from `/sys/fs/cgroup/cpuacct/cpuacct.usage` and `/sys/fs/cgroup/cpu/cpu.stat` eliminated CLI process spawn jitter entirely.

---

### 7. Knee Diagnosis & Architectural Takeaway

* **Caller Latency vs Database Knee**:
  * To an external caller, the latency knee appears at **$W=1$ to $W=2$** (p50 rises from 43 ms to 70 ms) due to network socket serialization and sequential statement round-trips.
  * To PostgreSQL, the capacity knee occurs at **$W=8$**: throughput flattens at 40–45 leads/s, while database CPU utilization reaches 180%–208% of the 2.0 vCPU limit.
* **Architectural Remedy**:
  1. Use the **Partial Index** `WHERE status = 'QUEUED'` to keep B-tree leaf traversal clean and eliminate head-of-index worker contention.
  2. Implement **single-round-trip batching / CTE transactions** to eliminate the 9 TCP network round trips per lead cycle.

---

### 8. Technical Interview Deep-Dive Q&As

#### Q1: How is idempotency guaranteed when consumer events arrive more than once from NATS JetStream?
**Answer**:
Idempotency is enforced at the database transaction layer using PostgreSQL unique constraints and atomic conflict resolution:
```sql
INSERT INTO processed_events (event_id, consumer_name, processed_at)
VALUES ($1, $2, clock_timestamp())
ON CONFLICT (event_id, consumer_name) DO NOTHING;
```
When an outbox event is pulled from JetStream, the consumer starts a transaction, attempts to insert the idempotency record with `ON CONFLICT (event_id, consumer_name) DO NOTHING`, and inspects `cmdTag.RowsAffected()`. If 0 rows were inserted, the event was already processed by a previous delivery; the consumer immediately acks the message and commits the empty transaction without re-executing domain state updates.

#### Q2: What happens if `FOR UPDATE SKIP LOCKED` is executed inside a subquery, and how does lease recovery work?
**Answer**:
In PostgreSQL, `FOR UPDATE SKIP LOCKED` inside a subquery locks all matching candidate rows returned by the subquery for the duration of the transaction. If outer `WHERE` clauses or subsequent joins filter out some of those rows, those rows **remain locked** until transaction commit or rollback, preventing other concurrent workers from claiming them.

In DialPulse, the unified query combines uncalled leads and expired leases in an `OR` clause. Splitting this into two queries is an architectural design pattern prototyped in `cmd/loadgen`:
1. **Queued Fast Path**: Claims uncalled leads with a lean index scan (`status = 'QUEUED'`).
2. **Expired Lease Recovery Path**: A background sweeper running periodically or as a fallback path targeting `status = 'DIALING' AND locked_at < clock_timestamp() - interval`.
*Note*: The production implementation in `internal/storage/postgres.go` currently executes the single unified query; separating the expired lease path is a benchmarked design improvement.

#### Q3: Why is a lease token used in distributed claiming, how is it verified, and how does the Dead-Letter Queue (DLQ) work?
**Answer**:
1. **Lease Token Purpose & Optimistic Locking**:
   The `lease_token` is a cryptographically unique UUID (`gen_random_uuid()::text`) generated anew on every claim. It acts as an epoch identifier. When recording an outcome, optimistic concurrency is enforced by equality compare at the row level:
   ```sql
   UPDATE leads SET status = $2, failure_reason = $3, lease_token = NULL
   WHERE id = $1 AND lease_token = $2;
   ```
   If a worker experiences a long network pause or GC freeze exceeding the lease TTL (`DefaultLeaseTTL = 30s`), another worker reclaims the lead and issues a new `lease_token`. When the slow worker finally attempts to commit, `RowsAffected() == 0`, and the repository returns `ErrStaleLease`, preventing split-brain overwrites.
2. **Database Clock Reliance**:
   Lease expiration is strictly evaluated against PostgreSQL's transaction clock: `locked_at < clock_timestamp() - interval`. This eliminates dependency on synchronized server wall clocks across distributed worker nodes.
3. **Dead-Letter Queue (DLQ) Headers**:
   When an event fails message processing repeatedly or contains corrupted payloads, the consumer routes the message to the DLQ stream (`dialpulse.dlq.>`) with headers matching the exact Go constants in `internal/queue/outbox_consumer.go`:
   - For poison messages exceeding retries (`meta.NumDelivered > MaxDeliveries`):
     - `Nats-Msg-Id`: `dlq_<event.ID>`
     - `x-original-event-id`: `<event.ID>`
     - `x-dlq-error`: `<error message>`
     - `x-dead-letter-reason`: `"max_deliveries_exceeded"`
     - `x-delivery-count`: `"<count>"`
   - For malformed messages without valid IDs:
     - `Nats-Msg-Id`: `<new-uuid>`
     - `x-stream-sequence`: `"<seq>"`
     - `x-dead-letter-reason`: `"malformed_missing_id"`
     - `x-dlq-reason`: `"missing_nats_msg_id"`

---

### 9. Honest Limitations

1. **Co-located Architecture**: The Go load generator, PostgreSQL container, and NATS broker were executed on the same physical host. CPU scheduling, Docker WSL2 virtualization boundaries, and context switching introduce variance not present in dedicated multi-node cloud environments.
2. **Docker-on-Windows Hypervisor Overhead**: Networking traverses the WSL2 virtual Ethernet adapter and gvproxy translation layer, adding approximately 3.5–4.7 ms of baseline socket latency to every PostgreSQL transaction round-trip (verified via 1,000-sample `SELECT 1` ping benchmark).
3. **Telemetry Granularity**: Docker container CPU was sampled at 1.0s intervals with an 800 ms timeout to prevent named pipe deadlocks, smoothing out micro-bursts of sub-millisecond CPU spikes.
4. **No Carrier Telephony Latency**: This benchmark measures database claim-and-record limits only; actual carrier dialing involves 3–15 seconds of PSTN trunk ring time per attempt.

---

### 10. Reproduction Commands

```bash
# 1. Start isolated PostgreSQL with resource caps and NATS JetStream
docker run -d --name dialpulse-postgres -p 5432:5432 --cpus=2.0 --memory=2g \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=dialpulse_test \
  postgres:16-alpine -c max_connections=100 -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=all

docker run -d --name dialpulse-nats -p 4222:4222 -p 8222:8222 nats:2-alpine -js -m 8222

# 2. Apply migrations
Get-Content migrations/001_init.sql | docker exec -i dialpulse-postgres psql -U postgres -d dialpulse_test
Get-Content migrations/002_outbox.sql | docker exec -i dialpulse-postgres psql -U postgres -d dialpulse_test

# 3. Run SELECT 1 Ping Round-Trip Benchmark
go run ./cmd/loadgen -ping-bench=true

# 4. Run One-Round-Trip CTE Prototype (W=1, 1000 leads)
go run ./cmd/loadgen -scenario=A -workers=1 -leads=1000 -runs=1 -one-round-trip=true

# 5. Run 1M Leads Index Comparison & Partly Drained EXPLAIN
go run ./cmd/loadgen -dataset-experiment=true

# 6. Run Back-to-Back Ablation at W=8 across A, A_RELAY, AB_NOOP, AB
go run ./cmd/loadgen -scenario=A -workers=8 -leads=2000 -runs=3
go run ./cmd/loadgen -scenario=A_RELAY -workers=8 -leads=2000 -runs=3
go run ./cmd/loadgen -scenario=AB_NOOP -workers=8 -leads=2000 -runs=3
go run ./cmd/loadgen -scenario=AB -workers=8 -leads=2000 -runs=3
```

---

## Author

- **Shantanu Juvekar** ([@yoohooshantanu](https://github.com/yoohooshantanu))
- Email: `santanujuvekar@gmail.com`
- License: MIT
