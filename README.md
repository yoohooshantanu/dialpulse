# PredictiveDialer

[![CI](https://github.com/yoohooshantanu/dialpulse/actions/workflows/ci.yml/badge.svg)](https://github.com/yoohooshantanu/dialpulse/actions)
![Go](https://img.shields.io/badge/Go-1.23%2B-00ADD8)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)
![NATS JetStream](https://img.shields.io/badge/NATS-JetStream-27AAE1)

A predictive dialer for outbound calling campaigns, written in Go. It decides how many numbers to call, which leads pass basic curfew and Do-Not-Call checks right now, which worker owns which lead, and what happens when something crashes halfway through. Postgres is the source of truth, NATS JetStream carries the events, and the phone network is a mock.

*(The GitHub repo is called dialpulse. The Go module and binary are named predictive-dialer from an architectural rename.)*

I wanted to build the boring-sounding, hard-in-practice part of a call center: the part where a few hundred things happen at once and none of them are allowed to happen twice.

---

## Contents

- [The idea in two minutes](#the-idea-in-two-minutes)
- [What is real and what is mocked](#what-is-real-and-what-is-mocked)
- [Life of a lead](#life-of-a-lead)
- [The pieces, one by one](#the-pieces-one-by-one)
- [Run it](#run-it)
- [API](#api)
- [Data model](#data-model)
- [Observability](#observability)
- [Tests](#tests)
- [The load test, and what it taught me](#the-load-test-and-what-it-taught-me)
- [Limitations](#limitations)
- [Repo map](#repo-map)

---

## The idea in two minutes

Picture a call center with 10 agents. Most calls to strangers go nowhere: voicemail, busy, nobody picks up. If you dial one number per free agent, your agents spend most of their day listening to ringing.

A predictive dialer fixes that by dialing more numbers than there are free agents, betting that most of them will not answer. Say about 20% of calls get picked up. Then with 10 idle agents you dial roughly 50 numbers and expect about 10 hellos.

The catch is the bad day. Sometimes 14 people answer and you have 10 agents. Four people say "hello?" into silence and get hung up on. Those are abandoned calls, and US telemarketing rules cap them at 3% of answered calls. So the dialer has to be aggressive enough to keep agents busy and careful enough to stay under the cap. That balancing act is the pacer, and it is the fun part.

Everything else in this repo exists because the pacer's decisions have to survive the real world:
- Do not call people on the Do-Not-Call list.
- Do not call anyone before 8am or after 9pm their time.
- Do not call the same lead twice because two servers raced.
- If a worker freezes for a minute and comes back, it must not overwrite the work of the worker that replaced it.
- If the database commits but the message broker is down, the event must not vanish.
- If the broker delivers the same event twice, the books must not count it twice.

Each of those has its own section below, with the code that does it and the test that tries to break it.

---

## What is real and what is mocked

| Real | Mocked or simplified |
| :--- | :--- |
| The pacer math, in-process | The carrier. `MockTelephonyDialer` sleeps 10 to 25 ms, then rolls dice: 20% answer, 15% voicemail, 5% busy, rest no answer. There is no SIP, no audio, no real phone call. |
| Postgres schema, claiming, leases, fencing, transactions | The AI agents. An "agent" is a counter of free slots. A connected call just holds a slot for 30 to 60 ms. |
| Transactional outbox, relay, NATS JetStream publish | Time zones. The governor knows about 37 US area codes, not the whole numbering plan. Unknown area codes fail closed. |
| Idempotent consumer, poison-message handling, dead-letter queue | Per-state calling rules. It enforces the federal 8am to 9pm window only. |
| 53 tests that run against live Postgres and NATS in CI | The outbox consumer is not started by the server binary. The server starts the relay. The consumer runs in the tests and in the load generator. |

If you are skimming for the honest summary: this is a correctness-and-concurrency project, not a telephony project.

---

## Life of a lead

```mermaid
flowchart TD
    A["POST leads: normalize phone, dedupe"] --> B["Postgres: status QUEUED"]
    B --> C["50 ms tick: pacer says how many to dial"]
    C --> D["Subtract calls already ringing"]
    D --> E["Claim leads: FOR UPDATE SKIP LOCKED, new lease token"]
    E --> F{"In DNC filter or registry?"}
    F -- yes --> X["Dead-letter queue: DNC_PROHIBITED"]
    F -- no --> G{"Inside 8am to 9pm local time?"}
    G -- unknown area code --> Y["Dead-letter queue: unknown_timezone"]
    G -- too early or late --> H["Reschedule to next 8am local"]
    G -- ok --> I["Write attempt start to DB, then dial"]
    I --> J{"Carrier result"}
    J -- busy or no answer --> K["Retry later with backoff, or DLQ when attempts run out"]
    J -- voicemail --> L["Terminal: VOICEMAIL"]
    J -- answered --> M{"Free agent slot?"}
    M -- yes --> N["CONNECTED"]
    M -- no --> O["ABANDONED, pacer backs off"]
    N --> P["One transaction: fenced lead update, call attempt, outbox event"]
    O --> P
    K --> P
    L --> P
    P --> Q["Relay publishes to NATS JetStream"]
    Q --> R["Consumer: dedupe on event id, update aggregates, DLQ poison"]
```

In words:
1. **Ingest**: A batch of leads comes in. Phone numbers are stripped to digits (a leading US 1 on an 11-digit number is dropped), checked against an in-memory index, then inserted with `ON CONFLICT DO NOTHING` against a unique `(campaign_id, phone_number)` constraint. The response tells you how many were accepted and how many were duplicates. Ingestion is synchronous: if storage fails, the request fails.
2. **Tick**: Every 50 ms the campaign engine asks the pacer how many calls to place, subtracts the ones still ringing, and claims that many leads from Postgres.
3. **Gate**: Each claimed lead goes through the compliance checks before anything dials.
4. **Dial**: The attempt is written to the database before the carrier is called. If that write fails, the dial is aborted.
5. **Record**: The outcome, the call attempt row, and an outbox event are committed in one transaction.
6. **Publish**: A relay moves outbox rows to NATS. A consumer applies them downstream, once, even if the broker delivers them more than once.

---

## The pieces, one by one

### The pacer (`internal/pacer`)

Each tick the pacer computes how many leads to dial:

```go
freeing_soon = active_calls * (avg_ring_sec / avg_handle_sec)
capacity     = idle_agents + freeing_soon
dials        = capacity / answer_rate
```

`freeing_soon` is the Little's-law-flavored part: calls already in progress will finish during the time a new call spends ringing, so those agents are about to become free and can be counted early.

On top of that:
- **Answer rate** starts at the campaign's historical rate. After 50 dials it blends to 80% observed and 20% prior, with a floor of 5% so a bad streak cannot make the math explode.
- **Abandon brake**: Abandon rate is abandoned over answered. At 90% of the target (default 2.5%) the multiplier is scaled down on a ramp, never below 20% of itself. At or past the target the pacer drops to progressive mode: one dial per idle agent, no gambling.
- **Ceiling**: Dials are capped at `agents * max_dial_multiplier` (default 3.5).
- **In-flight reservation**: The engine keeps a counter of calls currently ringing and subtracts it from the pacer's number. Without this, two consecutive 50 ms ticks would both see the same idle agents and double-book them (`TestInFlightDialLimiting` covers it).
- **One outcome per attempt**: The pacer records exactly one disposition per call, so its feedback loop cannot count the same call twice (`TestPacerSingleOutcomeAccounting`).

To be precise about naming: this is a Little's-law-style heuristic, not a full Erlang-C model.

### The compliance gate

> [!NOTE]
> **Scope & regulatory boundaries**: This gate enforces an internal Do-Not-Call registry and federal 8am–9pm curfew windows. It demonstrates fail-closed filtering and time zone routing, not full telemarketing regulatory compliance (state-specific mini-TCPA curfews like Florida/Oklahoma 8pm cutoffs, prior-express-written-consent tracking, and mobile vs. landline carrier scrubs are outside the scope of this project).

**Do-Not-Call (`internal/dnc`)**: A Bloom filter in a bitset, sized for 1,000,000 numbers at a 0.1% false-positive rate (about 1.8 MB and 10 hash functions at the server's defaults). Hashing is FNV-1a with Kirsch-Mitzenmacher double hashing, so one pair of hashes simulates all ten. A false positive means a clean number gets skipped, which is the safe direction. There are no false negatives.

The filter is a fast path, not the authority. A number the filter has never seen is still checked against the `dnc_numbers` table in Postgres, so numbers added through another instance are caught too. If that database check errors out, the lead is not dialed: it is pushed back 1 minute without burning an attempt (fail closed). On startup the registry is loaded into the filter, and if that load fails the server refuses to start.

**Time zones (`internal/timezone`)**: Federal rules say no telemarketing calls before 8am or after 9pm at the recipient's local time. The governor maps area code to time zone and checks the window. A lead outside the window goes back to the queue with `next_attempt_at` set to the next 8am local time. Nothing spins waiting for morning (`TestCurfewHoldoffNoBusySpinAndCleanShutdown`).

The edge cases are where it gets fun:
- **Arizona (602, 480, 623)** maps to `America/Phoenix`, which ignores daylight saving. In summer, 14:30 UTC is 8:30am in Denver but 7:30am in Phoenix. Treating Arizona like Mountain Time would put calls out an hour early.
- **Area code 928** covers both the Navajo Nation, which observes daylight saving, and the rest of northern Arizona, which does not. The governor requires both zones to be inside the window, so it never calls someone at 8:30pm local on a technicality.
- **915 (El Paso)** is Mountain Time, unlike most of Texas.
- **Unknown area codes** fail closed: straight to the dead-letter queue with reason `unknown_timezone`, no guessing and no infinite retry.

Area codes are a heuristic. Number portability means people move and keep their number, and the governor does not know about that.

### Claiming work from Postgres, safely

Any number of server instances can point at the same database and each one claims its own batch:

```sql
UPDATE leads
SET status = 'DIALING',
    locked_by = $1,
    locked_at = clock_timestamp(),
    lease_token = gen_random_uuid()::text
WHERE id IN (
    SELECT id FROM leads
    WHERE campaign_id = $2
      AND (
        ((status = 'QUEUED' OR status = 'DRAFT') AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
        OR (status = 'DIALING' AND locked_at < clock_timestamp() - $4::interval)
      )
    ORDER BY priority DESC, created_at ASC
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING ...;
```

`FOR UPDATE SKIP LOCKED` means a worker never waits on a row someone else holds. It skips ahead. No queue of blocked workers, and a lead cannot be handed to two of them.

The `OR` branch also reclaims expired leases: a lead stuck in `DIALING` past the lease TTL (2 minutes) is up for grabs again. That is how a crashed worker's leads come back.

Every claim stamps a fresh random lease token.

### Fencing: the zombie worker problem

Worker A claims a lead and then freezes (GC pause, network stall, laptop lid). Its lease expires. Worker B reclaims the lead with a new token and starts work. Then A wakes up, convinced it still owns the lead.

Every write that follows a claim re-checks ownership under a row lock: the status must still be `DIALING`, the token must match, and the lease must still be valid by the database's clock (`clock_timestamp()`), so a worker's own clock skew cannot cheat. A mismatch updates zero rows and surfaces as `ErrStaleLease`. A stale worker abandons the lead and does not touch the row, because it now belongs to someone else.

The token is compared for equality at the row. It is not a counter and it does not rely on ordering. The stale-worker behavior has its own tests against live Postgres (`TestLivePostgresDistributedLeaseFencingStaleWorkerRejected`, `TestLivePostgresStaleWorkerOutcomeRejection`, `TestQueueLevelStaleWorkerCleanupDoesNotMutateReclaimedLease`, and a few more).

Leases are 2 minutes and the carrier dial timeout is 10 seconds, so a normal dial finishes well inside its lease. A real call that streams audio for longer would need a heartbeat that refreshes `locked_at`. That is noted in the code and not built.

### Recording an outcome: one transaction

`RecordCallOutcomeTx` does all of this in a single Postgres transaction, with up to 3 retries:
1. `SELECT ... FOR UPDATE` the lead, verify status, token and lease.
2. Update the lead's status and failure reason.
3. Insert or update the `call_attempts` row.
4. Insert an `outbox_events` row with a deterministic id, `evt_<attemptID>_<eventType>`.
5. Commit.

Either the outcome and its event both exist, or neither does. That is the whole point of the next section.

### Retries and the dead-letter queue

- **Busy or no answer**: retry if attempts remain (default max 3). The delay starts at 15 minutes and doubles each attempt, with up to 10% jitter.
- **Voicemail**: terminal in this version.
- **Out of attempts**: dead-letter queue, reason `EXHAUSTED_MAX_ATTEMPTS`.
- **Carrier error**: dead-letter queue, reason `CARRIER_TRUNK_ERROR: ...`.
- **DNC hit**: dead-letter queue, reason `DNC_PROHIBITED`.
- **Unknown area code**: dead-letter queue, reason `unknown_timezone`.

`GET /v1/campaigns/{id}/dlq` lists everything in there and why.

### The outbox, the relay, and the consumer

The classic dual-write problem: commit to the database, then publish to the broker. If the process dies between the two, the event is lost. Publish first and commit second, and you may announce something that never happened. There is no atomic transaction across Postgres and NATS, so PredictiveDialer does not try to fake one.

```mermaid
sequenceDiagram
    participant W as Dialer worker
    participant PG as Postgres
    participant R as Outbox relay
    participant N as NATS JetStream
    participant C as Consumer
    W->>PG: BEGIN, update lead, insert attempt, insert outbox row, COMMIT
    loop every 50 ms
        R->>PG: BEGIN, SELECT unpublished FOR UPDATE SKIP LOCKED (batch of 10)
        R->>N: publish each, Nats-Msg-Id = event id
        N-->>R: ack
        R->>PG: UPDATE published_at, COMMIT
    end
    N->>C: deliver
    C->>PG: BEGIN, INSERT processed_events ON CONFLICT DO NOTHING, update campaign_outcomes, COMMIT
    C-->>N: ack
```

**Relay**: Several relays can run at once because each claims its batch with `SKIP LOCKED`. If any publish fails or times out, the transaction rolls back and the rows stay unpublished, so the next cycle retries them. Delivery is at least once: a crash after publishing but before marking the rows means a repeat later. Events across leads are not globally ordered, and consumers must not assume they are.

**Why the broker's dedup is not enough**: JetStream dedups on `Nats-Msg-Id`, but only inside its `duplicate_window` (a couple of minutes by default). Re-publish after that and it is treated as new. `TestDedupWindowExpiryStreamAcceptsDuplicateConsumerIdempotentEffect` forces exactly this.

**Consumer**: Business effects and the idempotency record happen in one Postgres transaction: insert `(event_id, consumer_name)` into `processed_events` with `ON CONFLICT DO NOTHING`, and only if a row was actually inserted, update the per-campaign counters. A duplicate event inserts nothing, so it applies no effect and is acked straight away. "At least once" delivery plus an idempotent consumer gives you exactly-once effects, which is the honest version of exactly-once.

**Poison messages**: The durable pull consumer is configured with `MaxDeliver: -1`, so the broker never gives up on its own. The consumer counts deliveries itself (`NumDelivered`) and enforces the limit (3 by default) in application code. Why? If the broker enforced it and the DLQ publish happened to fail on the final attempt, the message would just vanish. With the limit in code, a failed DLQ publish means a `NakWithDelay` and another try, and `TestDLQPublishFailsOnThirdDeliveryRedeliveredAndLandsInDLQ` proves the message still lands.

Poison messages go to `predictive_dialer.dlq.<aggregate_type>` with headers `x-dead-letter-reason: max_deliveries_exceeded`, `x-original-event-id`, `x-delivery-count` and `x-dlq-error`. A message with no `Nats-Msg-Id` is malformed: it goes to the DLQ at once with `x-dead-letter-reason: malformed_missing_id` and is acked, since retrying garbage helps no one.

One known gap: if the consumer crashes after the DLQ publish and before the ack, you get a duplicate DLQ entry. Treat the DLQ as at-least-once too.

**Lag**: `predictive_dialer_outbox_lag_seconds` is computed at scrape time as the age of the oldest unpublished row. It does not depend on the relay's own loop, so if the relay stalls the number keeps climbing instead of freezing at zero.

**Housekeeping**: `PurgePublishedOutboxEvents` trims old published rows. The `processed_events` ledger grows forever unless you prune it, and the retention window must be longer than any redelivery horizon. That job is not scheduled for you.

---

## Run it

You need Docker. Go 1.23+ if you want to run tests or build locally.

```bash
git clone https://github.com/yoohooshantanu/dialpulse
cd dialpulse
docker compose up -d
```

That starts the engine on `:8086`, Postgres 16 (migrations run on first boot), NATS with JetStream on `:4222`, and Prometheus on `:9091`.

Create a campaign, add leads, start dialing:

```bash
# 1. a campaign with 5 agents
curl -s -X POST localhost:8086/v1/campaigns -d '{
  "id": "demo",
  "tenant_id": "acme",
  "name": "Demo",
  "mode": "PREDICTIVE",
  "allocated_agents": 5
}'

# 2. a few leads (212 = New York, 303 = Denver, 415 = San Francisco)
curl -s -X POST localhost:8086/v1/campaigns/demo/leads -d '[
  {"phone_number": "+1 212 555 0101", "name": "Ada", "priority": 10},
  {"phone_number": "+1 303 555 0102", "name": "Grace", "priority": 5},
  {"phone_number": "+1 415 555 0103", "name": "Linus"}
]'

# 3. go
curl -s -X POST localhost:8086/v1/campaigns/demo/start

# 4. watch
curl -s localhost:8086/v1/campaigns/demo/stats
curl -s localhost:8086/v1/campaigns/demo/dlq
```

Heads up: the curfew check uses the recipient's local time. If you run this at an hour when the US numbers are outside 8am to 9pm, nothing will dial. That is the compliance gate doing its job, and `predictive_dialer_timezone_blocks` on `/metrics` will go up. Come back in the morning, Pacific time.

Block a number:

```bash
curl -s -X POST localhost:8086/v1/dnc -d '{"phone_numbers": ["+1 212 555 0101"]}'
```

Configuration is three environment variables:

| Variable | Default | Effect |
| :--- | :--- | :--- |
| `PORT` | `8086` | HTTP port |
| `DATABASE_URL` | unset | Postgres connection string. Unset means an in-memory store, and everything is lost on restart. |
| `NATS_URL` | unset | JetStream address. Unset means the outbox relay is disabled and events pile up in Postgres until you set it. |

On restart the server reloads active campaigns and their pending leads from Postgres, and reloads the DNC registry into the Bloom filter.

---

## API

| Method | Path | What it does |
| :--- | :--- | :--- |
| `POST` | `/v1/campaigns` | Create a campaign (starts as DRAFT) and persist it |
| `POST` | `/v1/campaigns/{id}/leads` | Batch ingest a JSON array of leads. Returns accepted and duplicate counts |
| `GET` | `/v1/campaigns/{id}/leads/{lead_id}` | One lead and its status |
| `POST` | `/v1/campaigns/{id}/start` | Start the dispatch loop |
| `GET` | `/v1/campaigns/{id}/stats` | Live numbers: dial multiplier, answer rate, abandon rate, idle agents |
| `GET` | `/v1/campaigns/{id}/dlq` | Dead-lettered leads with failure reasons |
| `POST` | `/v1/dnc` | Add numbers to the DNC registry (Postgres and the Bloom filter) |
| `GET` | `/healthz` | Health check |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/debug/pprof/*` | Go profiling |

Campaign fields you can set: `id`, `tenant_id`, `name`, `mode`, `allocated_agents` (default 10), `target_abandon_rate` (0.025), `max_dial_multiplier` (3.5), `average_handle_sec` (90), `average_ring_sec` (15), `historical_answer_rate` (0.20).

---

## Data model

Two migrations in `migrations/`.

| Table | Purpose |
| :--- | :--- |
| `campaigns` | Pacing config and lifecycle state |
| `leads` | The work queue. Priority, status, attempts, locked_by, locked_at, lease_token, next_attempt_at. Unique on `(campaign_id, phone_number)` |
| `call_attempts` | Append-style log of every attempt: status, ring and talk duration, error |
| `dnc_numbers` | Persistent Do-Not-Call registry |
| `outbox_events` | Events written in the same transaction as the outcome, with published_at set by the relay. Partial index on unpublished rows |
| `processed_events` | Consumer idempotency ledger, primary key `(event_id, consumer_name)` |
| `campaign_outcomes` | Per-campaign counters per status, updated by the consumer |

Lead statuses: `QUEUED`, `DIALING`, `RINGING`, `ANSWERED`, `CONNECTED`, `BUSY`, `NO_ANSWER`, `VOICEMAIL_AMD`, `ABANDONED`, `COMPLETED`, `FAILED`, `DEAD_LETTER_QUEUE`.

---

## Observability

`/metrics` exposes: `predictive_dialer_total_dials`, `predictive_dialer_total_connects`, `predictive_dialer_total_abandons`, `predictive_dialer_abandon_rate_pct`, `predictive_dialer_dnc_hits`, `predictive_dialer_timezone_blocks`, `predictive_dialer_total_leads_ingested`, `predictive_dialer_active_dialers`, `predictive_dialer_storage_errors`, and `predictive_dialer_outbox_lag_seconds`. The compose file ships a Prometheus that scrapes it every 5 seconds. Logging is structured, and pprof is on `/debug/pprof/` when you need to see where the CPU went.

---

## Tests

53 top-level test functions (72 counting table-driven subtests). CI runs all of them on every push against a real Postgres 16 and a real NATS JetStream, with the race detector on.

```bash
export TEST_DATABASE_URL="postgres://postgres:password@localhost:5432/dialpulse_test?sslmode=disable"
export TEST_NATS_URL="nats://localhost:4222"
export REQUIRE_DB=1 REQUIRE_NATS=1
go test -v -race ./tests/...
```

(Apply `migrations/001_init.sql` and `002_outbox.sql` to that database first.)

What they cover, grouped by what could go wrong:
- **Compliance**: Bloom filter behavior, the curfew table including Arizona and 928, unknown area codes, DNC persistence across restarts, DNC fails closed when the database errors.
- **Pacing**: the math, in-flight limiting, single-outcome accounting.
- **Concurrency**: competing workers never claim the same lead, competing engines, expired-lease recovery, stale workers rejected, attempt-start lock-wait expiry.
- **Durability**: idempotent ingestion across restarts, crash-safe attempt-start persistence, campaign recovery on startup, atomic outcome persistence.
- **Outbox and consumer**: events accumulate while NATS is down and drain when it returns, a crash between publish and mark is harmless, dedup-window expiry, concurrent relays never double-publish, outbox lag keeps growing when the relay stops, poison messages and failed DLQ publishes, malformed messages.

Micro-benchmarks live in `tests/benchmark_test.go`. On my AMD Ryzen 5 laptop the Bloom lookup is about 520 ns and a pacer calculation about 255 ns. Treat those as one machine's numbers.

---

## The load test, and what it taught me

`cmd/loadgen` is a separate load generator. I pointed it at a Postgres 16 container capped at 2 CPUs and 2 GB, with NATS alongside.

Everything it reports is a claim-and-record rate with no dial time: how fast leads can move through claim, attempt-start, and outcome transactions. It is not calls per second, because no phone call is simulated. Every table below comes from a log in [`docs/loadtest/raw/`](docs/loadtest/raw/), and I report ranges over 3 runs, not single numbers.

### The big finding: one query was the whole problem

The claim query above has an OR in it, and I sorted by `(priority DESC, created_at)` while the only index was on `(campaign_id, status, priority, next_attempt_at)`. On a small table nobody notices. On a table with 1,000,000 leads, Postgres cannot use the index for that sort across the OR, so it reads the whole table and sorts it with an on-disk merge:

| Claim query | Plan | Execution time |
| :--- | :--- | :--- |
| Unified query, original index | Seq scan, external merge sort spilling 46 MB to disk | 7,273.8 ms |
| Split into a QUEUED-only fast path with a partial index `WHERE status = 'QUEUED'` | Index scan, 12 buffers | 10.5 ms |

(`explain_mode_a.txt` and `explain_mode_c.txt`, on a partly drained 1M-row table.)

Whole-system effect at 1M leads, 8 workers, claim-and-record rate:

| Variant | Leads per second (3 runs) |
| :--- | :--- |
| Unified query | 3.7 to 4.1 |
| Split, composite index `(campaign_id, priority, created_at)` | 62.2 to 88.7 |
| Split, partial index on `QUEUED` | 69.6 to 109.6 |

The last two overlap at 8 workers, so the partial index is not a clear throughput win there. Where it clearly wins is claim latency under load. At 32 workers the composite index's claim latency climbs (17.7 to 38.5 ms amortized per lead) because every worker walks the same index head, past thousands of already-completed rows. The partial index only contains queued rows, so there is nothing to walk past (9.7 to 14.4 ms).

Compare numbers within a table only. Across sessions my laptop varies by tens of percent.

### What the engine does today

The fix is not in `internal/storage/postgres.go`. The shipped claim path is still the unified query, which is fine for tens of thousands of leads and falls over past a few hundred thousand. The split query and the partial index live in the load generator as a measured prototype. Shipping them as a migration needs care: the unified query also claims DRAFT leads and reclaims expired leases in one pass, and a split version has to keep both behaviors and avoid starving lease recovery when the queue is always full. I wrote that down instead of pretending it is done.

### Where the time goes per lead

Postgres was not the bottleneck. Container CPU at 8 workers stayed around 0.7 to 1.1 cores of the 2 available, and the throttling counter (`nr_throttled`) was 0. The most common wait state was `ClientRead`: Postgres sitting idle waiting for the Go program to send its next statement. The connection pool never ran dry.

Each lead costs 11 round trips between Go and Postgres (5 for attempt start, 6 for the outcome transaction, plus a share of the batch claim). On my Docker-on-Windows setup a `SELECT 1` costs about 3 ms idle and about 5.6 ms median under load, so the round trips alone account for a large share of a lead's roughly 90 ms cycle. As a sanity check I wrote a prototype that collapses the whole record path into one CTE query. In one back-to-back run at 1 worker it went from 12.9 to 72.1 leads/s. That is a single run of a prototype, so read it as "round trips dominate", not as a benchmark. I have not accounted for every millisecond of the cycle and I am not going to pretend I have.

### What I got wrong first

My first explanation for the slow cycle was that Postgres was CPU-saturated. `docker stats` on Windows showed numbers above 200% on a 2-CPU cap. Reading the container's cgroup counters directly showed about half that, with no throttling. `docker stats` was the one lying.

### Other costs I measured

At 8 workers, adding the outbox relay dropped throughput by roughly 10% (49.6 to 53.8 down to 45.4 to 48.4 leads/s in the back-to-back run), mostly from `UPDATE ... SET published_at` churn on `outbox_events`. The no-op consumer and the full consumer overlap with the relay-only run, so I cannot separate their cost from noise. In the full pipeline every event updates the same `campaign_outcomes` row, which is a hot-row contention point: fine for one campaign, something to shard if you scale it.

### Reproducing it

The commands are in the repo history and the raw logs record which loadgen commit produced each table. The shape of it:

```bash
go run ./cmd/loadgen -ping-bench=true
go run ./cmd/loadgen -dataset-experiment=true
go run ./cmd/loadgen -scenario=A -workers=8 -leads=2000 -runs=3
go run ./cmd/loadgen -scenario=AB -workers=8 -leads=2000 -runs=3
```

(Scenarios: `A` workers only, `A_RELAY` plus relay, `AB_NOOP` plus a no-op consumer, `AB` the full pipeline. Run `-h` for the rest of the flags.)

---

## Limitations

Stated plainly, so you do not have to find them yourself:
1. **No real telephony**: The carrier and agents are mocks.
2. **The shipped claim query does not scale past a few hundred thousand leads** (see above). The fix is prototyped, not merged into the engine.
3. **The time-zone table is 37 area codes**: Unknown ones are dead-lettered, which is safe but not useful at scale. State-specific calling windows (e.g., FL/OK 8pm cutoffs) are not implemented, and number portability is ignored.
4. **The server does not run the outbox consumer**: It is exercised by tests and the load generator.
5. **`processed_events` and `outbox_events` pruning is a function, not a job**.
6. **Lease heartbeats are not implemented**: Calls longer than the 2-minute lease would need them.
7. **Load test environment**: Generator, Postgres and NATS shared one Windows laptop through Docker and WSL2, with 3 to 6 ms of network overhead per query. Results compare variants, not absolute capacity.
8. **Seeded data is denser than real data**: My 1M-row seed has only about 5,650 distinct `created_at` values (rows are generated faster than the clock ticks), which makes the indexes tighter than a table built up over time. Buffer counts are optimistic for that reason.

---

## Repo map

| Path | Description |
| :--- | :--- |
| `cmd/server/` | HTTP server and wiring |
| `cmd/loadgen/` | Load generator and benchmark scenarios |
| `internal/pacer/` | Predictive pacing math and abandon-rate brake |
| `internal/queue/` | Campaign engine, outbox relay, outbox consumer |
| `internal/storage/` | Postgres repository (claiming, fencing, transactions) and in-memory repository |
| `internal/dnc/` | Bloom filter and phone normalization |
| `internal/timezone/` | Area code to time zone, curfew window logic |
| `internal/metrics/` | Prometheus exposition |
| `internal/model/` | Types, statuses, errors, lease constants |
| `migrations/` | 001 core schema, 002 outbox and idempotency tables |
| `tests/` | Unit, concurrency, and live Postgres and NATS integration tests |
| `docs/loadtest/raw/` | Unedited output behind every load-test number |

---

## Author

Shantanu Juvekar, [@yoohooshantanu](https://github.com/yoohooshantanu).
