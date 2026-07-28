# Performance Benchmark & Financial Correctness Report

**Systems under test**: `payment-gateway-evtsrc` (event-sourced/CQRS) vs. `payment-gateway` (relational
baseline), benchmarked head-to-head through the identical BSI protocol workload.
**Date**: 2026-07-29
**Test tool**: [k6](https://k6.io) v0.55+

Earlier versions of this file contained numbers from 2026-07-28: one run per system against a
freshly-migrated database, plus a second run per system against the *same, never-reset* database
roughly 40 minutes later. That second-run pairing was never an independent repeat measurement — it
measured "cold" vs. "warm-with-tens-of-thousands-of-rows-of-history," not two samples of the same
condition (see that section's own admission in `docs/benchmark-remediation-guideline.md`'s "What's
still needed" list). This version replaces it with what that gap called for: every run below starts
from an empty, freshly-migrated database. evtsrc's run was repeated twice specifically to check
whether an anomaly found in the first run was a fluke; RDBMS was run once, since nothing in it
warranted a repeat.

---

## 1. Methodology

- **Load tool**: k6, `ramping-arrival-rate` executor, `preAllocatedVUs: 100`, `maxVUs: 2000`,
  50 → 500 → 1,000 → 2,000 TPS target over 90 seconds, identical profile on both systems.
- **Protocol under test**: the real production BSI adapter (`/api/bank/bsi`), full SHA-1 checksum
  verification, the same six seeded VA/amount pairs (CLOSED, OPEN, INSTALLMENT charge types) on
  both sides.
- **Scripts**: [`scenarios/suite-bsi.js`](suite-bsi.js) (evtsrc) and
  [`scenarios/suite-rdbms.js`](suite-rdbms.js) (RDBMS baseline) — identical request shape, checksum
  scheme, and ramp profile. evtsrc's source of truth is Kafka/RocksDB, not Postgres, so
  `suite-bsi.js` seeds its own six charges via `setup()` calling the real `POST /api/v1/charges`
  and polls until each hydrates to `ACTIVE` before the ramp starts. RDBMS was seeded via
  `scenarios/seed-db-direct.py` before its run.
- **Fresh database per run**: before every run in this report, both systems' state was wiped
  completely — RDBMS via `docker compose down` + volume removal + `docker compose up --build`;
  evtsrc via `docker compose down -v` (drops the Postgres projection and the Kafka broker/topics)
  plus deleting the local `target/rocksdb` directory (Kafka Streams' on-disk state store), followed
  by a fresh `mvn spring-boot:run`. Each run's BSI shared secret was freshly generated and applied
  immediately before that run (RDBMS: encrypted with the app's own `SecretCipher` and written to
  the escrow row; evtsrc: passed directly as the `BANK_SECRET_BSI` environment variable) and
  verified with a live checksum round-trip inquiry before any load was sent.
- **Run conditions**: `RUN_ID` and `BSI_SHARED_SECRET` are required environment variables — both
  scripts throw in the k6 init stage if either is missing.
- **Audit**: [`scenarios/verify-correctness.py`](verify-correctness.py), cross-checking the k6
  `payment_outcomes` metric (an independent ground truth of what actually happened per request)
  against the recorded payment rows, plus a RocksDB-vs-Postgres consistency check for evtsrc. Run
  with `--target evtsrc` or `--target rdbms` explicitly.
- **Hardware**: Apple M5, 10-core, 16GB — shared, not dedicated. evtsrc's own footprint (Kafka
  broker + 6 Kafka Streams threads + the app JVM) runs on the same machine as the load generator,
  which is itself relevant to the results below.

---

## 2. Measured results

| Metric | RDBMS (single run) | evtsrc Run 1 | evtsrc Run 2 |
|---|---|---|---|
| Run ID | `20260729053927` | `20260729055009` | `20260729060057` |
| Total requests | 86,439 | 78,183 | 79,240 |
| HTTP error rate | 0.00% | 0.00% | 0.00% |
| Dropped iterations | 185 | 8,441 | 7,384 |
| Effective throughput | 960.3 req/s | 860.3 req/s | 868.1 req/s |
| Min latency | 308 µs | 560 µs | 641 µs |
| Median latency | 858 µs | 12.09 ms | 27.9 ms |
| Avg latency | 3.30 ms | 174.81 ms | 136.04 ms |
| p90 | 4.66 ms | 611.19 ms | 424.95 ms |
| p95 | 13.88 ms | 1.05 s | 592.01 ms |
| p99 | **50.80 ms** | **3.22 s** | **1.16 s** |
| Max latency | 197.03 ms | 3.91 s | 1.41 s |
| Peak VUs used | 156 of 2,000 | 1,414 of 2,000 | 1,213 of 2,000 |
| Threshold `p(99)<500ms` | PASS | **FAIL** | **FAIL** |
| Threshold `http_req_failed<1%` | PASS | PASS | PASS |
| Accepted payments | 28,810 | 26,259 | 26,615 |
| Rejected (charge already closed) | 57,629 | 51,924 | 52,625 |
| Financial correctness audit | PASS | **FAIL** | **FAIL** |

Artifacts: `scenarios/results/2026-07-29-{rdbms,evtsrc}-fresh{,2}-{summary.json,raw.json.gz}`.

---

## 3. evtsrc saturates well before 2,000 TPS; RDBMS does not

Both systems ran the identical ramp on the identical machine. RDBMS absorbed it cleanly — p99 stayed
under 51ms, and the load generator only ever needed 156 of its 2,000 allotted virtual users. evtsrc
needed 1,200–1,400 VUs to sustain the *same* request rate, dropped far more iterations (7,400–8,400
vs. 185), and missed the `p(99)<500ms` threshold by 2.3x–6.4x in both of its runs.

[`scenarios/knee-analysis.py`](knee-analysis.py) buckets each run's raw time series into the ramp's
own stage windows, showing exactly where this happens:

**RDBMS** — flat and recovers:

| Stage | Obs. TPS | p50 | p95 | p99 |
|---|---|---|---|---|
| Ramp 50→500 TPS | 279.6 | 1.35ms | 5.81ms | 14.50ms |
| Ramp 500→1,000 TPS | 752.3 | 0.64ms | 4.04ms | 24.39ms |
| Ramp 1,000→2,000 TPS | 1,498.5 | 0.87ms | 17.08ms | 57.14ms |
| Ramp-down | 981.4 | 1.05ms | 23.00ms | 65.44ms |

**evtsrc Run 1** — a knee that never drains:

| Stage | Obs. TPS | p50 | p95 | p99 |
|---|---|---|---|---|
| Ramp 50→500 TPS | 278.7 | 7.03ms | 21.36ms | 49.47ms |
| Ramp 500→1,000 TPS | 747.1 | 6.39ms | 32.88ms | 157.75ms |
| Ramp 1,000→2,000 TPS | 1,349.6 | 18.88ms | 279.96ms | 1,510.17ms |
| Ramp-down | 740.1 | **727.48ms** | **3,387.19ms** | **3,583.90ms** |

**evtsrc Run 2** — same shape, milder:

| Stage | Obs. TPS | p50 | p95 | p99 |
|---|---|---|---|---|
| Ramp 50→500 TPS | 276.3 | 9.29ms | 73.67ms | 219.22ms |
| Ramp 500→1,000 TPS | 746.5 | 7.77ms | 39.89ms | 104.07ms |
| Ramp 1,000→2,000 TPS | 1,335.2 | 82.19ms | 504.27ms | 1,214.84ms |
| Ramp-down | 842.9 | 213.17ms | 891.01ms | 1,198.19ms |

RDBMS's median never moves and its tail recovers as soon as the ramp comes back down — normal
elastic behavior under a load spike. evtsrc's **median itself degrades** through the 1,000→2,000 TPS
stage and is *still climbing* in the ramp-down window (worse than the peak-load stage in Run 1) —
the signature of a backlog that formed faster than it could drain and had not finished draining when
the test ended. This is reproducible: both independently-reset runs show the same shape, just with
different severity (Run 1 was measurably worse than Run 2, both sharing the same 10 cores with
Kafka, Kafka Streams, Postgres, the app JVM, and the load generator itself, and machine-load
variance between the two attempts is a plausible source of that difference).

**Likely mechanism.** RDBMS's request path is a single, short-lived SQL transaction. evtsrc's
request path must complete a synchronous Kafka produce-and-acknowledge round trip (plus two
interactive RocksDB state-store reads) before the HTTP thread can return — inherently more wall-clock
per request even when nothing is contended. By Little's Law, sustaining the same target throughput
at a higher per-request latency requires proportionally more requests in flight at once — which is
exactly what the VU counts above show (RDBMS: 156 concurrent; evtsrc: 1,200+). Neither app sets
`server.tomcat.threads.max` (both default to Spring Boot's 200), so evtsrc's concurrency requirement
crossing that ceiling is a plausible amplifier of the tail, on top of the inherently higher
per-request cost. This is a plausible explanation consistent with the data, not a confirmed root
cause — confirming it would need runtime thread-pool/queue-depth metrics captured during the run,
which this pass didn't collect (see §6).

---

## 4. Financial correctness defect found under saturation (evtsrc only)

Both evtsrc runs failed `verify-correctness.py`. RDBMS did not. In each evtsrc run, exactly one
`bankReference` out of ~78,000–79,000 requests was recorded **twice** in the payment table — once as
a normal accepted payment (`is_double_settlement=false`) and once flagged as a double settlement
(`is_double_settlement=true`) — for the same charge, same amount:

| Run | bankReference | chargeId | Charge type | Amount |
|---|---|---|---|---|
| 1 | `20260729055009-1785279010406-715065` | `8f6b4076-...` | CLOSED | 3,200,000.00 |
| 2 | `20260729060057-1785279658729-854974` | `07ff836f-...` | CLOSED | 450,000.00 |

This is a genuine defect, not an artifact of the audit tooling: a single client-issued
`bankReference` (k6's `idTransaksi`, generated fresh per iteration from a millisecond timestamp plus
a random suffix) was recorded under two different outcomes by the server. It happened exactly once
per run, both times on a CLOSED-type charge, both times a charge that had just been created (within
under a second of the charge-creation timestamp in the app log) — consistent with the race the
codebase's own comments already flag as a known, deliberately-not-silently-absorbed gap:
`PaymentApplicationService`'s pre-validation "is NOT the authoritative serialization point — two
concurrent callbacks can both pass this pre-check before either event is applied."

What was ruled out by direct evidence rather than assumed:

- **Not a k6-side duplicate.** `idTransaksi` is generated fresh per iteration
  (`${RUN_ID}-${Date.now()}-${random}`); at ~870 req/s peak, two iterations colliding on both the
  millisecond and the random suffix is not something you'd expect to see even once across ~157,000
  total requests in this report, let alone identically in both runs.
- **Not a Kafka consumer-group rebalance.** The app log shows zero rebalance events
  (`PARTITIONS_REVOKED`/reassignment) during either run's load window.
  `max.poll.interval.ms=300000` is far above the 90-second test, so no poll-loop-stall-triggered
  rebalance is expected either.
- **Not a plain redelivery of the same Kafka record.** The log never once shows the topology's own
  `"Duplicate bankReference re-observed in topology, skipping apply"` guard firing — meaning this
  was not the same Kafka record being redelivered and reprocessed end-to-end.

What remains an open question: the exact mechanism by which the *same* `bankReference` ends up
applied through both code paths. One structural detail worth noting for whoever picks this up: all
three of evtsrc's Kafka Streams state stores (`charge-state-store`, `va-registry-store`,
`idempotency-store`) are declared with `Stores.persistentKeyValueStore(...)` and no
`.withCachingDisabled()` call, so Kafka Streams' default record cache is active on all of them —
and interactive queries (which is how `PaymentApplicationService`'s request-thread pre-check reads
these stores) are a documented case where a cached write is not guaranteed visible to a concurrent
reader until the cache flushes. That is a plausible contributor to the *general* pre-validation race
the code already documents, but it does not by itself explain how one single `bankReference` was
recorded under two different outcomes rather than two different `bankReference`s each getting their
own (correct) outcome — which is what happened correctly 51,924 and 52,625 other times in these two
runs. This needs dedicated reproduction (DEBUG-level Kafka Streams/producer logging, or rerunning
with caching explicitly disabled) before asserting a root cause; this report only asserts what it
directly observed.

**Why this matters for the comparison**: RDBMS's payment path is a single ACID transaction guarded
by `SELECT FOR UPDATE` — there is no code path in that design by which one client-issued reference
could be recorded under two different outcomes. evtsrc's audit failing twice, in the same specific
way, specifically during the saturation window from §3, is a real reliability difference the latency
numbers alone don't capture: under load, RDBMS's worst observed behavior in this report is *slow*;
evtsrc's is a small but non-zero rate of duplicate financial records (roughly 1 in 26,000–27,000
accepted payments in each run).

---

## 5. Financial correctness audit (`scenarios/verify-correctness.py`)

| Run | k6 outcome log vs. recorded payment rows | RocksDB vs. Postgres (evtsrc only) |
|---|---|---|
| RDBMS | PASS — 28,810 accepted, 57,629 rejected (0 or 1 flagged row each), 0 mismatches | N/A |
| evtsrc Run 1 | **FAIL — 1 mismatch** (see §4) out of 78,183 ground-truth entries | **FAIL — 1 mismatch**, same charge |
| evtsrc Run 2 | **FAIL — 1 mismatch** (see §4) out of 79,240 ground-truth entries | **FAIL — 1 mismatch**, same charge |

RDBMS's rejections correctly show zero footprint (its pessimistic lock rejects before any row is
written for the ordinary, non-racing case). evtsrc's rejections correctly show exactly one flagged
row each, with the one exception identified in §4 per run.

---

## 6. What's still needed for full confidence

1. **Root-cause the §4 defect.** Reproduce with DEBUG-level Kafka Streams consumer/producer logging,
   or rerun with `.withCachingDisabled()` on all three state stores to see whether the defect
   disappears — that would confirm or rule out the caching-staleness hypothesis directly instead of
   leaving it as a plausible-but-unconfirmed explanation.
2. **Confirm the §3 saturation mechanism.** Capture Tomcat thread-pool active-count and Kafka
   producer/consumer queue depth *during* a run to confirm the thread-pool-ceiling hypothesis rather
   than infer it from VU counts and latency shape alone.
3. **Dedicated (not shared) hardware**, and specifically for evtsrc: isolate how much of its
   overhead is the Kafka broker and Streams threads competing with the app JVM and the load
   generator for the same cores, versus an inherent cost of the produce-and-wait request path.
4. **RDBMS's own hot-row lock-contention behavior** (documented in an earlier version of this
   report from a long-running, never-reset database) was not re-verified in this pass, since every
   run here started from an empty database. If that finding matters for capacity planning, it needs
   its own dedicated long-running test with a small, deliberately concentrated VA pool.

No numbers above are invented or estimated — every figure comes from a committed artifact under
`scenarios/results/` and a live `verify-correctness.py` / `knee-analysis.py` run against it.
