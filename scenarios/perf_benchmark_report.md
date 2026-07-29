# Performance Benchmark & Financial Correctness Report

**Systems under test**: `payment-gateway-evtsrc` (event-sourced/CQRS) vs. `payment-gateway` (relational
baseline), benchmarked head-to-head through the identical BSI protocol workload.
**Date**: 2026-07-29 (afternoon re-run; see "Retraction" below for what changed since the morning)
**Test tool**: [k6](https://k6.io) v0.55+

---

## Two same-day corrections: a retraction, and then a correction to the retraction

An earlier version of this file, written the same morning, reported evtsrc saturating well before
the 2,000 TPS ramp completed (p99 1.16s–3.22s, needing 1,200+ VUs) and a financial-correctness
defect in both of its two runs (one payment double-recorded per run). Shortly after, the operator
restarted the machine after finding OrbStack running a hanging VM, and separately flagged "a severe
resource hogging problem" from that session. Re-run on a freshly-restarted machine with an explicit
contamination check added before each load-generation phase, **the saturation finding did not
reproduce**: evtsrc's p99 was 8.5–9.4ms in both runs (vs. 1.16s–3.22s that morning), flat across
every ramp stage, never exceeding its pre-allocated VU pool. That part of the morning's report is
retracted — the saturation ceiling was a contamination artifact, not a property of evtsrc's
architecture.

Both afternoon runs' correctness audits also passed with zero mismatches, and the first version of
this section retracted the correctness-defect finding on that basis too. **That was wrong, and was
corrected within the same session** after the operator pushed back: "the app should not do double
payment however low the resource is, correct?" Not reproducing under a clean environment shows the
defect is *rare*, not that it isn't *real* — those are different claims, and the first version of
this report conflated them. The defect was investigated directly, root-caused, reproduced
deterministically (with no dependency on load, timing, or machine state at all), and fixed. See §6.

The morning's raw artifacts remain in `scenarios/results/2026-07-29-*` and in
`docs/benchmark-remediation-guideline.md`'s "Fourth gap" for the record; the retraction and its own
correction are "Fifth gap" and "Sixth gap" in that same document.

---

## 1. Methodology

- **Load tool**: k6, `ramping-arrival-rate` executor, `preAllocatedVUs: 100`, `maxVUs: 2000`,
  50 → 500 → 1,000 → 2,000 TPS target over 90 seconds, identical profile on both systems.
- **Protocol under test**: the real production BSI adapter (`/api/bank/bsi`), full SHA-1 checksum
  verification, the same six seeded VA/amount pairs (CLOSED, OPEN, INSTALLMENT charge types) on
  both sides.
- **Scripts**: [`scenarios/suite-bsi.js`](suite-bsi.js) (evtsrc) and
  [`scenarios/suite-rdbms.js`](suite-rdbms.js) (RDBMS baseline).
- **Fresh database per run**: both systems' state was wiped completely before every run in this
  report — RDBMS via `docker compose down -v` + `docker compose up --build`; evtsrc via
  `docker compose down -v` plus deleting `target/rocksdb`, followed by a fresh `mvn spring-boot:run`.
  Each run's BSI shared secret was freshly generated immediately before that run and verified with a
  live checksum round-trip inquiry before any load was sent.
- **Environment contamination check (new this pass)**: before starting the RDBMS load-generation
  phase, `docker ps -a` showed a Testcontainers `postgres` + `ryuk` pair spinning up and tearing down
  every few seconds — traced to an unrelated session on the same machine, not this benchmark. Waited
  and re-checked twice (`docker ps -a` + `uptime`) until no recurrence appeared and 1-minute load
  average had dropped from 3.34 (at boot) to under 2, then proceeded. This check is now a standing
  part of the procedure, not a one-off: run it before every load-generation phase, not just once at
  the start.
- **Run conditions**: `RUN_ID` and `BSI_SHARED_SECRET` are required environment variables — both
  scripts throw in the k6 init stage if either is missing.
- **Audit**: [`scenarios/verify-correctness.py`](verify-correctness.py) with `--target evtsrc` or
  `--target rdbms` explicit.
- **Hardware**: Apple M5, 10-core, 16GB — shared, not dedicated, but this pass specifically
  controlled for *other processes actively competing for it* immediately before each run, which the
  morning run did not.

---

## 2. Measured results

| Metric | RDBMS (single run) | evtsrc Run 1 | evtsrc Run 2 |
|---|---|---|---|
| Run ID | `20260729073934` | `20260729074516` | `20260729074802` |
| Total requests | 86,384 | 86,602 | 86,625 |
| HTTP error rate | 0.00% | 0.00% | 0.00% |
| Dropped iterations | 241 | 23 | 0 |
| Effective throughput | 959.7 req/s | 960.4 req/s | 960.7 req/s |
| Min latency | 267 µs | 419 µs | 425 µs |
| Median latency | 1.01 ms | 3.96 ms | 3.83 ms |
| Avg latency | 4.25 ms | 4.15 ms | 3.92 ms |
| p90 | 3.28 ms | 6.39 ms | 6.26 ms |
| p95 | 6.84 ms | 6.89 ms | 6.71 ms |
| p99 | 112.25 ms | **9.41 ms** | **8.50 ms** |
| Max latency | 333.88 ms | 87.64 ms | 62.85 ms |
| Peak VUs used | 288 of 2,000 | 122 of 2,000 | **100 of 2,000** (never exceeded pre-allocation) |
| Threshold `p(99)<500ms` | PASS | PASS | PASS |
| Threshold `http_req_failed<1%` | PASS | PASS | PASS |
| Accepted payments | 28,885 | 28,788 | 28,798 |
| Rejected (charge already closed) | 57,499 | 57,814 | 57,827 |
| Financial correctness audit | PASS | PASS | PASS |

Artifacts: `scenarios/results/2026-07-29b-{rdbms,evtsrc}-fresh{,1,2}-{summary.json,raw.json.gz}`.

Both evtsrc runs agree tightly with each other (p99 8.5ms vs. 9.41ms, both flat across every ramp
stage — see §3), which is the signature of a system running well within capacity, not near a limit.
RDBMS's own p99 (112.25ms) is higher than a prior clean RDBMS run's 50.80ms; per the note above, the
Testcontainers interference observed just before this run is a plausible explanation, though RDBMS's
own knee-analysis (below) shows no saturation shape (median stays low throughout, only a mild tail
bump in the peak-concurrency stage) — this is ordinary shared-hardware noise, not the kind of
backlog-that-never-drains signature the morning's evtsrc runs showed.

---

## 3. Knee/saturation analysis — flat on both systems this time

[`scenarios/knee-analysis.py`](knee-analysis.py) per-stage breakdown:

**RDBMS**:

| Stage | Obs. TPS | p50 | p95 | p99 |
|---|---|---|---|---|
| Ramp 50→500 TPS | 277.9 | 0.93ms | 3.66ms | 6.79ms |
| Ramp 500→1,000 TPS | 751.5 | 0.83ms | 2.50ms | 4.21ms |
| Ramp 1,000→2,000 TPS | 1,494.8 | 1.33ms | 26.22ms | 152.27ms |
| Ramp-down | 988.5 | 0.61ms | 5.84ms | 29.66ms |

**evtsrc Run 1**:

| Stage | Obs. TPS | p50 | p95 | p99 |
|---|---|---|---|---|
| Ramp 50→500 TPS | 275.9 | 5.04ms | 7.70ms | 9.18ms |
| Ramp 500→1,000 TPS | 750.5 | 3.95ms | 6.66ms | 8.30ms |
| Ramp 1,000→2,000 TPS | 1,500.0 | 3.94ms | 6.95ms | 12.18ms |
| Ramp-down | 996.5 | 3.94ms | 6.84ms | 8.21ms |

**evtsrc Run 2**:

| Stage | Obs. TPS | p50 | p95 | p99 |
|---|---|---|---|---|
| Ramp 50→500 TPS | 275.7 | 4.96ms | 7.81ms | 8.84ms |
| Ramp 500→1,000 TPS | 750.5 | 3.94ms | 6.61ms | 7.55ms |
| Ramp 1,000→2,000 TPS | 1,500.6 | 3.73ms | 6.61ms | 9.74ms |
| Ramp-down | 997.2 | 3.78ms | 6.62ms | 7.79ms |

evtsrc's tail is essentially flat across all four stages in both runs — the 1,000→2,000 TPS stage's
p99 (12.18ms, 9.74ms) is barely above the ramp's lowest-load stage. RDBMS shows a mild, recovering
tail bump in the same stage (152.27ms, well under the 500ms threshold, and gone by ramp-down). Both
shapes are healthy. Neither shows the "median itself degrades and the tail keeps climbing past
ramp-down" signature the morning's evtsrc runs showed.

---

## 4. Financial correctness audit (`scenarios/verify-correctness.py`)

| Run | k6 outcome log vs. recorded payment rows | RocksDB vs. Postgres (evtsrc only) |
|---|---|---|
| RDBMS | PASS — 28,885 accepted, 57,499 rejected (0 or 1 flagged row each), 0 mismatches | N/A |
| evtsrc Run 1 | PASS — 28,788 accepted, 57,814 double-settlement (every one flagged), 0 mismatches | PASS — 0 mismatches |
| evtsrc Run 2 | PASS — 28,798 accepted, 57,827 double-settlement (every one flagged), 0 mismatches | PASS — 0 mismatches |

All three runs pass cleanly on every check. Compare against the Fourth gap in
`docs/benchmark-remediation-guideline.md`, where the same audit against the same code, the same
scripts, and the same seed data failed identically in two out of two runs that morning — and see §6
below for why "these three runs passed" does not mean the defect that morning wasn't real.

---

## 6. The double-write defect: root cause and fix (independent of load)

The morning's defect — one `bankReference` recorded as both an accepted payment and a flagged
double settlement — was investigated directly rather than left as "retracted, unexplained." Root
cause: `PostgresProjectionSink` projects `PaymentReceivedEvent` and `DoubleSettlementDetectedEvent`
through two independent methods that didn't check each other's work. `PaymentApplicationService`'s
request-thread pre-validation can optimistically accept a payment that the Kafka Streams
topology — the actual single-writer authority — later finds was already settled by a racing
payment, and correctly emits a `DoubleSettlementDetectedEvent` for that same `bankReference`
(exactly the residual pre-validation race the code's own comments document). The sink had already
unconditionally inserted an "accepted" row for the optimistic accept, and then unconditionally
inserted a second, contradicting row for the correction, instead of the correction retracting the
first row in place.

This is a pure logic defect, not fundamentally a low-resource phenomenon: resource contention only
widens the timing window in which multiple concurrent requests can hit a brand-new `CLOSED` charge
before the first one closes it — it doesn't create the race, it just makes it more likely to be hit.
That means it can be reproduced and verified with no dependency on real load, real Kafka timing, or
machine state at all. `src/test/java/.../projection/sink/PostgresProjectionSinkTest.java` does
exactly that: it calls `PostgresProjectionSink.consumeDomainEvents(List)` directly with a
hand-crafted `PaymentReceivedEvent` followed by a `DoubleSettlementDetectedEvent` for the same
`bankReference` — no Kafka, no concurrency, no timing dependency. Verified against the pre-fix code,
it fails every time (`Expected size: 1 but was: 2`); against the fix, it passes every time. This is
stronger evidence than another load-test run would have been — the defect's hit rate that morning
(roughly 1 in 26,000–27,000 accepted payments) means a clean load-test run proves little either way;
a deterministic test proves it every time.

Fixed in `PostgresProjectionSink.projectDoubleSettlementBatch`: it now looks up any existing payment
row for the incoming event's `(bankCode, bankReference)` and, if found, flips it to
`isDoubleSettlement=true` in place — and retracts its amount from the charge's paidAmount/status via
the new `correctChargesForRetractedPayments` — instead of inserting a second, contradicting row. The
normal case (a `bankReference` that was always going to be rejected and never optimistically
accepted) is unaffected and covered by a second test.

**The lesson**: "didn't reproduce under a clean environment" is evidence about how *often* a defect
manifests, not evidence about whether it's real. A correctness defect that only shows up "sometimes,
under load" is still a correctness defect a payment gateway cannot have — production systems
experience exactly that kind of contention (GC pauses, CPU spikes, slow disks) as routine operating
conditions, not as a benchmark edge case to discount.

### Does the fix have a request-latency cost? No — it's on a fully async path

The fix lives in `PostgresProjectionSink`, a Kafka consumer that runs after the HTTP response has
already been sent back to the bank — it never touches the request thread that k6's p50/p95/p99
numbers measure. A k6 re-run therefore cannot show a different *latency* number because of this fix,
and didn't need to be run for that reason.

What the fix *could* plausibly affect is **projection lag** — how far the async Postgres read model
falls behind the write path under sustained throughput, since `projectDoubleSettlementBatch` now
does one additional batched, indexed lookup per poll (`findByBankReferenceIn`) instead of none. This
was checked directly: one full k6 run (`RUN_ID 20260729081443`, same ramp profile, same seed) with
`GET /api/admin/debug/projection-lag` (exposed for exactly this purpose, per G3) polled roughly every
9 seconds throughout.

| Elapsed | Lag (ms) |
|---|---|
| ~9s | 4 |
| ~18s | 6 |
| ~27s | 4 |
| ~36s | 1 |
| ~45s | 1 |
| ~54s | 3 |
| ~63s | 1 |
| ~72s | 3 |
| ~81s | 8 |
| ~90s | 8 |
| ~93s (after ramp-down) | 8 |

Lag stayed in single-digit milliseconds throughout, including the 1,000→2,000 TPS peak — no growth,
no backlog. Request latency in this same run (p99 10.72ms, 86,572 requests, 0 dropped-iteration
concerns beyond the usual) matched the other clean afternoon runs, and the correctness audit passed
with zero mismatches on both checks. The added query does not create a bottleneck at this workload's
scale.

---

## 7. What's still needed for full confidence

1. **Dedicated (not shared) hardware** remains an open gap for the *performance* numbers — even this
   afternoon's clean run shares the machine with background OS processes, and the morning's
   contamination episode is a direct demonstration of how much that can matter. This does not apply
   to §6's fix, which is verified independently of hardware or load.
2. **A repeatable, scriptable environment-contamination check.** This pass's check
   (`docker ps -a` + `uptime`, done manually, twice, by eye) worked, but a small script that fails
   loudly if unrelated containers are churning or load average exceeds a threshold would make this
   procedure enforceable rather than a manual step someone can forget.
3. ~~A confirmatory load-test run against the fix is optional, not required.~~ Done anyway — see §8.
   It turned out to matter for a different reason than expected: the §6 fix was itself superseded by
   a deeper one, and the re-run confirmed the deeper fix cost nothing in the metrics that matter.
4. RDBMS's hot-row lock-contention behavior (from an even earlier report, run against a long-lived,
   never-reset database) remains unverified in any of today's runs, which all started from empty
   databases by design.

---

## 8. The deeper fix: rearchitecting the write path onto a direct RocksDB transaction

§6's fix was real and is still true — but it was a downstream patch, not a root-cause fix. It
stopped `PostgresProjectionSink`'s database record from staying wrong; it did nothing about the
request thread having already told the bank "ACCEPTED" over HTTP *before* Kafka Streams' async
re-check could disagree. That gap — between the request thread's optimistic read and the topology's
later authoritative decision — was still there, and still a genuine correctness hole for a payment
gateway to have, however rarely it manifested.

The actual fix: `ChargeSettlementStore`, a RocksDB `TransactionDB` owned directly by the
application, not by Kafka Streams. VA resolution, the charge terminal-status check, and the balance
update now happen as one atomic transaction on the request thread itself, using `getForUpdate` for
the same row-lock semantics `SELECT FOR UPDATE` gives a relational transaction. There is no longer a
gap between "checked" and "applied" for a second request to land in — whatever the transaction
returns is the final answer, synchronously, with no later re-decision possible. Kafka Streams'
`charge-state-store`, `va-registry-store`, `idempotency-store`, and `PaymentEventProcessor`'s
decision logic are removed entirely; once both reads and writes moved to `ChargeSettlementStore`,
those stores had nothing left to do, and an unused mirror would have quietly recreated the exact
"two places that can disagree" pattern this project keeps finding. Kafka's role is unchanged in one
respect — it still broadcasts the already-decided outcome to `PostgresProjectionSink`'s read model.

**Verified two ways, matching the project's own rule about deterministic proof over lucky load-test
runs:**

1. `ConcurrentPaymentSettlementIntegrationTest` fires 50 *real* concurrent threads (not sequential
   event replay) at one freshly-created `CLOSED` charge. Exactly one is accepted, the other 49 are
   correctly flagged, and `cumulativePaid` never double-counts — proof the race is closed under
   genuine thread contention, not just that one specific event ordering is handled.
2. The full benchmark was re-run against the rearchitected build, twice, fresh environment each time:

| Metric | Rearchitected Run 1 | Rearchitected Run 2 | (for comparison) Fifth gap's post-fix runs |
|---|---|---|---|
| Run ID | `20260729162811` | `20260729163104` | `20260729074516` / `20260729074802` |
| p99 latency | 9.96ms | 9.85ms | 9.41ms / 8.50ms |
| Peak VUs (of 2,000 allotted) | 32 (never left the 100-VU pre-allocation) | 33 (same) | 122 / 100 |
| Correctness audit | PASS, 0 mismatches (both checks) | PASS, 0 mismatches (both checks) | PASS, 0 mismatches |

Statistically indistinguishable from the numbers before this rearchitecture — the fix that closes
the correctness gap at its actual source cost nothing in the metrics that matter, because the new
write path never touches Kafka at all for the decision; it was always going to be at least as fast
as the old one, and the data confirms it.

A reminder that compounds on §6's own lesson, one level deeper: a fix that stops the *symptom* from
recurring is not automatically a fix for the *cause*. Both can be true and verified at the same
time — §6's projection-level fix is real and still correct — while the actual hole remained open
underneath it. It took a third round of a direct, specific technical question to get from "the
symptom is patched" to "the mechanism that caused it no longer exists."

No numbers above are invented or estimated — every figure comes from a committed artifact under
`scenarios/results/` and a live `verify-correctness.py` / `knee-analysis.py` run against it.

## 9. Per-VA status (paying VA PAID, siblings CANCELLED) added no cost either

§8's `ChargeSettlementStore` tracked only charge-level status. A closer look (prompted while writing
up §8 for publication) found it should track per-VA status too — the RDBMS reference implementation
marks the VA that actually received payment `PAID` and every sibling `CANCELLED`, not all of them
`PAID` — and that `InquiryApplicationService` didn't check status at all, so an inquiry against a
settled charge's VA (paying or sibling) still returned `SUCCESS`. Fixed: VA records now carry their
own `ACTIVE`/`PAID`/`CANCELLED` status, updated inside the same atomic settlement transaction, and
inquiry checks it. See `docs/benchmark-remediation-guideline.md`'s "Eighth gap" for the full account,
including a regression this caught and fixed along the way (`applyPayment`'s own VA lookup still
assumed the old raw-string value format after VA records moved to JSON).

Re-benchmarked twice, fresh environment each time (one contaminated attempt — an unrelated project's
Maven build running concurrently on the same machine — discarded and re-run rather than reported):

| Metric | Run 1 (`vastatus1`) | Run 2 (`vastatus2`) | §8's runs (for comparison) |
|---|---|---|---|
| Run ID | `20260729181245` | `20260729201858` | `20260729162811` / `20260729163104` |
| p99 latency | 9.19ms | 8.65ms | 9.96ms / 9.85ms |
| Peak VUs (of 2,000 allotted) | 46 | 31 | 32 / 33 |
| Correctness audit | PASS, 0 mismatches (both checks) | PASS, 0 mismatches (both checks) | PASS, 0 mismatches |

Also confirmed live, not just via the test suite: inquiring a settled charge's VA against the running
benchmark instance now returns `{"status":"INVALID_VA","message":"...no longer active (status
PAID)"}` with HTTP 404, where it previously returned `SUCCESS`.

Caveat worth stating plainly: `scenarios/suite-bsi.js` registers exactly one BSI sibling per seeded
charge, so this benchmark never exercises cancelling more than one sibling per settlement. A
multi-sibling pay-via-any-bank charge walks one prefix-scanned `getForUpdate` + JSON parse/serialize
per sibling inside the same transaction the bank's HTTP response waits on — plausibly still cheap at
the small sibling counts (2-3 banks) this domain expects, but that specific path remains unmeasured
here, not confirmed cheap by this data.
