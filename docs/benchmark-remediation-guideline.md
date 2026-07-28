# Benchmark Remediation Guideline

Audit date: 2026-07-28. Scope: `payment-gateway-evtsrc` codebase, its k6 perf test design/execution,
and the published comparison against the relational `payment-gateway` baseline.

This document records what the audit found and defines what a proper implementation and a legitimate
re-benchmark must look like. Implementation follows this writeup.

---

## Part I — Audit Findings (what is wrong today)

### F1. The hot path implements none of the claimed validation

Claimed (README §3.5): sub-millisecond pre-validation against local RocksDB (idempotency check,
VA/invariant check) before appending to Kafka; HTTP 400 for `CHARGE_ALREADY_CLOSED` / `INVALID_AMOUNT`;
HTTP 200 duplicate-ACK for replayed references.

Actual (`service/PaymentApplicationService.java`, `web/api/BankCallbackController.java`):

- No store lookup of any kind on the request thread. The request is serialized into a
  `PaymentReceivedEvent` and appended via `kafkaTemplate.send(...).get()`. Every well-formed JSON
  body gets `200 SUCCESS`.
- Fallback defaults are substituted for missing fields: `"UNKNOWN_CHARGE"`, `"GENERIC_BANK"`,
  `bankReference` defaulted to a fresh eventId, `paymentTimestamp` defaulted to `now()`. This
  violates the fail-loud/no-fallback principle and silently manufactures unmatchable events.
- Consequence for the benchmark: the measured system was "JSON serialization + single-partition
  Kafka append on localhost", not a payment gateway.

### F2. The streams topology enforces no invariants

`streams/PaymentGatewayStreamsTopology.java`:

- `idempotency-store` is written but never read. Nothing in the system blocks a duplicate
  `bankReference`.
- Overpayment is silently swallowed: `newAmount = current.subtract(payment).max(ZERO)`. A CLOSED
  charge accepts unlimited full payments with no error, no flag, no event.
- No sibling-VA cancellation on full settlement. `va-registry-store` entries are never retired.
- `DoubleSettlementDetectedEvent` exists as a class and has a projection handler, but no code path
  emits it. The published "Double Settlements: 0" is vacuous — the detector does not exist.
- `log.info` per record inside hot processors (charge hydration, VA registry, payment) — a
  throughput drag and a payload leak into logs.

### F3. The financial correctness audit is tautological and ran against an incomplete projection

- `projection/sink/PostgresProjectionSink.java` computes `paid_amount` / `remaining_amount` /
  `status` from the same payment rows it inserts, in the same transaction.
  `scenarios/verify-correctness.py` then checks `paid_amount == SUM(payments)` — which cannot fail
  by construction. It verifies sink arithmetic, not business invariants.
- Only 1,119 payments were projected out of ~113,000 acknowledged callbacks at audit time: the
  sink is a single consumer on an auto-created 1-partition topic doing per-event JPA round-trips
  (existence check + insert + charge read + charge update). Projection lag was hours, not the
  claimed 10–100 ms. The audit certified a read model that was missing ~99% of the acknowledged
  traffic.
- Framed operationally: the system returned `200 OK` to ~85k unique-reference payment
  notifications that were not visible anywhere queryable when audited.

### F4. Infrastructure does not match the documented topology

- `compose.yml` sets no `KAFKA_NUM_PARTITIONS`; no `NewTopic` beans exist. All topics auto-create
  with 1 partition. The README's partition sizing framework (6 starter / 12 production) was not in
  effect; 6 configured stream threads had one partition to share.
- `num.standby.replicas: 0`, single broker — the HA/failover claims (warm standby promotion <1s)
  are untested and untestable in this setup.
- `spring.jpa.hibernate.ddl-auto: update` coexists with Flyway. Pick Flyway only.

### F5. The k6 suites are different workloads, contradicting the "single reusable suite" claim

- `scenarios/suite.js` (evtsrc): generic `/api/v1/payments`, no auth/signature, `chargeId` handed
  to the server in the payload (no VA resolution exercised), 17 VAs across 6 bank codes — three of
  which (BCA/BNI/BRI) have no adapter in `payment-gateway` at all.
- `scenarios/suite-rdbms.js` (baseline): the real production BSI adapter `/api/bank/bsi`,
  proprietary payload, SHA-1 checksum (valid because the seeded escrow secret is NULL and Java
  string concatenation yields the literal `"null"`), 6 BSI VAs mapping to 6 distinct charges.
- Different endpoints, different per-request validation cost, different charge/contention mix.
  The evtsrc suite concentrates 6 sibling VAs on one charge; the RDBMS suite has no sibling
  contention at all — its serialized-lock pressure fell on the 2 OPEN charge rows only.

### F6. "0% error rate / 100% success" is meaningless on both sides

- The BSI adapter maps every rejection to HTTP 200 with an in-body response code (protocol
  behavior). evtsrc accepts everything. `http_req_failed` therefore cannot register a business
  rejection on either system.
- Actual accepted-payment workload in the RDBMS run: ~28.9k payments (≈ 2/6 of traffic, on the two
  OPEN VAs) ≈ ~320 real ACID transactions/sec; the rest was lookup-and-reject. Neither report
  states this decomposition.

### F7. The §8.1 head-to-head table in `scenarios/perf_benchmark_report.md` is not traceable to any run

- Added in a docs-only commit (`71cc0c8`) with no run artifacts.
- The evtsrc column contradicts the measured runs in §3–4 of the same file: p50 682 µs / 86,581
  completed / zero drops, versus measured p50 553 ms (Run 1) and 2.66 s (Run 2) with 23,654 and
  36,001 dropped iterations.
- The column is a collage: min 875 µs = Run 2's min; max 3.47 s = Run 1's max; "p95 4.12 ms" =
  Run 2's max of 4.12 **s** with the unit swapped; p50/p90/p99 appear in no k6 output.
- Request count identical to the RDBMS run to the request (86,581) and throughput identical to
  0.007% — not what independent runs produce.
- Assorted drift: seed described as 19 VAs (§2) and 23 VAs (§8.1); `suite.js` contains 17; §2 says
  ramp starts at 100 TPS, the script says `startRate: 50`.

Sections 8.1–8.3 of the report, and README §3.5/§5.4 claims, must be treated as invalid until
re-measured.

### What IS credible

The RDBMS run hangs together arithmetically (86,581 ≈ the ramp-schedule integral; 28,875 payments
≈ 2/6 of traffic; IDR 2.17B ≈ 28.9k × avg 75k) and exercised the full production path (checksum,
generation-aware VA lookup, charge row lock, idempotency re-check, payment insert, audit event,
webhook enqueue) at p50 0.57 ms / p99 16.26 ms with zero dropped iterations on shared laptop
hardware. That baseline stands and supports the Option A selection in
`payment-gateway/docs/architecture-comparison.md`.

---

## Part II — Implementation Guideline (what "properly done" means)

### [DONE] G1. Implement the claimed hot path in evtsrc — or delete the claim

Implemented in `src/main/java/com/artivisi/paymentgateway/service/PaymentApplicationService.java` (`processPayment`), `src/main/java/com/artivisi/paymentgateway/web/api/PaymentOutcome.java`, `src/main/java/com/artivisi/paymentgateway/web/api/PaymentCallbackRequest.java`, and `src/main/java/com/artivisi/paymentgateway/web/api/BankCallbackController.java`. Covered by `src/test/java/com/artivisi/paymentgateway/web/api/BankCallbackControllerIntegrationTest.java` (ACCEPTED / DUPLICATE / REJECTED_INVALID_VA / REJECTED_CHARGE_CLOSED / missing-field / negative-amount cases). Idempotency, VA resolution, charge terminal-status check, and all fallback-free field validation are implemented exactly as specified. Gap against this section's literal text: the per-charge-type amount check ("CLOSED: must equal remaining; INSTALLMENT: must not exceed remaining") is not implemented — `REJECTED_INVALID_AMOUNT` is declared in `PaymentOutcome` and already mapped to HTTP 400 in both callback controllers, but no code path produces it; the only amount check on the request thread is "> 0" as part of general field validation.

On the HTTP request thread, before any Kafka append:

1. **Idempotency**: look up `bankCode + "_" + bankReference` in `idempotency-store` (interactive
   query, same pattern as `InquiryApplicationService`). Hit → return the duplicate-ACK response
   (HTTP 200 with a `DUPLICATE` status and the original event reference). Do not append a second
   event.
2. **VA resolution**: look up `bankCode + "_" + vaNumber` in `va-registry-store`. Miss → HTTP 404
   `INVALID_VA`. The client must NOT supply `chargeId`; the gateway resolves it. Remove `chargeId`
   from the callback request DTO.
3. **Invariant check**: load the charge from `charge-state-store`. CLOSED/fully-paid charge →
   HTTP 400 `CHARGE_ALREADY_CLOSED`. Amount checks per charge type (CLOSED: must equal remaining;
   INSTALLMENT: must not exceed remaining; OPEN: any positive amount) → HTTP 400 `INVALID_AMOUNT`.
4. Only then append `PaymentReceivedEvent` (with `send(...).get()` retained — the ack must be
   durable before the 200).

Remove every fallback default in `PaymentApplicationService`. Missing/blank `vaNumber`,
`bankCode`, `bankReference`, `amount`, `paymentTimestamp` → HTTP 400 with an explicit field error.
No `"UNKNOWN_CHARGE"`, no `"GENERIC_BANK"`, no substituted timestamps.

Known and accepted residual race (same as the relational gateway's documented residual): two
callbacks passing pre-validation before the topology applies the first event. That is exactly what
the double-settlement detection in G2 must catch — never silently absorb.

### [DONE] G2. Enforce invariants in the topology and emit the detection event

Implemented in `src/main/java/com/artivisi/paymentgateway/streams/PaymentGatewayStreamsTopology.java` (`PaymentEventProcessor`, `ChargeHydrationProcessor`) and `src/main/java/com/artivisi/paymentgateway/domain/event/DoubleSettlementDetectedEvent.java`. The double-settlement race is proven end-to-end by `BankCallbackControllerIntegrationTest.testPaymentCallback_ChargeAlreadyClosed_Negative` (asserts `is_double_settlement=true` lands in the projection) and `testPaymentCallback_ConcurrentFullSettlement_ExactlyOneApplied` (two concurrent full payments against one CLOSED charge, asserts `cumulativePaid` settles exactly once). Note: sibling-VA retirement is achieved because every sibling VA resolves to the same `chargeId` in `va-registry-store` and that charge's status flips to `PAID`, not via separate per-VA cancellation records — a later inquiry/payment against any sibling observes the same terminal charge status.

In `PaymentEventProcessor` (the single-writer per partition key, so this is the authoritative
serialization point):

- Re-check idempotency against `idempotency-store` before applying. Duplicate → skip apply, emit
  nothing new.
- Re-check the charge state. If the payment overfills a CLOSED/INSTALLMENT charge (the
  pre-validation race), emit `DoubleSettlementDetectedEvent` to `payment-events` (or a dedicated
  topic) instead of applying it, so it lands in the projection flagged `is_double_settlement=true`
  for out-of-band refund. Never `max(ZERO)`-floor an overpayment.
- On full settlement: mark the charge CLOSED in `charge-state-store` AND retire all sibling
  entries in `va-registry-store` (requires the charge record to carry its sibling VA list, or a
  reverse index). A retired VA's next inquiry returns `INVALID_VA`.
- Track `cumulativePaid` explicitly instead of destructively rewriting `totalAmount` — the current
  code loses the original bill amount, which the inquiry response needs.
- Drop per-record `log.info` in all processors; log errors only.

### [DONE] G3. Fix the projection sink so lag is measurable and bounded

Implemented in `src/main/java/com/artivisi/paymentgateway/config/KafkaTopicConfig.java` (explicit `NewTopic` beans, partition count from `app.kafka.partitions`, default 6), `src/main/java/com/artivisi/paymentgateway/projection/sink/PostgresProjectionSink.java` (batch `@KafkaListener`, `saveAll`), `src/main/java/com/artivisi/paymentgateway/projection/repository/PaymentProjectionRepository.java` (`findByBankReferenceIn` for batch idempotency), and the new `GET /api/admin/debug/projection-lag` endpoint (`src/main/java/com/artivisi/paymentgateway/web/api/ProjectionLagController.java`). `spring.jpa.hibernate.ddl-auto: update` was removed from production `application.yml` (Flyway is the sole schema authority). No load-bearing test/measurement exists yet for the lag figure itself — see the "risksOrFollowups" of the `projection-sink` and `k6-suite` stages: `num.stream.threads` is still hardcoded independent of `app.kafka.partitions`, and no benchmark has been run against the new partitioning to read the endpoint under load.

- Create topics explicitly (`KafkaAdmin`/`NewTopic` beans or compose init) with the partition
  count the README documents (6 for the starter profile). Set
  `spring.kafka.listener.concurrency` to match.
- Batch the sink: `@KafkaListener(batch = true)` + `saveAll`, or aggregate per poll. Per-event
  existence-check + 4 JPA ops cannot keep up and invalidates any "projection lag 10–100 ms" claim.
- Expose projection lag (consumer group lag or a `projected_at - event timestamp` gauge). The
  benchmark report must state measured lag, and the audit must not run until lag is zero.

### [DONE] G4. Make the benchmark workloads identical

Adapter implemented (Preferred option) in `src/main/java/com/artivisi/paymentgateway/web/api/bsi/` (`BsiAdapterController`, `BsiRequest`, `BsiResponse`, `BsiResponseCode`, `BsiChecksum`) and `src/main/java/com/artivisi/paymentgateway/config/BankSecretProperties.java`, mounted at `/api/bank/bsi`. Covered by `src/test/java/com/artivisi/paymentgateway/web/api/BsiAdapterControllerIntegrationTest.java`. Workload scripts made identical in `scenarios/suite-bsi.js` / `scenarios/suite-rdbms.js` (G6 below). Not yet done: an actual dual-system run with a real (non-`NULL`) escrow secret configured on both apps — this is a benchmark-execution step, not a code change, and remains open per the `k6-suite` stage's risksOrFollowups.

Pick ONE of these and use it for both systems:

- **Preferred**: implement the BSI proprietary adapter (`/api/bank/bsi`, checksum verification
  against the escrow secret, same response-code mapping) in evtsrc, and run
  `scenarios/suite-rdbms.js` unchanged against both. This benchmarks the production protocol on
  both sides. Seed both systems with the same escrow secret (a real value, not NULL).
- Alternative: implement the generic endpoint in `payment-gateway` with equivalent semantics.
  Weaker, because it benchmarks a synthetic path neither system runs in production.

Either way:

- Same seed dataset, same VA→charge topology on both sides, including sibling-VA contention
  (several VAs of one charge in the hot set) — that is the scenario the whole single-debt design
  exists for.
- Remove bank codes that have no adapter (BCA/BNI/BRI) or implement them; do not benchmark
  fictional banks.
- The callback payload must not contain `chargeId` on either side.

### G5. Measure what matters, not `http_req_failed`

- Tag requests in k6 by expected outcome and check the **in-body** response code
  (`responseCode`/`status`), not just HTTP status. Report accepted-payment TPS separately from
  reject/duplicate TPS. "0.00% error rate" over a workload that is mostly rejections is not a
  result.
- Design the workload so a known fraction is real accepted payments for the whole run (OPEN and
  INSTALLMENT charges sized so they never fill, plus a stream of fresh CLOSED charges if create-VA
  throughput is part of the story).
- Include duplicate-reference injections (replay a % of `bankReference`s) and assert duplicate-ACK
  semantics under load on both systems.
- Keep the ramping-arrival-rate profile, but state VU limits and report dropped iterations
  prominently; a VU-limited run is a load-generator result, not a server ceiling.

### [DONE — tooling only, re-benchmark itself NOT done] G6. Make results traceable

Script/tooling portions implemented in `scenarios/run-benchmark.sh` (fail-loud on missing `RUN_ID`/`BSI_SHARED_SECRET`, wraps `k6 run` with `--summary-export` and `--out json`), `scenarios/suite-bsi.js` (`RUN_ID`-prefixed `bankReference`/`idTransaksi` for run isolation, `payment_outcomes` outcome metric), and `scenarios/results/README.md` (artifact-naming and traceability contract). NOT done: no run metadata capture (git SHA, docker images, JVM flags, partition counts, dataset checksum) exists in any script; `scenarios/results/` contains only the README, no committed run artifacts; and the full-ramp, N≥2-measured-runs re-benchmark itself has not been executed — only a syntax/init check and a small manual smoke test (`k6 --vus 2/3`, 10–30 iterations) were run during development, per the `k6-suite` stage's own summary.

- Run k6 with `--summary-export` (and ideally `--out json`), commit the raw summaries under
  `scenarios/results/<date>-<system>-<scenario>.json`, and generate every table in
  `perf_benchmark_report.md` from those files. No number appears in the report without a
  committed artifact behind it.
- Record run metadata: git SHA of each system, docker images, JVM flags, partition counts,
  dataset checksum.
- One warm-up run (discarded) + N≥2 measured runs per system, reported individually. Never mix
  values from different runs into one column.

### [DONE] G7. Fix the audit so it checks business invariants

Implemented in `scenarios/verify-correctness.py`: PRIMARY CHECK 1 cross-checks the k6 `payment_outcomes` ground truth (from `--out json`) against the payment table (every accepted `bankReference` has exactly one row, every rejected one has zero); PRIMARY CHECK 2 (evtsrc only) cross-checks RocksDB (`GET /api/v1/charges/{id}`) against the Postgres projection for `cumulativePaid`/status agreement. The old tautological `paid_amount == SUM(payments)` check is retained as a demoted, non-exit-code-affecting "SECONDARY / SINK-CONSISTENCY CHECK". Validated with live smoke tests against real Postgres containers and a real k6-generated JSON-lines sample; not yet run against a full end-to-end evtsrc + Kafka run (that is part of the deferred re-benchmark, not this script's own correctness).

`verify-correctness.py` (or a replacement) must assert, per system, after projection lag reaches
zero:

- Every ACCEPTED callback (per the k6 outcome log) has exactly one recorded payment; every
  REJECTED/DUPLICATE callback has none. This requires k6 to log `bankReference` + outcome (e.g.
  via `--out json` or a per-VU accumulator flushed in `handleSummary`).
- A CLOSED charge has at most one settling payment; siblings retired after settlement (inquiry on
  a sibling returns not-found/closed).
- INSTALLMENT: `SUM(payments) <= amount`; OPEN: unbounded but every payment traceable to an
  accepted callback.
- Double-settlement flags equal the number of injected/raced overpayments — checked against the
  k6 outcome log, not assumed zero.
- For evtsrc, audit BOTH the RocksDB state (via an admin/interactive-query endpoint) and the
  Postgres projection, and assert they agree.

### G8. Correct the published documents

- `scenarios/perf_benchmark_report.md`: delete §8.1–8.3 (untraceable) and the "0 double
  settlements / 100% PASS" claims for evtsrc; replace after re-measurement per G6.
- `README.md`: fix §3.5 (describe the implemented hot path only), §5.4 (the single-suite claim),
  the seed-data counts (17 VAs today), and the partition claims to match actual topic creation.
- Reconcile ramp profile descriptions with `startRate: 50` in the scripts.

### Post-implementation verification (2026-07-28)

The 7-stage implementation above self-reported `compileStatus: PASS` at every stage, and the
"Verify" stage reported a captured pre-edit run of 19/19 green — but its own final confirmation
run (after adding the acceptance-criteria tests) never completed (crowded out by an unrelated
concurrent build on the same machine) and was never re-checked. Trust but verify: an independent
`mvn test` run immediately afterward found **3 real failures**, none of which were bugs in the
production validation/topology logic itself — all three were races in test setup exposed by that
logic now actually depending on asynchronous RocksDB hydration:

1. **VA-hydration race**: `TestSupport.awaitChargeStatus` only polled the charge's own status
   (from `charge-events`); it did not wait for the sibling VA to be indexed in
   `va-registry-store` (from the independently-lagging `va-events` stream). Tests fired a payment
   the instant the charge showed `ACTIVE` and intermittently hit a real, correctly-returned
   `REJECTED_INVALID_VA` on a VA that resolved moments later. Fixed by adding
   `TestSupport.awaitVaResolvable` (polls `POST /api/v1/inquiry`) and calling it after
   `awaitChargeStatus` in every test that creates a charge+VA and immediately pays it
   (`BankCallbackControllerIntegrationTest`, `BsiAdapterControllerIntegrationTest`,
   `FinancialCorrectnessIntegrationTest`).
2. **Stale RocksDB state across separate test JVM runs**: `spring.kafka.streams.properties.state.dir`
   defaulted to the production path `./target/rocksdb`, which is not cleaned between separate
   `mvn test` invocations (no `mvn clean`) even though `@EmbeddedKafka` starts a *fresh* broker
   with a new topic/cluster identity every JVM run. Re-running tests without a clean reliably
   desynced the topology (a payment would be pre-validation-ACCEPTED and durably appended to
   Kafka, yet never applied — `cumulativePaid` stuck at 0 indefinitely). Fixed in
   `AbstractIntegrationTest` by pointing `state.dir` at a fresh `Files.createTempDirectory(...)`
   per JVM run, and by pinning `app.kafka.partitions=1` for tests to match `@EmbeddedKafka`'s
   pre-created topics (removing a partition-count race between `KafkaTopicConfig`'s `NewTopic`
   beans and the embedded broker's own topic pre-creation).
3. **Idempotency pre-validation can race the topology's async write**: `PaymentApplicationService`
   checks `idempotency-store` on the request thread, but that store is only *written* by
   `PaymentEventProcessor` after it consumes the event from Kafka — pre-validation holds a
   read-only interactive query, it cannot write. A retry fired with zero delay after the first
   call's `ACCEPTED` response can race ahead of that write and also come back `ACCEPTED` instead
   of `DUPLICATE`. **The ledger is never double-counted** — the topology's own idempotency
   re-check still catches it before a second apply — but the HTTP-level acknowledgment can be
   momentarily inaccurate for a back-to-back-with-no-delay retry. This is now a documented,
   accepted residual window (same category as the double-settlement race), narrowed by
   `TestSupport.awaitCumulativePaidAtLeast`, which real bank retry logic (timeout-then-retry, not
   zero-delay) will not normally hit.

Also hardened `BsiAdapterController.handle()` to catch all exceptions and return a BSI-shaped
`ERROR` response instead of leaking Spring's generic (non-BSI) 500 error body — the bank's parser
only understands the wire format, not a Spring stack-trace JSON body.

After these fixes: `mvn test` passes 21/21 across **7 consecutive full runs, with and without a
prior `mvn clean`**, with no other changes in between. This is the actual, verified state of the
build as of this note — treat any `[DONE]` tag above as scoped to what its own paragraph says,
not as a claim that every test that touches that area is race-free; this section is what closed
the gap between "the stage agent said PASS" and "an independent, repeated run says PASS."

### Acceptance criteria for the re-benchmark

1. Both systems benchmarked through the same adapter protocol, same seed, same script, same
   machine, sequential runs, raw k6 exports committed.
2. evtsrc rejects invalid/duplicate/closed-charge callbacks at the HTTP layer per G1 (proven by
   integration tests: valid → 200, duplicate → 200-duplicate, closed → 400, unknown VA → 404,
   missing field → 400).
3. Double-settlement race demonstrably detected and flagged (a test that fires two concurrent
   full payments at one CLOSED charge and finds exactly one applied + one flagged).
4. Audit passes per G7 on both systems, run at zero projection lag, cross-checked against the
   load generator's outcome log.
5. Report states accepted-payment TPS and reject TPS separately, with knee/saturation analysis
   derived from the exported time series, per system.
