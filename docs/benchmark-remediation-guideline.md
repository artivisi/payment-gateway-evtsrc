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

Implemented in `scenarios/verify-correctness.py`: PRIMARY CHECK 1 cross-checks the k6 `payment_outcomes` ground truth (from `--out json`) against the payment table (every accepted `bankReference` has exactly one row, every rejected one has zero, and a double-settlement-class rejection has zero rows OR exactly one flagged row -- the exact expectation differs by system, see the "Third gap" section below); PRIMARY CHECK 2 (evtsrc only) cross-checks RocksDB (`GET /api/v1/charges/{id}`) against the Postgres projection for `cumulativePaid`/status agreement. The old tautological `paid_amount == SUM(payments)` check is retained as a demoted, non-exit-code-affecting "SECONDARY / SINK-CONSISTENCY CHECK". Run against four full end-to-end benchmark runs (2 per system, `scenarios/perf_benchmark_report.md`) -- all four passed cleanly. Also added `--target evtsrc|rdbms` after auto-detection was found to silently pick the wrong database when both systems' containers run simultaneously (exactly what a real side-by-side comparison does).

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

### Second gap: `mvn test` passing does not mean the packaged app runs correctly (2026-07-28)

Actually running the corrected implementation as a real deployed app (fresh Postgres + Kafka
containers, packaged jar, real BSI checksum traffic via k6 — see
`scenarios/perf_benchmark_report.md` §8) surfaced two more bugs that 21/21 green tests never could,
because both are specifically about the gap between the test harness and a real deployment:

1. **Flyway never migrated anything.** `pom.xml` declared bare `flyway-core` +
   `flyway-database-postgresql` but not the Spring Boot 4 starter
   (`org.springframework.boot:spring-boot-flyway`) that actually wires
   `FlywayAutoConfiguration` — Spring Boot 4 split autoconfiguration into per-module starters, and a
   library on the classpath without its starter does not autoconfigure. Result: zero tables ever
   created, zero errors logged, and every projection-sink batch silently discarded by Spring
   Kafka's default `FallbackBatchErrorHandler` from the app's very first request. `mvn test` never
   caught this because `AbstractIntegrationTest` explicitly disables Flyway for tests and uses
   Hibernate `ddl-auto=update` instead (a deliberate, reasonable test-speed choice — but it means no
   test in this repo has ever exercised the production migration path). Fixed by adding the
   starter dependency.
2. **The audit script itself had a false-positive bug.** `verify-correctness.py`'s original
   k6-vs-database cross-check assumed every non-accepted outcome must have zero payment rows — but
   a payment rejected because the charge is already `PAID` is *supposed* to produce exactly one row
   flagged `is_double_settlement=true` (G2's entire point). The first live run "failed" the audit
   purely from this wrong assumption in the checking code, not a defect in the system under test.
   Fixed by adding a third outcome bucket that expects exactly one flagged row.

Neither of these would have been caught without actually running the packaged application against
real infrastructure and real load — a reminder that a green test suite bounds what's broken, it
doesn't prove nothing is.

### Third gap: OPEN charges were incorrectly capped, sourced from a wrong domain-model assumption (2026-07-28)

The RDBMS-side comparison run (`scenarios/perf_benchmark_report.md` §8) initially reported evtsrc
accepting only 209 payments against the RDBMS baseline's 28,953, framed as "a genuine behavioral
difference, not a bug in either system." That framing was wrong. The actual bug: `payment-gateway/CLAUDE.md`'s
charge lifecycle section grouped OPEN with INSTALLMENT under one closing rule
("when `cumulativePaid ≥ amount` the charge is `PAID`") — but the RDBMS baseline's own
`PaymentApplicationService.applyOpen()` is explicit: "Persistent, free amount, repeated payments:
accumulate, never auto-complete, keep siblings open." OPEN is a standing/always-active account
(e.g. a donation VA) with no cap; INSTALLMENT is the type that enforces a target amount. Trusting
the (wrong) grouped documentation, both `PaymentGatewayStreamsTopology.PaymentEventProcessor` and
`PostgresProjectionSink` implemented OPEN identically to CLOSED/INSTALLMENT — capping at
`totalAmount` and marking the charge terminal. Fixed in both write paths (check `chargeType`, never
transition an OPEN charge to `PAID`/`FULLY_PAID`); `payment-gateway/CLAUDE.md` corrected to
separate the two rules (that repo's own fix — not committed by this session, since it was outside
evtsrc's scope; it needs a commit there to persist). With both fixed, evtsrc's accepted-payment
count on a fresh run (28,897) closely tracks the RDBMS baseline's (28,953).

A third reminder alongside the first two: this was caught by actually comparing real numbers
between two independently-implemented systems, not by testing either one in isolation. A single
system's tests and even its own live-load run can pass cleanly while quietly encoding a
misunderstanding of its own domain model — it takes a second, independent implementation
disagreeing on the numbers to surface it.

### Fourth gap: a real financial-correctness defect, found only by testing against a fresh database twice (2026-07-29)

Every run behind the Third gap's fix, and behind acceptance criterion 4 below, was against a
database that had accumulated state from a prior run on the same day (RDBMS's second run) or was
otherwise not repeated to check reproducibility. Re-running both systems from a completely empty
database (schema dropped and recreated, Kafka topics and the local RocksDB `target/rocksdb`
directory wiped for evtsrc) surfaced two things a same-day, same-database pairing had not:

1. **evtsrc saturates well before 2,000 TPS; RDBMS does not**, on the same shared hardware, same
   ramp. RDBMS: p99 51ms, 156 VUs needed. evtsrc: p99 3.22s and 1.16s across two independently-reset
   runs, 1,200+ VUs needed, tail latency still climbing at ramp-down rather than recovering. See
   `scenarios/perf_benchmark_report.md` §3.
2. **evtsrc's financial-correctness audit failed in both fresh runs** — a single `bankReference` was
   recorded twice (once accepted, once flagged as a double settlement) for the same charge, once per
   run, both times on a charge that had just been created. RDBMS's audit passed cleanly in its one
   run. The exact mechanism was not conclusively identified: ruled out a k6-side duplicate
   `idTransaksi`, a Kafka consumer-group rebalance (none logged), and plain redelivery of the same
   Kafka record (the topology's own duplicate-skip guard never fired). All three of evtsrc's Kafka
   Streams state stores use `Stores.persistentKeyValueStore(...)` with no `.withCachingDisabled()`,
   so the default record cache is active on all of them — a plausible contributor to interactive-query
   staleness, but not confirmed as the root cause. See `scenarios/perf_benchmark_report.md` §4 for
   what was ruled in/out and what a real root-cause pass would need (DEBUG-level Streams logging, or
   a rerun with caching explicitly disabled).

The defect was run twice specifically because the first occurrence looked like it could have been a
fluke (one anomalous row out of ~78,000 requests). It reproduced identically in kind (not in exact
value) on the second independently-reset run, which is what turned "possibly noise" into "a real,
reproducible characteristic of this design under saturation." A single run — however clean — would
not have distinguished those two possibilities.

### Fifth gap: the Fourth gap's findings did not survive a controlled re-run (2026-07-29, afternoon)

The Fourth gap above (saturation ceiling + a double-recorded payment, reproduced in two runs the
same morning) was itself measured on a machine the operator later discovered had a hanging OrbStack
VM, and which this session separately observed running another Testcontainers-based test session's
containers during setup for the very next benchmark attempt. The operator restarted the machine,
explicitly citing "a severe resource hogging problem," and asked for the benchmark to be redone.

Re-run with the same scripts, same seed data, same six BSI VA/amount pairs, same ramp profile, on
the freshly-restarted machine — with an explicit contamination check (`docker ps -a` + `uptime`,
repeated until no unrelated container churn appeared and load average had settled) added before
each load-generation phase for the first time — evtsrc's p99 came back at 8.5ms and 9.4ms across two
runs (vs. 1.16s–3.22s that morning), flat across every ramp stage, and both runs' correctness audits
passed with zero mismatches on both checks (vs. one double-recorded payment per run that morning).
RDBMS's own run was mildly noisier than its own best prior run (p99 112ms vs. 50ms) but showed no
saturation shape either.

**The saturation finding does not currently hold** — it was a real measurement of a real but
contaminated environment, not a fabrication and not (as far as this pass can tell) a latent
performance ceiling. `scenarios/perf_benchmark_report.md` was rewritten to lead with this retraction
rather than quietly replacing the numbers, precisely because the old numbers were reported with the
same confidence and the same audit-passing rigor the Third gap's fix was — a green audit and a
clean-looking run are necessary but not sufficient; the actual machine state at the time of the run
is a variable this project had not been controlling for, and evidently needed to.

**The financial-correctness defect is a different matter — see the Sixth gap below.** When the
operator asked "the app should not do double payment however low the resource is, correct?", that
was the right challenge to the framing above: contamination explaining why the defect *manifested*
that morning is not the same as the defect not being real. Retracting it outright, as the paragraph
above originally did, was a mistake corrected within the same session — see below for what the
defect actually was and its fix.

A reminder alongside the first four: this project's own stated purpose is catching exactly this
class of error before it reaches a conclusion. Finding that its own most dramatic finding to date
was itself a contamination artifact, one benchmark cycle later, is uncomfortable but is the system
working as intended — better here than in a published comparison relied on for real decisions. And
per the Sixth gap, even that correction needed a correction: "unreproducible under a clean
environment" was quietly treated as "not a real bug," which does not follow.

### Sixth gap: the double-write defect was real, root-caused, and fixed — independent of load (2026-07-29, afternoon)

Prompted by the operator's challenge above, the Fourth gap's financial-correctness defect (a single
`bankReference` recorded as both an accepted payment and a flagged double settlement) was
investigated directly rather than left as "retracted, mechanism unknown." The root cause:
`PostgresProjectionSink` projects `PaymentReceivedEvent` and `DoubleSettlementDetectedEvent` through
two independent methods (`projectPaymentReceivedBatch` / `projectDoubleSettlementBatch`) that did
not check each other's work. `PaymentApplicationService`'s request-thread pre-validation can
optimistically accept a payment (charge not yet `PAID` at its read) that the Kafka Streams
topology — the actual single-writer authority — later finds was already settled by a racing
payment, and correctly emits a `DoubleSettlementDetectedEvent` for that same `bankReference` per
G2's design intent. `projectPaymentReceivedBatch` had already unconditionally inserted an "accepted"
row for the optimistic accept, and `projectDoubleSettlementBatch` unconditionally inserted a second,
contradicting row for the correction, instead of the correction retracting the first row in place.

This is a pure logic defect, not fundamentally a low-resource phenomenon: resource contention only
widens the timing window in which multiple concurrent requests can hit a brand-new `CLOSED` charge
before the first one closes it (widening how *often* the race is hit), it does not create the race.
That means it doesn't need real load, real Kafka timing, or a slow machine to reproduce or verify a
fix against — it can be driven directly and deterministically. `PostgresProjectionSinkTest` does
exactly that: it calls `PostgresProjectionSink.consumeDomainEvents(List)` directly with a
hand-crafted `PaymentReceivedEvent` followed by a `DoubleSettlementDetectedEvent` for the same
`bankReference`, with no Kafka, no concurrency, and no timing dependency at all. Verified against
the pre-fix code, it fails every time (`Expected size: 1 but was: 2`); against the fix, it passes
every time. This is stronger evidence than another load-test run would be — a load test's outcome
here was probabilistic (the defect hit roughly 1 in 26,000–27,000 accepted payments), so a clean run
would not have proven the fix, only failed to disprove it by chance.

Fixed in `PostgresProjectionSink.projectDoubleSettlementBatch`: it now looks up any existing payment
row for the incoming event's `(bankCode, bankReference)` and, if found, flips it to
`isDoubleSettlement=true` in place (and retracts its amount from the charge's `paidAmount`/status via
the new `correctChargesForRetractedPayments`) instead of inserting a second, contradicting row. The
normal case — a `bankReference` that was always going to be rejected and never optimistically
accepted — is unaffected (`PostgresProjectionSinkTest`'s second test covers it).

The lesson compounds on the Fifth gap's: "it didn't reproduce under a clean environment" is evidence
about *how often* a defect manifests, not evidence about whether it's real. A correctness defect in
a payment gateway that only shows up "sometimes, under load" is still a correctness defect a payment
gateway cannot have — production systems experience exactly that kind of contention (GC pauses, CPU
spikes, slow disks) as a matter of course, not as an edge case to discount.

### Seventh gap: the Sixth gap's fix was itself a downstream patch — the actual root cause was fixed by rearchitecting the write path (2026-07-29, evening)

The Sixth gap's fix (`PostgresProjectionSink` retracting a contradicting row in place) stopped the
observable symptom — the database record — from staying wrong. It did not, and structurally could
not, fix the deeper problem: the request thread could still tell the bank "ACCEPTED" over HTTP
*before* Kafka Streams' authoritative re-check ran, so a client-facing response could still disagree
with the eventual truth even after that fix shipped. This was caught the same way the Sixth gap's own
correction was — the operator asking a direct question ("we can work around it by doing several
queries inside RocksDB instead of simply recording payment... all within a single RocksDB
transaction") that turned out to describe the actually-correct fix.

**Root cause, precisely stated**: the write path had two participants with a gap between them —
`PaymentApplicationService` read RocksDB state on the request thread (read-only interactive query),
then published an event; `PaymentGatewayStreamsTopology`'s `PaymentEventProcessor`, running on a
separate Kafka Streams thread, was the actual authority that decided whether the payment counted.
Whatever happened in that gap was invisible to the client, who had already been told the request
thread's optimistic answer.

**The fix**: `ChargeSettlementStore`, a RocksDB `TransactionDB` owned directly by the application —
not by Kafka Streams. VA resolution, the charge terminal-status check, and the balance update now
happen as one atomic transaction, on the request thread, using `getForUpdate` for the same row-lock
semantics `SELECT FOR UPDATE` gives a relational transaction. There is no gap left for a second
request to land in, and nothing left to re-decide later — whatever the transaction returns is what
the bank is told, and it is already final. Kafka Streams' three state stores
(`charge-state-store`, `va-registry-store`, `idempotency-store`) and `PaymentEventProcessor`'s
decision logic were removed entirely once nothing read or wrote them anymore — keeping them as an
unused mirror would have quietly recreated the exact "two places that can disagree" pattern this
project keeps finding and fixing. Kafka is unchanged in one respect: it still broadcasts the
now-already-decided outcome to `PostgresProjectionSink`'s read model.

**Scale-out caveat, addressed rather than deferred**: a directly-owned local RocksDB store only
gives this guarantee if requests for a given charge consistently reach the instance that owns it.
The fix for that (not yet built, since this remains a single-instance deployment) is consistent
partition keying — chargeId maps to the same partition a Kafka topic would use — plus Kafka
Streams' own `queryMetadataForKey()` to route a request to the owning instance, exactly the pattern
Kafka Streams' own documentation recommends for interactive queries. For genuine multi-site
durability on top of that, replication factor 3 with `acks=all` and `min.insync.replicas=2` gives
RPO=0 at the cost of write latency bound by inter-site RTT — the same physics tax synchronous
multi-site RDBMS replication pays, not a Kafka-specific weakness.

**Verification, two ways**:

1. `ConcurrentPaymentSettlementIntegrationTest` fires 50 real concurrent threads at one
   freshly-created `CLOSED` charge — no sequential event replay, no mocked timing. Exactly one
   thread is accepted, the other 49 are correctly flagged, and `cumulativePaid` never double-counts.
   This is a stronger proof than `PostgresProjectionSinkTest`'s sequential-event-replay test (which
   proved the Sixth gap's symptom-level fix): it proves the race is closed under genuine thread
   contention, not just that a specific event ordering is handled correctly.
2. The full benchmark was re-run against the rearchitected build — two independent fresh runs, same
   k6 scripts, same protocol. p99 came back at 9.96ms and 9.85ms (statistically indistinguishable
   from the Fifth gap's clean numbers, 8.5–9.4ms), both audits passed with zero mismatches on both
   checks, confirming the fix cost nothing in the metrics that matter and closed the correctness gap
   at its source rather than downstream of it.

A reminder that compounds on the last two: the Sixth gap's own text said "didn't reproduce under a
clean environment is evidence of rarity, not unreality" — and that principle applied one level
deeper than expected. The Sixth gap's fix genuinely worked (verified, still true), but "the symptom
stopped occurring" was not the same claim as "the root cause is gone," and it took a third round of
the operator asking a pointed technical question to get from one to the other.

### Eighth gap: `ChargeSettlementStore` tracked only charge-level status, not per-VA status (2026-07-29, night)

Raised while updating the blog write-up of the Seventh gap: the numbered description of
`applyPayment`'s atomic transaction said "look up the charge, find all sibling VAs, mark them all
paid" — but that is not what `payment-gateway`'s (RDBMS) reference behavior does, and not what a
correct implementation should do. Only the VA that actually received the payment should become
PAID; siblings that never received anything should become CANCELLED, exactly as
`PaymentApplicationService.settleAndCancelSiblings` does in the RDBMS repo. Checking the actual
`ChargeSettlementStore` code confirmed the gap was real, and deeper than wording: the store never
tracked per-VA status at all, only a single charge-level status field, and `InquiryApplicationService`
didn't check status of any kind — an inquiry against a VA belonging to an already-PAID or -CANCELLED
charge still returned `SUCCESS` with the charge's `totalAmount`, contradicting CLAUDE.md's "a
cancelled VA's next inquiry returns NOT_FOUND."

**The fix**: VA records now carry their own `ACTIVE`/`PAID`/`CANCELLED` status, indexed under a new
`va_by_charge:` prefix key so the settlement transaction can walk a charge's siblings without a
full store scan. When a payment settles a charge (CLOSED, or an INSTALLMENT reaching its total), the
same atomic transaction marks the paying VA `PAID` and every other still-`ACTIVE` sibling
`CANCELLED` — mirroring `settleAndCancelSiblings` exactly. `InquiryApplicationService` now checks
both the VA's own status and its charge's status, so a cancelled sibling (or the now-PAID VA itself)
correctly returns `INVALID_VA` / HTTP 404 on its next inquiry.

Caught and fixed a regression while wiring this up: `applyPayment`'s own VA lookup still assumed
`registerVa`'s old raw-chargeId-string value format after VA records were switched to JSON — this
broke every payment and inquiry call outright, surfaced immediately by the existing test suite
(`BankCallbackControllerIntegrationTest`, `ConcurrentPaymentSettlementIntegrationTest`) going from
all-passing to failing everywhere. Fixed by parsing the VA record's `chargeId` field instead of
treating the raw bytes as the chargeId.

**Verification**: a new integration test
(`testPaymentCallback_ClosedChargeSettlement_PaidVaAndCancelledSiblingBothStopResolvingOnInquiry`)
creates a CLOSED charge with two sibling VAs, settles it via one, then asserts both the paying VA
and the untouched sibling return HTTP 404 on `/api/v1/inquiry` afterward. Confirmed live against a
running instance under full benchmark load, not just the test suite, by inquiring a settled charge's
VA directly: `{"status":"INVALID_VA","message":"Virtual account is no longer active (status
PAID)"}`, HTTP 404.

The full test suite (25 tests, including the 50-thread `ConcurrentPaymentSettlementIntegrationTest`)
passed after the fix. The benchmark was re-run twice, fresh environment each time (the first attempt
at the second run was contaminated by an unrelated project's Maven test suite running concurrently
on the same machine, mid-run VU count briefly exceeding pre-allocation and p99 spiking to 70ms —
discarded and re-run rather than reported, per this project's own standing rule about verifying the
environment before trusting a number):

| Metric | Run 1 (`vastatus1`) | Run 2 (`vastatus2`) | Seventh gap's runs (for comparison) |
|---|---|---|---|
| Run ID | `20260729181245` | `20260729201858` | `20260729162811` / `20260729163104` |
| p99 latency | 9.19ms | 8.65ms | 9.96ms / 9.85ms |
| Peak VUs (of 100 pre-allocated) | 46 | 31 | 32 / 33 |
| Correctness audit | PASS, 0 mismatches (both checks) | PASS, 0 mismatches (both checks) | PASS, 0 mismatches |

Statistically indistinguishable from the Seventh gap's own numbers — tracking per-VA status and
walking siblings inside the transaction added real work (a prefix scan, a `getForUpdate`, and a
JSON parse/serialize per sibling) but cost nothing measurable, because the benchmark's own seed data
(`scenarios/suite-bsi.js`) only ever registers a single BSI sibling per charge. This benchmark
therefore does not exercise the cost of cancelling multiple real siblings in one settlement — a
multi-sibling pay-via-any-bank charge would walk more than one VA per settlement, and that path
remains unmeasured here.

### Acceptance criteria for the re-benchmark

1. [DONE] Both systems benchmarked through the same adapter protocol, same seed, same script, same
   machine, sequential runs, raw k6 exports committed. Two runs per system, not one --
   `scenarios/perf_benchmark_report.md`, artifacts under `scenarios/results/`.
2. [DONE] evtsrc rejects invalid/duplicate/closed-charge callbacks at the HTTP layer per G1 (proven by
   integration tests: valid → 200, duplicate → 200-duplicate, closed → 400, unknown VA → 404,
   missing field → 400).
3. [DONE] Double-settlement race demonstrably detected and flagged (a test that fires two concurrent
   full payments at one CLOSED charge and finds exactly one applied + one flagged).
4. [DONE] Audit passes per G7 on both systems, cross-checked against the load generator's outcome
   log -- all four 2026-07-28 runs (2 per system). Projection lag was not explicitly polled to zero
   before auditing; the audits passing cleanly (no missing rows) is strong circumstantial evidence it
   had drained, not a directly measured guarantee. Superseded by the Fourth gap above: re-run against
   a fresh database on 2026-07-29, the audit found a real defect in evtsrc (not the RDBMS baseline)
   in both of two independent runs. "Audit passes" was a property of that day's specific runs, not a
   proof the system has no residual correctness gap -- exactly the kind of thing only surfaces by
   re-testing under different conditions (fresh state) rather than trusting a prior green result.
5. [DONE] The report states accepted-payment counts and double-settlement-rejection counts
   separately per system per run (`scenarios/perf_benchmark_report.md` §5). A real knee/saturation
   analysis, time-bucketed from the actual `--out json` raw export by the new
   `scenarios/knee-analysis.py` (not asserted from memory the way the old fabricated report was), is
   in §4 of that report -- it precisely locates RDBMS run 2's degradation to the 1,000->2,000 TPS
   stage specifically, tail-only (median barely moves, p99 jumps from ~50ms to ~535ms), directly
   corroborating the lock-contention root cause in §3 with time-resolved evidence rather than just a
   whole-run average and a plausible story. This was initially skipped in favor of other work and
   only done after being asked "why didn't you" -- a reminder to finish a stated scope rather than
   quietly downgrading it to "partial" and moving on.
