# Performance Benchmark & Financial Correctness Report

**Project**: `payment-gateway-evtsrc` (Event-Sourced CQRS Payment Gateway)
**Date**: 2026-07-26
**Test Tool**: k6 v0.55+ (Grafana Labs)
**Test Script**: [`scenarios/suite.js`](file:///Users/endymuhardin/workspace/produk/payment-gateway-evtsrc/scenarios/suite.js)

> [!WARNING]
> **Sections 1–7 predate `docs/benchmark-remediation-guideline.md`'s remediation and are not comparable to a future re-run.** They were measured against `scenarios/suite.js` hitting a generic, unauthenticated `/api/v1/payments` endpoint with caller-supplied `chargeId` and no server-side pre-validation, idempotency check, or double-settlement detection (findings F1/F2/F5/F7). That script now throws immediately on execution rather than run. The numbers below are kept as historical record only — see "Re-benchmark Required" (§8) for what a valid re-run requires.

---

## 1. Hardware Under Test

| Component | Specification |
|---|---|
| **Machine** | Apple Mac17,3 |
| **CPU** | Apple M5 — 10 cores |
| **RAM** | 16 GB (17,179,869,184 bytes) |
| **OS** | macOS (arm64) |
| **JDK** | Eclipse Temurin 25.0.3+9 |
| **Spring Boot** | 4.1.0 |
| **Kafka** | `apache/kafka:latest` (KRaft mode, single broker, Docker container) |
| **PostgreSQL** | PostgreSQL 18 (Docker container) |
| **RocksDB** | `rocksdbjni` 10.10.1.1 (embedded, `./target/rocksdb`) |

> [!NOTE]
> All components (App JVM, Kafka broker, PostgreSQL) ran on the **same physical machine** sharing CPU and memory. Production deployments on dedicated infrastructure would yield significantly better results.

---

## 2. Test Scenario Configuration

| Parameter | Value |
|---|---|
| **Scenario Name** | `bank_callbacks` |
| **Executor** | `ramping-arrival-rate` |
| **Duration** | 90 seconds (4 stages) |
| **Ramp Profile** | 100 → 500 → 1,000 → 2,000 → 0 TPS |
| **Max VUs** | 2,000 |
| **Workload** | `POST /api/v1/payments` — Simulated multi-bank callbacks (MAYBANK, BSI, CIMB, BCA, BNI, BRI) |
| **Seed Data** | 8 charges, 19 sibling Virtual Accounts across 3 client institutions |

---

## 3. Benchmark Results — Run 1 (Warm JVM)

| Metric | Value |
|---|---|
| **Total Requests Completed** | 62,971 |
| **HTTP Error Rate** | **0.00%** (0 failures out of 62,971) |
| **Dropped Iterations** | 23,654 (request arrival rate exceeded VU capacity) |
| **Effective Avg Throughput** | 698.83 req/s |
| **Min Latency** | **1.24 ms** |
| **Median Latency (p50)** | 553.55 ms |
| **p90 Latency** | 2.61 s |
| **p95 Latency** | 2.89 s |
| **p99 Latency** | 3.30 s |
| **Max Latency** | 3.47 s |
| **Avg Latency** | 1.12 s |
| **Data Received** | 15 MB (162 kB/s) |
| **Data Sent** | 21 MB (237 kB/s) |

### Threshold Results

| Threshold | Condition | Result |
|---|---|---|
| `http_req_failed` | `rate < 0.01` | ✅ **PASS** (0.00%) |
| `http_req_duration` | `p(99) < 500ms` | ❌ **FAIL** (3.30s) — expected at saturation |

---

## 4. Benchmark Results — Run 2 (Cold Start / Accumulated State)

| Metric | Value |
|---|---|
| **Total Requests Completed** | 50,623 |
| **HTTP Error Rate** | **0.00%** (0 failures out of 50,623) |
| **Dropped Iterations** | 36,001 |
| **Effective Avg Throughput** | 560.55 req/s |
| **Min Latency** | **875 µs** |
| **Median Latency (p50)** | 2.66 s |
| **p90 Latency** | 3.49 s |
| **p95 Latency** | 3.69 s |
| **p99 Latency** | 3.86 s |
| **Max Latency** | 4.12 s |
| **Avg Latency** | 1.96 s |

> [!IMPORTANT]
> Run 2 experienced higher latencies due to accumulated RocksDB state and concurrent Kafka Streams topology processing from prior test runs on the same JVM instance.

---

## 5. Performance Curve Analysis — Knee & Saturation Point

```
  Throughput (TPS)
  2,000 ┤                              ╭──────── Saturation Plateau
        │                             ╱         (VU cap reached, latency > 2s)
  1,000 ┤                    ╭───────╯
        │                   ╱  ← Knee Point (~800–1,000 TPS)
    500 ┤            ╭─────╯     (latency transitions from <10ms to ~500ms)
        │           ╱
    100 ┤──────────╯  ← Linear Scaling Zone
        │               (sub-10ms latency, zero queue backlog)
      0 ┼────────────────────────────────────────────────
        0s     15s     30s     45s     60s     75s     90s
```

### Phase Analysis (from Run 1 data)

| Phase | Time Window | Target TPS | Observed TPS | Active VUs | Latency Profile | Behavior |
|---|---|---|---|---|---|---|
| **Linear Scaling** | 0s – 15s | 100 → 500 | 100 → 500 | 4 – 100 | **1.24 ms – 10 ms** | Throughput scales linearly. Near-zero queue depth. |
| **Knee Point** | 15s – 35s | 500 → 1,000 | 500 → 833 | 100 – 190 | **10 ms → 500 ms** | Tomcat worker threads start queuing. Median latency crosses 100ms. |
| **Saturation Plateau** | 35s – 75s | 1,000 → 2,000 | 700 – 900 | 200 – 2,000 | **500 ms → 3.3 s** | Max VU cap (2,000) hit at t=58s. Kafka producer and JVM GC contend for CPU on shared hardware. |
| **Ramp-Down** | 75s – 90s | 2,000 → 0 | Draining | 2,000 → 0 | Draining queued requests | In-flight requests complete. Zero failures. |

### Key Findings

1. **Operational Knee: ~800 – 1,000 TPS**
   - Below this point, response times are consistently sub-10ms.
   - Above this point, request processing queues build up and median latency transitions into the hundreds-of-milliseconds range.
   - **Recommendation**: For single-node deployment, size capacity planning at **~750 TPS sustained** to maintain sub-50ms p99.

2. **Saturation Point: ~2,000 TPS (VU-limited)**
   - The system reached the 2,000 VU cap at t=58s. k6 emitted: `"Insufficient VUs, reached 2000 active VUs and cannot initialize more"`.
   - Even at full saturation, **zero requests failed** — the system queued rather than rejected.
   - True throughput ceiling was not reached (VU-limited, not server-limited). Higher VU caps or dedicated hardware would push the ceiling further.

3. **Zero Failures Under All Load Levels**
   - Across both runs (113,594 total requests), the HTTP error rate was **0.00%**.
   - No connection pool exhaustion, no Kafka producer timeouts, no `504 Gateway Timeout`.

---

## 6. Financial Correctness & Invariant Audit

### Audit Tool
- CLI Script: [`scenarios/verify-correctness.py`](file:///Users/endymuhardin/workspace/produk/payment-gateway-evtsrc/scenarios/verify-correctness.py)
- Integration Test: [`FinancialCorrectnessIntegrationTest.java`](file:///Users/endymuhardin/workspace/produk/payment-gateway-evtsrc/src/test/java/com/artivisi/paymentgateway/migration/FinancialCorrectnessIntegrationTest.java)

### Post-Test Audit Results

```
==============================================================================
 FINANCIAL CORRECTNESS & INVARIANT AUDIT HARNESS
==============================================================================
Total Payments Recorded : 1,119
Total Paid Volume       : IDR 1,943,800,000.00
Double Settlements      : 0
------------------------------------------------------------------------------
✅ ALL FINANCIAL INVARIANTS AND BALANCE CONSTRAINTS VERIFIED PERFECTLY!
```

### Invariants Verified

| Invariant | Formula | Result |
|---|---|---|
| **Payment Sum Parity** | `charge.paid_amount == SUM(payment.amount WHERE charge_id)` | ✅ Matched for all charges |
| **Balance Accounting** | `charge.remaining_amount == MAX(0, total_amount - paid_amount)` | ✅ Matched for all charges |
| **Status Consistency** | `FULLY_PAID` when remaining=0, `PARTIALLY_PAID` when paid>0, `ACTIVE` otherwise | ✅ Consistent for all charges |
| **Idempotency** | `is_double_settlement` count == 0 | ✅ Zero double settlements |

### Integration Test Suite

```
Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS (21.545 s)
```

---

## 7. Summary & Recommendations

### Measured Performance Profile (Single-Node, Shared Hardware)

| Metric | Measured Value |
|---|---|
| **Min Latency** | **875 µs** |
| **Sustained Linear TPS (sub-10ms p99)** | ~750 TPS |
| **Operational Knee** | ~800 – 1,000 TPS |
| **Peak Throughput (VU-limited)** | ~2,000 TPS |
| **HTTP Error Rate** | **0.00%** across 113,594 requests |
| **Financial Correctness** | ✅ All invariants verified |

### Capacity Planning Recommendations

| Deployment Tier | Expected Sustained TPS | Hardware |
|---|---|---|
| **Single Node (Dev/Staging)** | ~750 TPS (sub-50ms p99) | 1 App + 1 Kafka + 1 PG (shared machine) |
| **Single Node (Dedicated)** | ~2,000+ TPS | 1 App (8 vCPU) + 1 Kafka + 1 PG (separate hosts) |
| **3-Node Production** | ~5,000+ TPS | 3 App Instances (12 partitions) + 3 Kafka brokers + PG HA cluster |

> [!TIP]
> The benchmark was VU-limited, not server-limited. The true throughput ceiling on dedicated hardware with higher VU caps and Kafka partition parallelism would be significantly higher.

---

## 8. Re-benchmark: evtsrc (measured) — RDBMS side pending

The previous version of this section ("Architectural Comparison & Analysis") was deleted. Per `docs/benchmark-remediation-guideline.md` finding F7, its head-to-head table was not traceable to any k6 run and internally contradicted the measured runs in §3–4 of this same file: request counts identical to another run to the digit (86,581), a min/max/p95 collage of values pulled from different runs (including a µs/s unit swap on the quoted "p95"), and seed-data VA counts (19 in §2, 23 in the deleted §8.1) that disagreed with each other and with the script's actual seed. None of that table's numbers came from a committed k6 artifact.

Sections 1–7 above measured a workload (`scenarios/suite.js`, generic `/api/v1/payments`, no pre-validation, no checksum, caller-supplied `chargeId`) that this remediation found exercised none of the gateway's real validation logic. `scenarios/suite.js` now throws immediately rather than run, so it cannot be used by mistake.

### 8.1 What this section reports

The evtsrc side has been re-run against the corrected implementation (real RocksDB pre-validation, authoritative topology re-check, double-settlement detection, real BSI checksum) with committed artifacts. **The RDBMS baseline side has not been re-run** — the sibling `payment-gateway` app's BSI escrow has no usable shared secret configured (its `client_secret` column is `NULL`, encrypted-at-rest via AES-256-GCM; setting a real one requires either admin credentials on the running instance or completing a fresh instance's bootstrap+TOTP enrollment flow, neither of which is something to script around unattended). This is a **single-system measurement, not a comparison** — the head-to-head table this guideline calls for is still open pending that decision. See `docs/benchmark-remediation-guideline.md`'s verification notes for the exact blocker.

### 8.2 Run conditions

| | |
|---|---|
| **Date** | 2026-07-28 |
| **Run ID** | `20260728133645` |
| **Target** | evtsrc app, freshly built jar, fresh Postgres 18 + Kafka (KRaft, 6 partitions) containers, `localhost:8081` |
| **Hardware** | Apple M5, 10-core, 16GB — **shared**, not dedicated: a separate project's own test suite (Oracle Testcontainers) and this machine's long-running RDBMS `payment-gateway` instance were both active on the same host during this run. Numbers below are if anything a conservative lower bound on dedicated hardware. |
| **Script** | `scenarios/suite-bsi.js` against the real `/api/bank/bsi` adapter, real SHA-1 checksum, `RUN_ID`/`BSI_SHARED_SECRET` set (no defaults) |
| **Artifacts** | `scenarios/results/2026-07-28-evtsrc-summary.json`, `scenarios/results/2026-07-28-evtsrc-raw.json.gz` (gzipped from k6's 342MB `--out json` for a committable size) |

Two bugs were found and fixed while producing this run (both now in the codebase, not just this report):

- **Flyway never ran.** Spring Boot 4 split autoconfiguration into per-module starters; `pom.xml` had bare `flyway-core`/`flyway-database-postgresql` but not `org.springframework.boot:spring-boot-flyway`, so Flyway silently migrated nothing — no tables, no error, no log line. Every projection-sink write was failing and being discarded by Spring Kafka's default batch error handler. This was invisible to `mvn test` because tests bypass Flyway entirely (`AbstractIntegrationTest` uses Hibernate `ddl-auto=update` for schema in tests) — it only surfaces when the packaged app is actually run against a real Postgres instance, which no prior step in this remediation had done. Fixed by adding the `spring-boot-flyway` starter.
- **The audit script's own classification bug.** `scenarios/verify-correctness.py` originally expected every non-`ACCEPTED` k6 outcome to have zero payment rows — but a payment rejected because the charge is already `PAID` is *supposed* to produce exactly one row flagged `is_double_settlement=true` (that is the entire point of G2: never silently absorb an overpayment). The first run against the fixed app "failed" the audit purely because of this false assumption. Fixed by adding a third classification bucket (`is_double_settlement_outcome`) that expects exactly one flagged row instead of zero.

### 8.3 Measured results (evtsrc, BSI protocol, real checksum)

| Metric | Value |
|---|---|
| **Total requests** | 86,614 |
| **HTTP error rate** | 0.00% |
| **Dropped iterations** | 23 (out of 86,625 scheduled) |
| **Effective throughput** | 960.4 req/s (ramp-averaged; peak target was 2,000 TPS) |
| **Min latency** | 367 µs |
| **Avg / median latency** | 4.26 ms / 3.94 ms |
| **p90 / p95 / p99** | 6.51 ms / 7.22 ms / **15.38 ms** |
| **Max latency** | 68.43 ms |
| **Peak VUs actually used** | 83 (of 101 pre-allocated, 2,000 cap) |
| **Threshold `p(99)<500ms`** | ✅ PASS (15.38ms) |
| **Threshold `http_req_failed rate<0.01`** | ✅ PASS (0.00%) |

The system never came close to needing its allotted VU capacity — every stage of the ramp (50→500→1,000→2,000 TPS target) was absorbed with single-digit-to-low-double-digit millisecond p99, a marked contrast to the pre-remediation runs in §3–4 (p50 in the hundreds of ms to seconds), because those runs measured an implementation that did no real validation work and had a single Kafka partition; this run has 6 partitions, a batched projection sink, and the full validation/topology path actually executing per request.

### 8.4 Financial correctness audit (`scenarios/verify-correctness.py`, corrected)

| Check | Result |
|---|---|
| **k6 outcome log vs `payment_projection` rows** | ✅ PASS — 209 accepted payments each have exactly one row; 86,393 payments correctly rejected as double-settlement (the charge they targeted was already `PAID`) each have exactly one row flagged `is_double_settlement=true`; every other rejection has zero rows. Zero disagreements. |
| **RocksDB (`GET /api/v1/charges/{id}`) vs Postgres projection** | ✅ PASS — `cumulativePaid`/status agree for all 6 charges touched by this run. Zero disagreements. |
| **Sink-arithmetic self-consistency (secondary, informational)** | Fails by design for any charge that accumulated double-settlement rows — this check naively sums *all* payment rows including flagged overpayment attempts against the original charge amount, which will never match once double-settlements exist. It is explicitly demoted and does not gate the audit's exit code (see `docs/benchmark-remediation-guideline.md` F3); the two checks above are the ones that matter and both pass. |

209 accepted payments against 6 seeded VAs, with the rest of the traffic hitting already-settled charges, is expected given the workload design: 4 of the 6 seed VAs are CLOSED-type charges that settle after one payment and reject everything after (§5.4 of `scenarios/suite-bsi.js`'s header comment documents this; a future run could weight VA selection toward the two OPEN/INSTALLMENT VAs, or seed more CLOSED charges, to keep a larger fraction of traffic in the accepted state for the whole ramp).

### 8.5 What is still required for a legitimate comparison

1. A real (non-`NULL`) BSI shared secret configured on the RDBMS baseline (`payment-gateway`), through its own admin flow or a fresh bootstrapped instance — not scripted by an agent without the operator's credentials.
2. `scenarios/suite-rdbms.js` run against that instance with the same `RUN_ID` and secret, producing its own committed summary/raw artifacts.
3. `scenarios/verify-correctness.py` run against the RDBMS side's results.
4. At least one more measured run per system (N≥2), per the guideline's G6 acceptance criteria — this section reports a single run.

No numbers are invented for the RDBMS side above; §3–7's pre-remediation numbers remain the only historical reference until that comparison run happens.


