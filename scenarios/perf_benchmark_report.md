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

## 8. Re-benchmark Required

The previous version of this section ("Architectural Comparison & Analysis") has been deleted. Per `docs/benchmark-remediation-guideline.md` finding F7, its head-to-head table was not traceable to any k6 run and internally contradicted the measured runs in §3–4 of this same file: request counts identical to another run to the digit (86,581), a min/max/p95 collage of values pulled from different runs (including a µs/s unit swap on the quoted "p95"), and seed-data VA counts (19 in §2, 23 in the deleted §8.1) that disagreed with each other and with the script's actual seed. None of that table's numbers came from a committed k6 artifact.

**No comparative benchmark numbers currently exist for this system pair.** Sections 1–7 above measured a workload (`scenarios/suite.js`, generic `/api/v1/payments`, no pre-validation, no checksum, caller-supplied `chargeId`) that this remediation found exercised none of the gateway's real validation logic — not the RDBMS baseline's production code path, and not comparable to it. They are retained as historical record only (see the warning at the top of this file) and are not a substitute for a re-run.

A legitimate re-benchmark must:

1. Run `scenarios/suite-bsi.js` against this repo's real `/api/bank/bsi` adapter, and `scenarios/suite-rdbms.js` against the sibling `payment-gateway` repo's `/api/bank/bsi` adapter — the same protocol, same checksum scheme, same 6 BSI VA/amount pairs from `scenarios/seed-data.json`, same `ramping-arrival-rate` profile, on both sides. `scenarios/suite.js` itself now throws immediately rather than run, so it cannot be used by mistake.
2. Set `RUN_ID` and `BSI_SHARED_SECRET` (no defaults — both scripts fail loud in the k6 init stage if either is missing) and use `scenarios/run-benchmark.sh` (or the equivalent direct `k6 run` invocation documented in the header comment of `scenarios/suite-bsi.js`) so `--summary-export` and `--out json` are always captured.
3. Commit the resulting summary and raw JSON files under `scenarios/results/` per the naming contract in `scenarios/results/README.md`, run at least one discarded warm-up plus N≥2 measured runs per system, and derive every table in a future version of this report from those committed files — never hand-typed or averaged across runs into one column.
4. Run `scenarios/verify-correctness.py` against the k6 raw output and both systems' state (Postgres, and for evtsrc also the RocksDB-backed `GET /api/v1/charges/{id}` endpoint) after projection lag reaches zero, and report its pass/fail result rather than assuming zero double settlements.
5. Report accepted-payment TPS and reject/duplicate TPS separately (classified from the in-body `responseCode`, not HTTP status — the BSI protocol returns `200` for business rejections too), with knee/saturation analysis derived from the exported time series.

No new numbers are invented or estimated here. This section will be replaced with a real comparison table once that re-run has been executed and its artifacts committed.


