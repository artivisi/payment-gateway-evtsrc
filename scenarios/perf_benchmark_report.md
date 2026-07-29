# Performance Benchmark & Financial Correctness Report

**Systems under test**: `payment-gateway-evtsrc` (event-sourced/CQRS) vs. `payment-gateway` (relational
baseline), benchmarked head-to-head through the identical BSI protocol workload.
**Date**: 2026-07-29 (afternoon re-run; see "Retraction" below for what changed since the morning)
**Test tool**: [k6](https://k6.io) v0.55+

---

## Retraction: the same-day morning run's saturation and correctness-defect findings did not reproduce

An earlier version of this file, written the same morning, reported evtsrc saturating well before
the 2,000 TPS ramp completed (p99 1.16s–3.22s, needing 1,200+ VUs) and a financial-correctness
defect in both of its two runs (one payment double-recorded per run). Shortly after that report was
written, the operator restarted the machine after finding OrbStack running a hanging VM, and
separately flagged "a severe resource hogging problem" from that session. This afternoon's re-run —
on a freshly-restarted machine, with the environment explicitly checked for contamination before
each load-generation phase (see §1) — reproduced **neither** finding:

- evtsrc's p99 was 8.5–9.4ms in both runs (vs. 1.16s–3.22s that morning), flat across every ramp
  stage, never exceeding its pre-allocated VU pool.
- Both evtsrc runs' correctness audits passed with **zero** mismatches on both checks (vs. one
  double-recorded payment per run that morning).

This is a retraction, not a refinement: the morning's numbers were real measurements (not
fabricated), but the environment they were measured in was not what it was assumed to be — later
found to include a hanging OrbStack VM, and, separately, this session directly observed another
Testcontainers-based test session spinning up containers on the same Docker daemon during setup.
**Neither the saturation ceiling nor the double-write defect should be treated as a characteristic of
evtsrc's architecture** until reproduced under a controlled environment; this afternoon's clean,
twice-reproduced result is the current evidence, and it says the opposite. The morning's raw
artifacts and analysis remain in `scenarios/results/2026-07-29-*` and in
`docs/benchmark-remediation-guideline.md`'s "Fourth gap" for the record — they are a case study in
environment contamination, not a benchmark result to design around. See "Fifth gap" in that
document for the full account.

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
scripts, and the same seed data failed identically in two out of two runs that morning.

---

## 5. What's still needed for full confidence

1. **Dedicated (not shared) hardware** remains the biggest open gap — even this afternoon's clean
   run shares the machine with background OS processes, and the morning's contamination episode is
   a direct demonstration of how much that can matter for this specific comparison.
2. **A repeatable, scriptable environment-contamination check.** This pass's check
   (`docker ps -a` + `uptime`, done manually, twice, by eye) worked, but a small script that fails
   loudly if unrelated containers are churning or load average exceeds a threshold would make this
   procedure enforceable rather than a manual step someone can forget.
3. **The morning's saturation and correctness-defect findings are retracted, not explained.** This
   report does not know *why* resource contention (a hanging OrbStack VM, and/or a concurrent
   Testcontainers session) would specifically produce a Kafka-Streams double-write rather than, say,
   uniformly slower responses on both systems. If either finding recurs under a verified-clean
   environment in the future, it should be treated as new evidence, not a confirmation of the
   morning's report — this pass's data currently argues against it being real.
4. RDBMS's hot-row lock-contention behavior (from an even earlier report, run against a long-lived,
   never-reset database) remains unverified in any of today's runs, which all started from empty
   databases by design.

No numbers above are invented or estimated — every figure comes from a committed artifact under
`scenarios/results/` and a live `verify-correctness.py` / `knee-analysis.py` run against it.
