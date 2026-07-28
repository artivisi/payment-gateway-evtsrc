# Performance Benchmark & Financial Correctness Report

**Systems under test**: `payment-gateway-evtsrc` (event-sourced/CQRS) vs. `payment-gateway` (relational
baseline), benchmarked head-to-head through the identical BSI protocol workload.
**Date**: 2026-07-28
**Test tool**: [k6](https://k6.io) v0.55+

Earlier versions of this file contained benchmark numbers measured against `scenarios/suite.js`, a
generic endpoint with no server-side pre-validation, checksum, or idempotency check — a workload
that exercised none of the gateway's real logic (`docs/benchmark-remediation-guideline.md` findings
F1/F2/F5/F7). That script and its numbers have been removed rather than kept as a labeled
"historical" appendix: they describe code that no longer exists, and keeping fabricated-looking
numbers around risks exactly the confusion this remediation exists to fix. See the guideline
document for the full audit of what was wrong and how it was fixed. Everything below is current,
real, and traceable to a committed artifact.

---

## 1. Methodology

- **Load tool**: k6, `ramping-arrival-rate` executor, 50 → 500 → 1,000 → 2,000 TPS target over 90
  seconds, same profile on both systems.
- **Protocol under test**: the real production BSI adapter (`/api/bank/bsi`), full SHA-1 checksum
  verification, the same six seeded VA/amount pairs (CLOSED, OPEN, INSTALLMENT charge types) on
  both sides.
- **Scripts**: [`scenarios/suite-bsi.js`](suite-bsi.js) (evtsrc) and
  [`scenarios/suite-rdbms.js`](suite-rdbms.js) (RDBMS baseline) — identical request shape, checksum
  scheme, and ramp profile. Reproduce with [`scenarios/run-benchmark.sh`](run-benchmark.sh) (evtsrc)
  or the equivalent direct `k6 run` invocation documented in `suite-rdbms.js`'s header comment
  (RDBMS).
- **Run conditions**: `RUN_ID` and `BSI_SHARED_SECRET` are required environment variables — both
  scripts throw in the k6 init stage if either is missing. Each system's BSI escrow secret must be
  a real, non-`NULL` value matching `BSI_SHARED_SECRET` (see `docs/benchmark-remediation-guideline.md`
  finding F5 for why a `NULL` secret previously made the checksum trivially forgeable).
- **Audit**: [`scenarios/verify-correctness.py`](verify-correctness.py), cross-checking the k6
  `payment_outcomes` metric (an independent ground truth of what actually happened per request)
  against the recorded payment rows, plus a RocksDB-vs-Postgres consistency check for evtsrc. Pass
  `--target evtsrc` or `--target rdbms` when both systems' database containers are running at once
  (auto-detection refuses to guess in that case — see the script's own history of getting this
  wrong).
- **Hardware**: Apple M5, 10-core, 16GB — **shared, not dedicated**. Two runs were captured per
  system to get a real repeat measurement rather than a single point estimate; see §3 for what that
  repeat measurement actually revealed.

---

## 2. Measured results

| Metric | evtsrc Run 1 | evtsrc Run 2 | RDBMS Run 1 | RDBMS Run 2 |
|---|---|---|---|---|
| Run ID | `20260728152250` | `20260728180925` | `20260728140654` | `20260728181811` |
| Total requests | 86,608 | 86,556 | 86,582 | 85,853 |
| HTTP error rate | 0.00% | 0.00% | 0.00% | 0.00% |
| Dropped iterations | 28 | 80 | 42 | 772 |
| Effective throughput | 958.2 req/s | 957.9 req/s | 962.0 req/s | 953.6 req/s |
| Min latency | 292 µs | 332 µs | 271 µs | 272 µs |
| Median latency | 3.95 ms | 4.12 ms | 674 µs | 1.21 ms |
| Avg latency | 4.36 ms | 4.85 ms | 2.01 ms | 27.28 ms |
| p90 | 6.59 ms | 6.99 ms | 3.11 ms | 27.66 ms |
| p95 | 7.60 ms | 8.70 ms | 5.09 ms | 247.08 ms |
| p99 | 18.13 ms | 27.07 ms | 21.15 ms | **471.82 ms** |
| Max latency | 243.78 ms | 233.05 ms | 165.33 ms | 1.07 s |
| Peak VUs used | 40 / 101 | 73 / 123 | 26 / 130 | 662 / 716 |
| Threshold `p(99)<500ms` | PASS | PASS | PASS | PASS (barely) |
| Threshold error rate <1% | PASS | PASS | PASS | PASS |

Artifacts: `scenarios/results/2026-07-28-{evtsrc,rdbms}-run{1,2}-{summary.json,raw.json.gz}`.

---

## 3. What the repeat run actually found: performance degrades on a hot, never-reset dataset

Both systems' second run is slower than their first — RDBMS dramatically so (p99 21ms → 472ms,
peak VUs 26 → 662). The initial hypothesis was cross-system resource contention (both stacks
running on the same shared machine at once); that was ruled out by tearing evtsrc's stack down
completely and re-running RDBMS run 2 in isolation — it was **still** degraded.

The actual cause, confirmed by querying the database directly:

```
SELECT id_charge, count(*) FROM payment GROUP BY id_charge ORDER BY count(*) DESC;
  c3d4e5f6-...  57,781 rows   (OPEN charge)
  a7b8c9d0-...  57,255 rows   (OPEN charge)
  ...           <10 rows each (CLOSED/INSTALLMENT charges)
```

The two `OPEN` charges in the seed data never close (by design — see
`payment-gateway/CLAUDE.md`'s charge lifecycle section), so every run's traffic against them keeps
accumulating in the same two rows, on a database that was never reset between runs today. By RDBMS
run 2, those two rows had **115,045** cumulative payment rows behind them. Under `SELECT FOR UPDATE`
row-locking, sustained concurrent traffic against a small, ever-growing hot set produces worse lock
queueing than evtsrc's design, which doesn't hold a row lock across the request — evtsrc degraded
too (p99 18ms → 27ms) but far less sharply.

This is a real, useful finding, not benchmark noise: **the workload's own design (a small number of
charges absorbing nearly all traffic) is an adversarial case for row-locking, and the two systems
degrade under it at different rates.** It also means the two runs per system captured here are *not*
independent, identically-distributed samples — run 2 always inherits run 1's accumulated state. A
methodologically clean repeat measurement needs the database reset (or fresh seed data) between
runs, which was not done here.

---

## 4. Financial correctness audit (`scenarios/verify-correctness.py`)

| Run | k6 outcome log vs. recorded payment rows | RocksDB vs. Postgres (evtsrc only) |
|---|---|---|
| evtsrc Run 1 | PASS — 28,897 accepted, 57,699 double-settlement (every one flagged), 0 disagreements | PASS — 0 disagreements |
| evtsrc Run 2 | PASS — 29,072 accepted, 57,472 double-settlement (every one flagged), 0 disagreements | PASS — 0 disagreements |
| RDBMS Run 1 | PASS — 28,953 accepted, 57,629 double-settlement-class rejections (zero-row, none flagged), 0 disagreements | N/A |
| RDBMS Run 2 | PASS — 28,512 accepted, 57,341 double-settlement-class rejections (zero-row, none flagged), 0 disagreements | N/A |

All four runs pass cleanly. The accepted/double-settlement split is stable across both systems and
both runs (evtsrc ~28.9–29.1k, RDBMS ~28.5–29.0k) — consistent with the two systems agreeing on the
underlying domain behavior (see `docs/benchmark-remediation-guideline.md`'s "Third gap" for the
OPEN-vs-INSTALLMENT bug this comparison surfaced and fixed).

RDBMS's double-settlement-class rejections correctly show zero footprint (its pessimistic lock
rejects before any row is written for the ordinary, non-racing case); evtsrc's show exactly one
flagged row each (every attempt against an already-`PAID` charge is visible for reconciliation, by
design — see G2 in the guideline).

---

## 5. What's still needed for full confidence

1. **Reset the database (or use fresh seed data) between runs.** The degradation in §3 is real, but
   it means these two runs measure "cold" vs. "warm-with-115k-rows-of-history," not two independent
   samples of the same condition. A clean comparison needs either a fresh schema per run or a much
   larger/more diverse VA pool so no single row absorbs tens of thousands of requests.
2. **Dedicated (not shared) hardware.** All four runs shared this machine with other work.
3. **A specific investigation into why RDBMS's tail degrades faster than evtsrc's** under the hot-row
   condition in §3 — plausible (lock queueing depth scaling with concurrent waiters) but not
   root-caused here; that's a targeted profiling task, not a benchmark-report claim.

No numbers above are invented or estimated — every figure comes from a committed artifact under
`scenarios/results/` and a live `verify-correctness.py` run against it.
