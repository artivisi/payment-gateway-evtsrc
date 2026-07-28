# scenarios/results/

Committed raw k6 artifacts, one pair of files per benchmark run:

- `<date>-<system>-summary.json` - k6 `--summary-export` output (aggregate stats: request counts,
  latency percentiles, threshold pass/fail).
- `<date>-<system>-raw.json` - k6 `--out json` output (full per-request/per-metric time series,
  including the `payment_outcomes` custom metric with `bankReference` / `outcome` / `httpStatus`
  tags - the only reliable record of what each request actually did, since the BSI wire protocol
  returns HTTP 200 for both accepted payments and business rejections).

`<system>` is `evtsrc` (this repo, produced by `scenarios/suite-bsi.js` /
`scenarios/run-benchmark.sh`) or `rdbms` (the sibling `payment-gateway` repo, produced by
`scenarios/suite-rdbms.js` run against that repo's app).

Per `docs/benchmark-remediation-guideline.md` (G6): every number that appears in
`perf_benchmark_report.md` must be derived from a file in this directory. Do not hand-type a
benchmark table. Do not average or merge numbers from different files/runs into a single column -
report one discarded warm-up run plus N>=2 measured runs per system, each individually.
