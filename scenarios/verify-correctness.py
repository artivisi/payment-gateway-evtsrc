#!/usr/bin/env python3
"""
Financial & State Correctness Audit Harness.

PRIMARY checks -- an INDEPENDENT ground truth, not the projection's own arithmetic:

  1. k6-vs-DB cross-check. Ground truth comes from the k6 "payment_outcomes" custom metric in
     the --out json raw results file (produced by scenarios/suite-bsi.js / suite-rdbms.js),
     filtered to --run-id. Every bankReference the load generator logged as accepted at the
     HTTP/protocol layer must have exactly one row in the payment table; every bankReference
     logged as a protocol-level rejection must have zero rows.
  2. RocksDB-vs-Postgres cross-check (evtsrc only). For every chargeId touched by this run's
     recorded payments, GET /api/v1/charges/{id} reads the RocksDB-backed charge-state-store
     directly from the running app and must agree with the Postgres projection's
     paid_amount/status for the same charge.

Exit code is 1 if either PRIMARY check finds a disagreement.

SECONDARY / sink-consistency check (kept, but demoted -- does NOT affect the exit code):
paid_amount == SUM(payment.amount) and the derived remaining_amount/status. This was the ENTIRE
old audit and is tautological: PostgresProjectionSink derives paid_amount from the very payment
rows this check also sums, in the same transaction, so it cannot fail by construction. See
docs/benchmark-remediation-guideline.md F3. Retained only as a sink sanity check.
"""

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request


# ------------------------------------------------------------------------------------------
# DB config detection (dual RDBMS / evtsrc-postgres autodetect -- unchanged mechanism, extended
# with bank_ref_col, which happens to be the same column name on both sides, and an explicit
# is_evtsrc flag used to gate PRIMARY CHECK 2).
# ------------------------------------------------------------------------------------------

def get_db_config():
    res = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
    running_containers = res.stdout.strip().split()

    for c in running_containers:
        if "evtsrc-postgres" in c:
            return {
                "container": c,
                "user": "pguser",
                "db": "payment_gateway_reporting",
                "payment_table": "payment_projection",
                "charge_table": "charge_projection",
                "client_col": "client_id",
                "total_amt_col": "total_amount",
                "paid_amt_col": "paid_amount",
                "rem_amt_col": "remaining_amount",
                "charge_id_fk": "charge_id",
                "bank_ref_col": "bank_reference",
                "has_double_settlement": True,
                "is_evtsrc": True,
            }

    for c in running_containers:
        if "payment-gateway-db-1" in c or (("db" in c or "postgres" in c) and "app" not in c):
            return {
                "container": c,
                "user": "paymentgateway",
                "db": "paymentgateway",
                "payment_table": "payment",
                "charge_table": "charge",
                "client_col": "id_consumer",
                "total_amt_col": "amount",
                "paid_amt_col": "cumulative_paid",
                "rem_amt_col": "(c.amount - c.cumulative_paid)",
                "charge_id_fk": "id_charge",
                "bank_ref_col": "bank_reference",
                "has_double_settlement": False,
                "is_evtsrc": False,
            }

    print("[ERROR] No database container found running.")
    sys.exit(1)


def sql_string_literal(value):
    return "'" + value.replace("'", "''") + "'"


def run_psql_json(cfg, query):
    cmd = [
        "docker", "exec", cfg["container"],
        "psql", "-U", cfg["user"], "-d", cfg["db"],
        "-t", "-A", "-c", f"SELECT json_agg(t) FROM ({query}) t;"
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"[ERROR] PSQL query failed: {res.stderr.strip()}")
        print(f"[ERROR] Query was: {query}")
        sys.exit(1)
    output = res.stdout.strip()
    if not output or output == "null":
        return []
    try:
        return json.loads(output)
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON from psql: {e}\nRaw output: {output}")
        sys.exit(1)


# ------------------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Cross-checks the k6 outcome log and (for evtsrc) the RocksDB-backed charge "
                     "state against the Postgres projection / RDBMS payment table, for one "
                     "benchmark run.",
    )
    parser.add_argument(
        "--k6-results",
        required=True,
        help="Path to the k6 '--out json' raw results file for this run (contains the "
             "payment_outcomes custom metric with bankReference/outcome/httpStatus tags). "
             "Required -- there is no default path.",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="The RUN_ID value passed to the k6 script for this run. Filters this run's "
             "bankReferences out of the k6 results file and out of the payment table so results "
             "from different runs are never mixed. Required -- there is no default.",
    )
    parser.add_argument(
        "--evtsrc-base-url",
        default="http://localhost:8081",
        help="Base URL of the running evtsrc app, used only for PRIMARY CHECK 2 "
             "(GET /api/v1/charges/{id} against the RocksDB-backed charge-state-store). Ignored "
             "when the detected database is the RDBMS baseline. Default: http://localhost:8081.",
    )
    return parser.parse_args()


# ------------------------------------------------------------------------------------------
# PRIMARY CHECK 1: k6 outcome log (independent ground truth) vs recorded payment rows
# ------------------------------------------------------------------------------------------

def load_k6_ground_truth(path, run_id):
    """
    Streams the k6 '--out json' file (JSON Lines, one object per line) and extracts every data
    point for the "payment_outcomes" custom metric, keyed by bankReference. k6 also writes one
    non-"Point" metric-registration line per metric name and, depending on the run, many other
    metrics (http_req_duration, iterations, ...) -- both are skipped.
    """
    try:
        fh = open(path, "r", encoding="utf-8")
    except OSError as e:
        print(f"[ERROR] --k6-results file could not be opened: {path}: {e}", file=sys.stderr)
        sys.exit(1)

    ground_truth = {}
    conflicting = []
    with fh:
        for line_num, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[ERROR] Malformed JSON on line {line_num} of {path}: {e}", file=sys.stderr)
                sys.exit(1)

            if obj.get("metric") != "payment_outcomes" or obj.get("type") != "Point":
                continue

            tags = obj.get("data", {}).get("tags", {})
            bank_reference = tags.get("bankReference")
            outcome = tags.get("outcome")
            http_status = tags.get("httpStatus")
            if not bank_reference or not outcome:
                print(
                    f"[ERROR] payment_outcomes data point on line {line_num} of {path} is "
                    f"missing bankReference/outcome tags: {line}",
                    file=sys.stderr,
                )
                sys.exit(1)

            if not bank_reference.startswith(run_id):
                continue

            if bank_reference in ground_truth and ground_truth[bank_reference]["outcome"] != outcome:
                conflicting.append(bank_reference)
            ground_truth[bank_reference] = {"outcome": outcome, "httpStatus": http_status}

    if not ground_truth:
        print(
            f"[ERROR] No payment_outcomes data points in {path} matched --run-id '{run_id}'. "
            "Wrong --k6-results file, or wrong --run-id? Nothing to audit.",
            file=sys.stderr,
        )
        sys.exit(1)

    if conflicting:
        print(
            f"[WARN] {len(conflicting)} bankReference(s) appeared more than once in {path} with "
            f"conflicting outcomes (last one wins): {conflicting[:10]}"
            + (" ..." if len(conflicting) > 10 else "")
        )

    return ground_truth


def is_accepted_outcome(outcome):
    """
    suite-bsi.js's classifyOutcome() maps the BSI wire responseCode "00" to "ACCEPTED_OR_DUPLICATE"
    -- the only bucket meaning the protocol layer accepted the request. Every other bucket
    (REJECTED_*, ERROR, HTTP_*, UNPARSEABLE_RESPONSE, UNMAPPED_RESPONSE_CODE_*) is a rejection at
    the HTTP/protocol layer. Since every bankReference generated for one run is unique
    (RUN_ID-timestamp-random), the "OR_DUPLICATE" half of this bucket cannot legitimately fire
    within a single run's own traffic.
    """
    return outcome.startswith("ACCEPTED")


def is_double_settlement_outcome(outcome):
    """
    A payment rejected because the charge was already PAID is NOT a payment the system silently
    drops -- per docs/benchmark-remediation-guideline.md G1/G2, PaymentApplicationService and
    PaymentGatewayStreamsTopology both emit a DoubleSettlementDetectedEvent for this case instead
    of absorbing the overpayment, and PostgresProjectionSink projects it as exactly one
    payment_projection row with is_double_settlement=true. The BSI wire protocol maps this (and
    the still-unimplemented per-charge-type amount check) to responseCode "13", which
    classifyOutcome() labels REJECTED_CHARGE_CLOSED_OR_INVALID_AMOUNT. Treating this bucket as
    "must have zero rows" (as an earlier version of this check did) is a false positive: it is a
    rejection from the caller's point of view AND a correctly-flagged row from the ledger's point
    of view, simultaneously -- that is the whole point of "never silently absorbed".
    """
    return outcome == "REJECTED_CHARGE_CLOSED_OR_INVALID_AMOUNT"


def fetch_run_payment_rows(cfg, run_id):
    """
    Returns {bank_reference: [row, ...]} for every payment-table row whose bank_reference starts
    with run_id. Uses LEFT(col, len) = literal instead of LIKE so no wildcard-escaping is needed
    for run_id values containing '%' or '_'.
    """
    ds_col = ", is_double_settlement" if cfg["has_double_settlement"] else ""
    literal = sql_string_literal(run_id)
    query = f"""
        SELECT {cfg['bank_ref_col']} AS bank_reference,
               {cfg['charge_id_fk']} AS charge_id,
               amount{ds_col}
        FROM {cfg['payment_table']}
        WHERE LEFT({cfg['bank_ref_col']}, {len(run_id)}) = {literal}
    """
    rows = run_psql_json(cfg, query)
    by_reference = {}
    for row in rows:
        by_reference.setdefault(row["bank_reference"], []).append(row)
    return by_reference


def cross_check_k6_vs_db(ground_truth, db_rows):
    disagreements = []
    accepted_per_k6 = 0
    double_settlement_per_k6 = 0
    rejected_per_k6 = 0
    recorded_in_db = 0

    for bank_reference, info in ground_truth.items():
        outcome = info["outcome"]
        rows = db_rows.get(bank_reference, [])

        if is_accepted_outcome(outcome):
            accepted_per_k6 += 1
            if rows:
                recorded_in_db += 1
            if len(rows) != 1:
                disagreements.append({
                    "bankReference": bank_reference,
                    "k6Outcome": outcome,
                    "httpStatus": info.get("httpStatus"),
                    "expectedRows": 1,
                    "actualRows": len(rows),
                    "rows": rows,
                })
        elif is_double_settlement_outcome(outcome):
            double_settlement_per_k6 += 1
            if rows:
                recorded_in_db += 1
            flagged = [r for r in rows if r.get("is_double_settlement")]
            if len(rows) != 1 or len(flagged) != 1:
                disagreements.append({
                    "bankReference": bank_reference,
                    "k6Outcome": outcome,
                    "httpStatus": info.get("httpStatus"),
                    "expectedRows": "1 (is_double_settlement=true)",
                    "actualRows": f"{len(rows)} ({len(flagged)} flagged)",
                    "rows": rows,
                })
        else:
            rejected_per_k6 += 1
            if len(rows) != 0:
                disagreements.append({
                    "bankReference": bank_reference,
                    "k6Outcome": outcome,
                    "httpStatus": info.get("httpStatus"),
                    "expectedRows": 0,
                    "actualRows": len(rows),
                    "rows": rows,
                })

    return accepted_per_k6, double_settlement_per_k6, rejected_per_k6, recorded_in_db, disagreements


def print_k6_vs_db_report(cfg, args, ground_truth, accepted_per_k6, double_settlement_per_k6,
                          rejected_per_k6, recorded_in_db, disagreements):
    print("=" * 78)
    print(" PRIMARY CHECK 1: k6 outcome log (independent ground truth) vs recorded payment rows")
    print("=" * 78)
    print(f"k6 results file                     : {args.k6_results}")
    print(f"Run ID filter                       : {args.run_id}")
    print(f"Ground-truth data points (this run) : {len(ground_truth)}")
    print(f"Accepted per k6                      : {accepted_per_k6}")
    print(f"Double-settlement (rejected+flagged) : {double_settlement_per_k6}")
    print(f"Rejected per k6 (no event at all)    : {rejected_per_k6}")
    print(f"Recorded (>=1 row) in {cfg['payment_table']:<20}: {recorded_in_db}")
    print("-" * 78)
    if disagreements:
        print(f"MISMATCHES: {len(disagreements)} (this is the defect the old script could never catch)")
        for m in disagreements[:50]:
            print(
                f"  bankReference={m['bankReference']} k6Outcome={m['k6Outcome']} "
                f"httpStatus={m['httpStatus']} expectedRows={m['expectedRows']} actualRows={m['actualRows']}"
            )
            for row in m["rows"]:
                ds_note = ""
                if "is_double_settlement" in row:
                    ds_note = f" is_double_settlement={row['is_double_settlement']}"
                print(f"      -> chargeId={row['charge_id']} amount={row['amount']}{ds_note}")
        if len(disagreements) > 50:
            print(f"  ... and {len(disagreements) - 50} more")
    else:
        print("No mismatches: every k6-accepted bankReference has exactly one row; every "
              "k6-flagged double-settlement bankReference has exactly one is_double_settlement=true "
              "row; every other k6-rejected bankReference has zero rows.")
    print()


# ------------------------------------------------------------------------------------------
# PRIMARY CHECK 2: RocksDB (charge-state-store, via the running app) vs Postgres [evtsrc only]
# ------------------------------------------------------------------------------------------

def fetch_rocksdb_charge_state(base_url, charge_id):
    url = f"{base_url.rstrip('/')}/api/v1/charges/{charge_id}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return None, f"GET {url} returned HTTP {e.code}"
    except (urllib.error.URLError, OSError) as e:
        return None, f"GET {url} failed: {e}"
    try:
        return json.loads(body), None
    except json.JSONDecodeError as e:
        return None, f"GET {url} returned non-JSON body: {e}"


def normalize_rocksdb_status(rocksdb_status, cumulative_paid):
    """
    The RocksDB charge-state-store (PaymentGatewayStreamsTopology) only ever writes "ACTIVE" or
    "PAID" -- it has no partially-paid marker. PostgresProjectionSink writes a third value,
    "PARTIALLY_PAID", for the same underlying state once any payment has landed. Comparing the
    two vocabularies literally would spuriously flag every partially-paid or fully-paid charge as
    a mismatch even when cumulativePaid agrees exactly. Map RocksDB's status into the projection's
    three-value vocabulary before comparing.
    """
    if rocksdb_status == "PAID":
        return "FULLY_PAID"
    if rocksdb_status == "ACTIVE":
        return "PARTIALLY_PAID" if cumulative_paid > 0 else "ACTIVE"
    return rocksdb_status  # unexpected value -- compare literally and let it mismatch loudly


def cross_check_rocksdb_vs_postgres(base_url, touched_charges, missing_in_pg):
    disagreements = []

    for charge_id in missing_in_pg:
        disagreements.append({
            "chargeId": charge_id,
            "reason": "referenced by a recorded payment row but has no row in the charge projection table",
        })

    for charge_id, pg_row in touched_charges.items():
        state, err = fetch_rocksdb_charge_state(base_url, charge_id)
        if err is not None:
            disagreements.append({"chargeId": charge_id, "reason": err})
            continue
        if "cumulativePaid" not in state or "status" not in state:
            disagreements.append({
                "chargeId": charge_id,
                "reason": f"RocksDB response missing cumulativePaid/status fields: {state}",
            })
            continue

        rocks_paid = float(state["cumulativePaid"])
        rocks_status = normalize_rocksdb_status(state["status"], rocks_paid)
        pg_paid = float(pg_row["paid_amount"])
        pg_status = pg_row["status"]

        if abs(rocks_paid - pg_paid) >= 0.01 or rocks_status != pg_status:
            disagreements.append({
                "chargeId": charge_id,
                "rocksDbCumulativePaid": rocks_paid,
                "postgresPaidAmount": pg_paid,
                "rocksDbStatusRaw": state["status"],
                "rocksDbStatusNormalized": rocks_status,
                "postgresStatus": pg_status,
            })

    return disagreements


def print_rocksdb_vs_postgres_report(cfg, args, db_rows, charge_audit):
    print("=" * 78)
    print(" PRIMARY CHECK 2: RocksDB charge-state-store (live app) vs Postgres projection")
    print("=" * 78)

    if not cfg["is_evtsrc"]:
        print(f"Skipped: {cfg['container']} is the RDBMS baseline, which has no RocksDB state "
              "store -- this cross-check is evtsrc-only per G7.")
        print()
        return []

    touched_charge_ids = sorted({row["charge_id"] for rows in db_rows.values() for row in rows})
    charge_by_id = {row["charge_id"]: row for row in charge_audit}

    touched_charges = {cid: charge_by_id[cid] for cid in touched_charge_ids if cid in charge_by_id}
    missing_in_pg = [cid for cid in touched_charge_ids if cid not in charge_by_id]

    print(f"evtsrc base URL                       : {args.evtsrc_base_url}")
    print(f"Distinct chargeIds touched by this run: {len(touched_charge_ids)}")

    disagreements = cross_check_rocksdb_vs_postgres(args.evtsrc_base_url, touched_charges, missing_in_pg)

    print("-" * 78)
    if disagreements:
        print(f"MISMATCHES: {len(disagreements)}")
        for m in disagreements[:50]:
            print(f"  chargeId={m['chargeId']}")
            for k, v in m.items():
                if k == "chargeId":
                    continue
                print(f"      {k}={v}")
        if len(disagreements) > 50:
            print(f"  ... and {len(disagreements) - 50} more")
    else:
        print("No mismatches: RocksDB cumulativePaid/status agree with the Postgres projection "
              "for every charge touched by this run.")
    print()
    return disagreements


# ------------------------------------------------------------------------------------------
# SECONDARY / sink-consistency check (the old headline check, demoted -- see module docstring)
# ------------------------------------------------------------------------------------------

def fetch_charge_audit(cfg):
    return run_psql_json(cfg, f"""
        SELECT
            c.id as charge_id,
            c.{cfg['client_col']} as client_id,
            c.charge_type,
            c.{cfg['total_amt_col']} as total_amount,
            c.{cfg['paid_amt_col']} as paid_amount,
            {cfg['rem_amt_col']} as remaining_amount,
            c.status,
            COALESCE(SUM(p.amount), 0) as actual_sum_payments,
            COUNT(p.id) as payment_count
        FROM {cfg['charge_table']} c
        LEFT JOIN {cfg['payment_table']} p ON p.{cfg['charge_id_fk']} = c.id
        GROUP BY c.id, c.{cfg['client_col']}, c.charge_type, c.{cfg['total_amt_col']}, c.{cfg['paid_amt_col']}, c.status, c.created_at
        ORDER BY c.created_at
    """)


def print_secondary_sink_consistency_check(cfg, charge_audit):
    print("=" * 78)
    print(" SECONDARY / SINK-CONSISTENCY CHECK -- demoted, does NOT gate the exit code")
    print(" paid_amount == SUM(payments) is computed by the same write path this check re-sums;")
    print(" it cannot fail by construction. See docs/benchmark-remediation-guideline.md F3.")
    print("=" * 78)

    payment_summary = run_psql_json(cfg, f"""
        SELECT
            COUNT(*) as total_payments,
            COALESCE(SUM(amount), 0) as total_paid_volume
            {", COUNT(CASE WHEN is_double_settlement THEN 1 END) as double_settlements" if cfg["has_double_settlement"] else ""}
        FROM {cfg['payment_table']}
    """)
    if payment_summary:
        s = payment_summary[0]
        print(f"Total Payments Recorded : {s.get('total_payments', 0):,}")
        print(f"Total Paid Volume       : IDR {float(s.get('total_paid_volume', 0)):,.2f}")
        if cfg["has_double_settlement"]:
            print(f"Double Settlements      : {s.get('double_settlements', 0)}")
    print("-" * 78)

    failures = 0
    print(f"{'Charge ID':<38} | {'Paid Amount':<15} | {'Payment Sum':<15} | {'Remaining':<12} | {'Status':<10} | {'Sink Check'}")
    print("-" * 115)

    for row in charge_audit:
        charge_id = row["charge_id"]
        total_amount = float(row["total_amount"])
        paid_amount = float(row["paid_amount"])
        remaining_amount = float(row["remaining_amount"])
        actual_sum = float(row["actual_sum_payments"])
        status = row["status"]
        ctype = row["charge_type"]

        paid_match = abs(paid_amount - actual_sum) < 0.01
        expected_remaining = max(0.0, total_amount - paid_amount)
        remaining_match = (ctype == "OPEN") or (abs(remaining_amount - expected_remaining) < 0.01)
        expected_status = "FULLY_PAID" if expected_remaining == 0 else ("PARTIALLY_PAID" if paid_amount > 0 else "ACTIVE")
        status_match = (status == expected_status or (expected_status == "FULLY_PAID" and status == "PAID")) or (ctype == "OPEN" and status == "ACTIVE")

        is_valid = paid_match and remaining_match and status_match
        if not is_valid:
            failures += 1
            audit_str = "FAIL"
        else:
            audit_str = "PASS"

        print(f"{charge_id:<38} | {paid_amount:>15,.2f} | {actual_sum:>15,.2f} | {remaining_amount:>12,.2f} | {status:<10} | {audit_str}")

    print("-" * 115)
    if failures == 0:
        print("Sink-consistency check: all rows PASS (expected -- this check is tautological, see above).")
    else:
        print(f"Sink-consistency check: {failures} row(s) FAIL. Informational only; does not affect exit code.")
    print()
    return failures


# ------------------------------------------------------------------------------------------
# main
# ------------------------------------------------------------------------------------------

def main():
    args = parse_args()
    ground_truth = load_k6_ground_truth(args.k6_results, args.run_id)
    cfg = get_db_config()

    print("=" * 78)
    print(f" FINANCIAL CORRECTNESS & INVARIANT AUDIT HARNESS ({cfg['container']} / {cfg['db']})")
    print("=" * 78)
    print()

    db_rows = fetch_run_payment_rows(cfg, args.run_id)
    accepted_per_k6, double_settlement_per_k6, rejected_per_k6, recorded_in_db, mismatches1 = \
        cross_check_k6_vs_db(ground_truth, db_rows)
    print_k6_vs_db_report(cfg, args, ground_truth, accepted_per_k6, double_settlement_per_k6,
                          rejected_per_k6, recorded_in_db, mismatches1)

    charge_audit = fetch_charge_audit(cfg)
    mismatches2 = print_rocksdb_vs_postgres_report(cfg, args, db_rows, charge_audit)

    print_secondary_sink_consistency_check(cfg, charge_audit)

    print("=" * 78)
    if mismatches1 or mismatches2:
        print(f"AUDIT FAILED: {len(mismatches1)} k6-vs-DB disagreement(s), "
              f"{len(mismatches2)} RocksDB-vs-Postgres disagreement(s).")
        sys.exit(1)
    else:
        print("AUDIT PASSED: k6 outcome log and RocksDB state agree with the recorded payment rows.")
        sys.exit(0)


if __name__ == "__main__":
    main()
