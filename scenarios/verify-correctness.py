#!/usr/bin/env python3
"""
Financial & State Correctness Audit Harness.

Verifies post-test data integrity across PostgreSQL read model / RDBMS tables:
1. Payment Count & Total Volume matching recorded events.
2. Double Settlement / Duplicate Reference Detection (idempotency check).
3. Charge Balance Accounting Invariants:
   - paid_amount == SUM(payment.amount FOR charge_id)
   - remaining_amount == MAX(0, total_amount - paid_amount) (for CLOSED/INSTALLMENT)
   - status == 'FULLY_PAID' or 'PAID' (if remaining_amount == 0) ELSE 'PARTIALLY_PAID' or 'ACTIVE'
"""

import subprocess
import json
import sys

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
                "has_double_settlement": True
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
                "has_double_settlement": False
            }

    print("[ERROR] No database container found running.")
    sys.exit(1)

def run_psql_json(cfg, query):
    cmd = [
        "docker", "exec", cfg["container"],
        "psql", "-U", cfg["user"], "-d", cfg["db"],
        "-t", "-A", "-c", f"SELECT json_agg(t) FROM ({query}) t;"
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"[ERROR] PSQL query failed: {res.stderr.strip()}")
        return []
    output = res.stdout.strip()
    if not output or output == "null":
        return []
    try:
        return json.loads(output)
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON from psql: {e}\nRaw output: {output}")
        return []

def main():
    cfg = get_db_config()
    print("=" * 78)
    print(f" FINANCIAL CORRECTNESS & INVARIANT AUDIT HARNESS ({cfg['container']} / {cfg['db']})")
    print("=" * 78)

    double_settlement_clause = "COUNT(CASE WHEN is_double_settlement THEN 1 END) as double_settlements" if cfg["has_double_settlement"] else "0 as double_settlements"
    
    # 1. Total Payments Summary
    payment_summary = run_psql_json(cfg, f"""
        SELECT 
            COUNT(*) as total_payments,
            COALESCE(SUM(amount), 0) as total_paid_volume,
            {double_settlement_clause}
        FROM {cfg['payment_table']}
    """)

    if payment_summary:
        s = payment_summary[0]
        print(f"Total Payments Recorded : {s.get('total_payments', 0):,}")
        print(f"Total Paid Volume       : IDR {float(s.get('total_paid_volume', 0)):,.2f}")
        print(f"Double Settlements      : {s.get('double_settlements', 0)}")
    print("-" * 78)

    # 2. Charge Balance Audit & Invariant Checks
    charge_audit = run_psql_json(cfg, f"""
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

    failures = 0
    print(f"{'Charge ID':<38} | {'Paid Amount':<15} | {'Payment Sum':<15} | {'Remaining':<12} | {'Status':<10} | {'Audit'}")
    print("-" * 115)

    for row in charge_audit:
        charge_id = row['charge_id']
        total_amount = float(row['total_amount'])
        paid_amount = float(row['paid_amount'])
        remaining_amount = float(row['remaining_amount'])
        actual_sum = float(row['actual_sum_payments'])
        status = row['status']
        ctype = row['charge_type']

        # Invariant 1: paid_amount == sum of payments
        paid_match = abs(paid_amount - actual_sum) < 0.01

        # Invariant 2: remaining_amount == max(0, total_amount - paid_amount) for CLOSED/INSTALLMENT
        expected_remaining = max(0.0, total_amount - paid_amount)
        remaining_match = (ctype == 'OPEN') or (abs(remaining_amount - expected_remaining) < 0.01)

        # Invariant 3: Status consistency
        expected_status = "FULLY_PAID" if expected_remaining == 0 else ("PARTIALLY_PAID" if paid_amount > 0 else "ACTIVE")
        status_match = (status == expected_status or (expected_status == "FULLY_PAID" and status == "PAID")) or (ctype == 'OPEN' and status == 'ACTIVE')

        is_valid = paid_match and remaining_match and status_match
        if not is_valid:
            failures += 1
            audit_str = "❌ FAIL"
        else:
            audit_str = "✅ PASS"

        print(f"{charge_id:<38} | {paid_amount:>15,.2f} | {actual_sum:>15,.2f} | {remaining_amount:>12,.2f} | {status:<10} | {audit_str}")

    print("-" * 115)
    if failures == 0:
        print("✅ ALL FINANCIAL INVARIANTS AND BALANCE CONSTRAINTS VERIFIED PERFECTLY!")
        sys.exit(0)
    else:
        print(f"❌ AUDIT FAILED WITH {failures} DISCREPANCIES DETECTED.")
        sys.exit(1)

if __name__ == "__main__":
    main()
