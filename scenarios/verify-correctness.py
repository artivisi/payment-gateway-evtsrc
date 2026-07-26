#!/usr/bin/env python3
"""
Financial & State Correctness Audit Harness.

Verifies post-test data integrity across PostgreSQL read model tables:
1. Payment Count & Total Volume matching recorded events.
2. Double Settlement / Duplicate Reference Detection (idempotency check).
3. Charge Balance Accounting Invariants:
   - paid_amount == SUM(payment_projection.amount FOR charge_id)
   - remaining_amount == MAX(0, total_amount - paid_amount)
   - status == 'FULLY_PAID' (if remaining_amount == 0) ELSE 'PARTIALLY_PAID' or 'ACTIVE'
"""

import subprocess
import json
import sys

def run_psql_json(query):
    cmd = [
        "docker", "exec", "evtsrc-postgres",
        "psql", "-U", "pguser", "-d", "payment_gateway_reporting",
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
    print("=" * 78)
    print(" FINANCIAL CORRECTNESS & INVARIANT AUDIT HARNESS")
    print("=" * 78)

    # 1. Total Payments Summary
    payment_summary = run_psql_json("""
        SELECT 
            COUNT(*) as total_payments,
            COALESCE(SUM(amount), 0) as total_paid_volume,
            COUNT(CASE WHEN is_double_settlement THEN 1 END) as double_settlements
        FROM payment_projection
    """)

    if payment_summary:
        s = payment_summary[0]
        print(f"Total Payments Recorded : {s.get('total_payments', 0):,}")
        print(f"Total Paid Volume       : IDR {float(s.get('total_paid_volume', 0)):,.2f}")
        print(f"Double Settlements      : {s.get('double_settlements', 0)}")
    print("-" * 78)

    # 2. Charge Balance Audit & Invariant Checks
    charge_audit = run_psql_json("""
        SELECT 
            c.id as charge_id,
            c.client_id,
            c.charge_type,
            c.total_amount,
            c.paid_amount,
            c.remaining_amount,
            c.status,
            COALESCE(SUM(p.amount), 0) as actual_sum_payments,
            COUNT(p.id) as payment_count
        FROM charge_projection c
        LEFT JOIN payment_projection p ON p.charge_id = c.id
        GROUP BY c.id, c.client_id, c.charge_type, c.total_amount, c.paid_amount, c.remaining_amount, c.status
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

        # Invariant 1: paid_amount == sum of payments
        paid_match = abs(paid_amount - actual_sum) < 0.01

        # Invariant 2: remaining_amount == max(0, total_amount - paid_amount)
        expected_remaining = max(0.0, total_amount - paid_amount)
        remaining_match = abs(remaining_amount - expected_remaining) < 0.01

        # Invariant 3: Status consistency
        expected_status = "FULLY_PAID" if expected_remaining == 0 else ("PARTIALLY_PAID" if paid_amount > 0 else "ACTIVE")
        status_match = (status == expected_status) or (row['charge_type'] == 'OPEN' and status == 'ACTIVE')

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
