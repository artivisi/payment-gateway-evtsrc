#!/usr/bin/env bash
# scenarios/seed-importer.sh
# Portable API-Based Seed Importer Reusable for BOTH RDBMS and CQRS Gateways
#
# Usage:
#   ./scenarios/seed-importer.sh http://localhost:8080 (RDBMS Baseline)
#   ./scenarios/seed-importer.sh http://localhost:8081 (CQRS Event-Sourced)

TARGET_URL="${1:-http://localhost:8080}"
SEED_FILE="$(dirname "$0")/seed-data.json"

if [ ! -f "$SEED_FILE" ]; then
    echo "[ERROR] Seed file not found at $SEED_FILE"
    exit 1
fi

echo "=========================================================================="
echo " Seeding Initial State via REST API -> $TARGET_URL"
echo " Seed File: $SEED_FILE"
echo "=========================================================================="

python3 -c "
import json, urllib.request

url = '$TARGET_URL/api/charges'
with open('$SEED_FILE') as f:
    items = json.load(f)

success = 0
for item in items:
    req = urllib.request.Request(url, data=json.dumps(item).encode('utf-8'), headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req) as resp:
            if resp.status in (200, 201):
                success += 1
                print(f'[SUCCESS] Seeded Charge {item.get(\"chargeId\")} ({len(item.get(\"siblingVas\", []))} VAs)')
    except Exception as e:
        print(f'[ERROR] Failed to seed {item.get(\"chargeId\")}: {e}')

print(f'\nSeeding Complete! Successfully seeded {success}/{len(items)} charges.')
"
