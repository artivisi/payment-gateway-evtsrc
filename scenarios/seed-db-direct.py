#!/usr/bin/env python3
import json
import subprocess
import os

SEED_FILE = os.path.join(os.path.dirname(__file__), "seed-data.json")
ENCRYPTED_SECRET = "AAAAAAAAAAAAAAAAyE1k3mv1WcCOamrnB9YpkyC8H8SgqUmKOFw="

def run_psql(sql):
    cmd = [
        "docker", "exec", "-i", "payment-gateway-db-1",
        "psql", "-U", "paymentgateway", "-d", "paymentgateway"
    ]
    res = subprocess.run(cmd, input=sql, text=True, capture_output=True)
    if res.returncode != 0:
        print("PSQL ERROR:", res.stderr.strip())
    elif res.stdout and "ERROR" in res.stdout:
        print("PSQL STDOUT ERROR:", res.stdout.strip())
    return res.stdout

print("Seeding PostgreSQL directly...")
# 1. Clean existing test data completely
run_psql("TRUNCATE TABLE payment, virtual_account, charge, escrow_account, consumer CASCADE;")

# 2. Ensure consumers with valid AES-GCM encrypted client_secret
consumers_sql = f"""
INSERT INTO consumer (id, code, name, client_id, client_secret, webhook_url, webhook_suspended, status, created_at, updated_at) VALUES
('00000000-0000-0000-0000-000000000101', 'CLIENT-UNIVERSITAS-TAZKIA', 'Universitas Tazkia', 'CLIENT-UNIVERSITAS-TAZKIA', '{ENCRYPTED_SECRET}', 'http://localhost/webhook', false, 'ACTIVE', now(), now()),
('00000000-0000-0000-0000-000000000102', 'CLIENT-RUMAH-SAKIT-ISLAM', 'Rumah Sakit Islam', 'CLIENT-RUMAH-SAKIT-ISLAM', '{ENCRYPTED_SECRET}', 'http://localhost/webhook', false, 'ACTIVE', now(), now()),
('00000000-0000-0000-0000-000000000103', 'CLIENT-FOUNDATION-PEDULI', 'Foundation Peduli', 'CLIENT-FOUNDATION-PEDULI', '{ENCRYPTED_SECRET}', 'http://localhost/webhook', false, 'ACTIVE', now(), now());
"""
run_psql(consumers_sql)

# 3. Ensure escrows with correct prefix and digit length (11 digits), client_secret NULL
escrows_sql = """
INSERT INTO escrow_account (id, code, provider, hosting_model, transport, auth_scheme, active_environment, enabled, settlement_account_number, settlement_account_name, company_id, va_prefix, va_digit_length, created_at, updated_at) VALUES
('00000000-0000-0000-0000-000000000201', 'MAYBANK', 'maybank', 'SELF_HOSTED', 'REST_JSON', 'SNAP', 'SANDBOX', true, '1234567890', 'Maybank Settlement', '8888', '88', 11, now(), now()),
('00000000-0000-0000-0000-000000000202', 'BSI',     'bsi',     'SELF_HOSTED', 'REST_JSON', 'PROPRIETARY', 'SANDBOX', true, '1234567890', 'BSI Settlement',     '9999', '99', 11, now(), now()),
('00000000-0000-0000-0000-000000000203', 'CIMB',    'cimb',    'SELF_HOSTED', 'SOAP_XML',  'PROPRIETARY', 'SANDBOX', true, '1234567890', 'CIMB Settlement',    '7777', '77', 11, now(), now()),
('00000000-0000-0000-0000-000000000204', 'BCA',     'bca',     'SELF_HOSTED', 'REST_JSON', 'PROPRIETARY', 'SANDBOX', true, '1234567890', 'BCA Settlement',     '1111', '11', 11, now(), now()),
('00000000-0000-0000-0000-000000000205', 'BNI',     'bni',     'SELF_HOSTED', 'REST_JSON', 'PROPRIETARY', 'SANDBOX', true, '1234567890', 'BNI Settlement',     '2222', '22', 11, now(), now()),
('00000000-0000-0000-0000-000000000206', 'BRI',     'bri',     'SELF_HOSTED', 'REST_JSON', 'PROPRIETARY', 'SANDBOX', true, '1234567890', 'BRI Settlement',     '3333', '33', 11, now(), now());
"""
run_psql(escrows_sql)

# 4. Insert charges and VAs from seed-data.json
with open(SEED_FILE) as f:
    items = json.load(f)

for item in items:
    cid = item["chargeId"]
    client_id = item["clientId"]
    ctype = item["chargeType"]
    tot = item["totalAmount"]
    desc = item.get("description", "Seed charge").replace("'", "''")

    charge_sql = f"""
    INSERT INTO charge (id, id_consumer, consumer_reference, payer_name, charge_type, amount, cumulative_paid, status, description, created_at, updated_at)
    SELECT '{cid}', id, '{cid}', '{client_id}', '{ctype}', {tot}, 0.00, 'ACTIVE', '{desc}', now(), now()
    FROM consumer WHERE client_id = '{client_id}';
    """
    run_psql(charge_sql)

    for va in item.get("siblingVas", []):
        bank = va["bankCode"]
        vanum = va["vaNumber"]
        va_sql = f"""
        INSERT INTO virtual_account (id, id_charge, id_escrow_account, va_number, status, created_at, updated_at)
        SELECT gen_random_uuid(), '{cid}', e.id, '{vanum}', 'ACTIVE', now(), now()
        FROM escrow_account e WHERE e.code = '{bank}';
        """
        run_psql(va_sql)

print("Direct database seeding complete!")
