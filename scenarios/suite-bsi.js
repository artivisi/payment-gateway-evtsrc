import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';
import { Counter } from 'k6/metrics';

// k6 Load Test Suite for payment-gateway-evtsrc, targeting the real BSI proprietary adapter
// (/api/bank/bsi) with the SAME request shape and checksum scheme as the sibling relational
// repo's scenarios/suite-rdbms.js: SHA1(nomorPembayaran + secret + tanggalTransaksi), the same
// six BSI VA/amount pairs, the same ramping-arrival-rate profile. This is the ONLY evtsrc script
// used for the head-to-head comparison from now on - scenarios/suite.js is retired (it hit a
// generic, unauthenticated, unrelated endpoint; see docs/benchmark-remediation-guideline.md
// findings F1/F5/F7).
//
// evtsrc's source of truth is Kafka/RocksDB, not Postgres, so seeding is done here via setup()
// calling the real POST /api/v1/charges endpoint (the old scenarios/seed-db-direct.py pattern of
// writing rows directly into Postgres is architecturally wrong for this system - that table is a
// downstream projection, not the source of truth).
//
// -------------------------------------------------------------------------------------------
// Producing traceable artifacts (per docs/benchmark-remediation-guideline.md, G6): both
// RUN_ID and BSI_SHARED_SECRET are required environment variables with no default - the script
// throws in the init stage if either is missing. RUN_ID prefixes every idTransaksi/bankReference
// this run generates so a later audit can filter one run out of an accumulating table.
// BSI_SHARED_SECRET must match the target app's app.bank-secrets.secrets.bsi (BANK_SECRET_BSI env
// var on the app) - an empty/missing secret is exactly the bug that made the old checksum "work"
// for the wrong reason (see BankSecretProperties's javadoc).
//
//   export RUN_ID=$(date +%Y%m%d%H%M%S)
//   export BSI_SHARED_SECRET=<same value as BANK_SECRET_BSI on the running app>
//
//   k6 run \
//     -e RUN_ID="$RUN_ID" \
//     -e BSI_SHARED_SECRET="$BSI_SHARED_SECRET" \
//     -e TARGET_URL=http://localhost:8080 \
//     --summary-export=scenarios/results/$(date +%Y-%m-%d)-evtsrc-summary.json \
//     --out json=scenarios/results/$(date +%Y-%m-%d)-evtsrc-raw.json \
//     scenarios/suite-bsi.js
//
// scenarios/run-benchmark.sh wraps the above (and creates scenarios/results/ if missing).
// -------------------------------------------------------------------------------------------

const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080';

const RUN_ID = __ENV.RUN_ID;
if (!RUN_ID) {
    throw new Error(
        'RUN_ID environment variable is required (e.g. -e RUN_ID=$(date +%Y%m%d%H%M%S)) so this ' +
        "run's payments can be filtered out of an accumulating audit table. No default."
    );
}

const BSI_SHARED_SECRET = __ENV.BSI_SHARED_SECRET;
if (!BSI_SHARED_SECRET) {
    throw new Error(
        'BSI_SHARED_SECRET environment variable is required and must match the target app\'s ' +
        'app.bank-secrets.secrets.bsi (BANK_SECRET_BSI). No default: an empty/missing secret ' +
        'makes the checksum trivially forgeable, which is the exact bug this replaces.'
    );
}

// Payment amount to send per BSI VA, identical to the six-entry BSI_VAS list hardcoded in
// scenarios/suite-rdbms.js, so both systems receive the same per-request payloads. CLOSED charges
// are paid their full totalAmount (settles - and starts rejecting - on the first accepted
// callback). OPEN/INSTALLMENT charges are paid a fraction of totalAmount so they keep accepting
// payments for the whole run instead of closing partway through (G5: a real accepted-payment
// stream, not a workload that degenerates into all-rejects).
const PAYMENT_AMOUNT_BY_VA = {
    '99012026001': 2500000.00, // CLOSED  2,500,000 - full amount
    '99012026002': 3200000.00, // CLOSED  3,200,000 - full amount
    '99012026003': 50000.00,   // OPEN    5,000,000 total - partial, never fills
    '99012026004': 1000000.00, // INSTALLMENT 6,000,000 total - partial
    '99022026002': 450000.00,  // CLOSED    450,000 - full amount
    '99032026001': 100000.00,  // OPEN   10,000,000 total - partial, never fills
};

// k6's open() is only usable in the init stage (top-level, per-VU init), not inside setup()/
// default() - read and parse the seed file once here, at module load time.
const SEED_DATA = JSON.parse(open('./seed-data.json'));

export const options = {
    scenarios: {
        bank_callbacks: {
            executor: 'ramping-arrival-rate',
            startRate: 50,
            timeUnit: '1s',
            preAllocatedVUs: 100,
            maxVUs: 2000,
            stages: [
                { duration: '15s', target: 500 },  // Ramp to 500 TPS
                { duration: '30s', target: 1000 }, // Ramp to 1,000 TPS
                { duration: '30s', target: 2000 }, // Ramp to 2,000 TPS peak burst
                { duration: '15s', target: 0 },    // Ramp down
            ],
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'], // <1% transport-level errors (BSI rejections are HTTP 200
                                         // with an in-body responseCode - see payment_outcomes).
        http_req_duration: ['p(99)<500'], // p99 < 500ms
    },
};

// Custom metric for post-hoc auditing: the BSI wire format returns HTTP 200 for both accepted
// payments and business rejections, so http_req_failed cannot tell them apart. This is the only
// record of "what actually happened" per request available to an external auditor without
// reading server logs.
const paymentOutcomes = new Counter('payment_outcomes');

/**
 * Seeds one CLOSED/OPEN/INSTALLMENT charge (with a single BSI sibling VA) per BSI-bank entry in
 * seed-data.json via the real POST /api/v1/charges endpoint, then polls GET /api/v1/charges/{id}
 * until each one hydrates to ACTIVE in RocksDB before handing the resolved VA list to default().
 *
 * Fails loudly (throws) on any creation error or on hydration timeout - the default() function
 * must never run against an empty or partially-resolved VA list.
 */
export function setup() {
    const bsiCharges = SEED_DATA
        .map((charge) => ({ charge, bsiVa: (charge.siblingVas || []).find((va) => va.bankCode === 'BSI') }))
        .filter((entry) => entry.bsiVa !== undefined);

    if (bsiCharges.length === 0) {
        throw new Error('setup(): no BSI-bank entries found in seed-data.json - nothing to benchmark');
    }

    const jsonHeaders = { headers: { 'Content-Type': 'application/json' } };
    const resolved = [];

    for (const { charge, bsiVa } of bsiCharges) {
        const paymentAmount = PAYMENT_AMOUNT_BY_VA[bsiVa.vaNumber];
        if (paymentAmount === undefined) {
            throw new Error(
                `setup(): no payment amount configured for BSI VA ${bsiVa.vaNumber} in ` +
                'PAYMENT_AMOUNT_BY_VA - seed-data.json has drifted from suite-rdbms.js\'s VA list'
            );
        }

        const createPayload = JSON.stringify({
            clientId: charge.clientId,
            chargeType: charge.chargeType,
            totalAmount: charge.totalAmount,
            description: charge.description,
            siblingVas: [{ bankCode: 'BSI', vaNumber: bsiVa.vaNumber }],
        });

        const createRes = http.post(`${TARGET_URL}/api/v1/charges`, createPayload, jsonHeaders);
        if (createRes.status !== 201) {
            throw new Error(
                `setup(): failed to create charge for BSI VA ${bsiVa.vaNumber}: ` +
                `HTTP ${createRes.status} - ${createRes.body}`
            );
        }

        const created = JSON.parse(createRes.body);
        if (!created.chargeId) {
            throw new Error(
                `setup(): charge creation response for BSI VA ${bsiVa.vaNumber} carried no ` +
                `chargeId: ${createRes.body}`
            );
        }

        resolved.push({ vaNumber: bsiVa.vaNumber, chargeId: created.chargeId, paymentAmount });
    }

    // Kafka Streams hydration (charge-events/va-events -> RocksDB charge-state-store /
    // va-registry-store) is asynchronous relative to the REST calls above. k6 has no Awaitility -
    // poll with a bounded retry loop and fail loud on timeout rather than silently proceeding
    // with VAs that would resolve as INVALID_VA under load.
    const maxAttempts = 50;
    const pollIntervalSeconds = 0.2; // 50 * 0.2s = 10s per charge, matching the Java-side default

    for (const entry of resolved) {
        let hydrated = false;
        for (let attempt = 0; attempt < maxAttempts && !hydrated; attempt++) {
            const res = http.get(`${TARGET_URL}/api/v1/charges/${entry.chargeId}`);
            if (res.status === 200) {
                const body = JSON.parse(res.body);
                if (body.status === 'ACTIVE') {
                    hydrated = true;
                    break;
                }
            }
            sleep(pollIntervalSeconds);
        }
        if (!hydrated) {
            throw new Error(
                `setup(): charge ${entry.chargeId} (BSI VA ${entry.vaNumber}) did not hydrate to ` +
                `ACTIVE within ${(maxAttempts * pollIntervalSeconds).toFixed(1)}s - aborting rather ` +
                'than running the load test against an unresolved VA'
            );
        }
    }

    return { vas: resolved };
}

/** yyyyMMddHHmmss in Asia/Jakarta (UTC+7, no DST) - the format BsiAdapterController parses. */
function bsiTimestamp() {
    const WIB_OFFSET_MS = 7 * 60 * 60 * 1000;
    const wib = new Date(Date.now() + WIB_OFFSET_MS);
    const pad = (n) => String(n).padStart(2, '0');
    return (
        `${wib.getUTCFullYear()}${pad(wib.getUTCMonth() + 1)}${pad(wib.getUTCDate())}` +
        `${pad(wib.getUTCHours())}${pad(wib.getUTCMinutes())}${pad(wib.getUTCSeconds())}`
    );
}

/**
 * Maps the BSI wire responseCode back to a coarse PaymentOutcome-shaped label. The wire protocol
 * cannot express everything the internal enum can:
 *  - "00" covers both ACCEPTED and DUPLICATE (a replayed idTransaksi still gets SUCCESS).
 *  - "13" covers both REJECTED_CHARGE_CLOSED and REJECTED_INVALID_AMOUNT.
 * An auditor who needs the finer split must cross-check the idempotency-store / RocksDB state
 * directly, not this log alone.
 */
function classifyOutcome(res) {
    if (res.status !== 200) {
        return `HTTP_${res.status}`;
    }
    let body;
    try {
        body = JSON.parse(res.body);
    } catch (e) {
        return 'UNPARSEABLE_RESPONSE';
    }
    switch (body.responseCode) {
        case '00': return 'ACCEPTED_OR_DUPLICATE';
        case '03': return 'REJECTED_INVALID_VA';
        case '13': return 'REJECTED_CHARGE_CLOSED_OR_INVALID_AMOUNT';
        case '30': return 'REJECTED_INVALID_REQUEST';
        case '25': return 'REJECTED_INVALID_CHECKSUM';
        case '12': return 'REJECTED_INVALID_ACTION';
        case '99': return 'ERROR';
        default: return `UNMAPPED_RESPONSE_CODE_${body.responseCode}`;
    }
}

export default function (data) {
    const vas = data.vas;
    const item = vas[Math.floor(Math.random() * vas.length)];

    const idTransaksi = `${RUN_ID}-${Date.now()}-${Math.floor(Math.random() * 1000000)}`;
    const tanggalTransaksi = bsiTimestamp();

    // BsiChecksum: SHA1(nomorPembayaran + sharedSecret + tanggalTransaksi)
    const rawChecksum = item.vaNumber + BSI_SHARED_SECRET + tanggalTransaksi;
    const checksum = crypto.sha1(rawChecksum, 'hex');

    const payload = JSON.stringify({
        action: 'payment',
        nomorPembayaran: item.vaNumber,
        nilai: item.paymentAmount,
        idTransaksi: idTransaksi,
        tanggalTransaksi: tanggalTransaksi,
        checksum: checksum,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(`${TARGET_URL}/api/bank/bsi`, payload, params);

    check(res, {
        'status is 200': (r) => r.status === 200,
    });

    paymentOutcomes.add(1, {
        bankReference: idTransaksi,
        outcome: classifyOutcome(res),
        httpStatus: String(res.status),
    });

    sleep(0.01);
}
