import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';
import { Counter } from 'k6/metrics';

// k6 Load Test Suite for the sibling relational payment-gateway (RDBMS baseline), targeting its
// real production BSI adapter (/api/bank/bsi). This is the RDBMS-side counterpart to
// scenarios/suite-bsi.js - same request shape, same checksum scheme, same six BSI VA/amount
// pairs, same ramping-arrival-rate profile, so the two systems are benchmarked through an
// identical workload. See docs/benchmark-remediation-guideline.md findings F5/F7 for why the
// previous two scripts were NOT comparable, and why this one previously computed its checksum
// against the literal string "null" (the escrow's client_secret was NULL in the seeded DB, and
// Java string concatenation of a null String produces the text "null" - the exact bug this fixes).
//
// -------------------------------------------------------------------------------------------
// Producing traceable artifacts (per docs/benchmark-remediation-guideline.md, G6): both RUN_ID
// and BSI_SHARED_SECRET are required environment variables with no default - the script throws
// in the init stage if either is missing. BSI_SHARED_SECRET must match the target escrow's real
// (non-NULL) client_secret.
//
//   export RUN_ID=$(date +%Y%m%d%H%M%S)
//   export BSI_SHARED_SECRET=<the real plaintext secret configured on the BSI escrow>
//
//   k6 run \
//     -e RUN_ID="$RUN_ID" \
//     -e BSI_SHARED_SECRET="$BSI_SHARED_SECRET" \
//     -e TARGET_URL=http://localhost:8080 \
//     --summary-export=scenarios/results/$(date +%Y-%m-%d)-rdbms-summary.json \
//     --out json=scenarios/results/$(date +%Y-%m-%d)-rdbms-raw.json \
//     scenarios/suite-rdbms.js
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
        'BSI_SHARED_SECRET environment variable is required and must match the target BSI ' +
        "escrow's real client_secret. No default: an empty/missing secret makes the checksum " +
        'trivially forgeable, which is the exact bug this script previously had (checksum computed ' +
        'against the literal string "null" because the seeded escrow secret was NULL).'
    );
}

// Same six BSI VA/amount pairs as scenarios/suite-bsi.js's PAYMENT_AMOUNT_BY_VA, from
// scenarios/seed-data.json: CLOSED VAs paid their full amount (settle - and start rejecting - on
// the first accepted callback); OPEN/INSTALLMENT VAs paid a fraction of their total so they keep
// accepting payments for the whole run instead of closing partway through.
const BSI_VAS = [
    { vaNumber: '99012026001', amount: 2500000.00 }, // CLOSED  2,500,000 - full amount
    { vaNumber: '99012026002', amount: 3200000.00 }, // CLOSED  3,200,000 - full amount
    { vaNumber: '99012026003', amount: 50000.00 },   // OPEN    5,000,000 total - partial, never fills
    { vaNumber: '99012026004', amount: 1000000.00 }, // INSTALLMENT 6,000,000 total - partial
    { vaNumber: '99022026002', amount: 450000.00 },  // CLOSED    450,000 - full amount
    { vaNumber: '99032026001', amount: 100000.00 },  // OPEN   10,000,000 total - partial, never fills
];

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
// reading server logs. Same metric name/tag shape as scenarios/suite-bsi.js so
// verify-correctness.py handles both systems identically.
const paymentOutcomes = new Counter('payment_outcomes');

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
 * Maps the BSI wire responseCode back to a coarse outcome label - identical mapping to
 * scenarios/suite-bsi.js's classifyOutcome() so both systems' logs use the same vocabulary.
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

export default function () {
    const item = BSI_VAS[Math.floor(Math.random() * BSI_VAS.length)];
    const idTransaksi = `${RUN_ID}-${Date.now()}-${Math.floor(Math.random() * 1000000)}`;
    const tanggalTransaksi = bsiTimestamp();

    // BsiChecksum: SHA1(nomorPembayaran + sharedSecret + tanggalTransaksi)
    const rawChecksum = item.vaNumber + BSI_SHARED_SECRET + tanggalTransaksi;
    const checksum = crypto.sha1(rawChecksum, 'hex');

    const payload = JSON.stringify({
        action: 'payment',
        nomorPembayaran: item.vaNumber,
        nilai: item.amount,
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
