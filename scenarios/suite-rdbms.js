import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';

// k6 Load Test Suite targeting production BSI bank adapter on payment-gateway (RDBMS)
// Target URL: http://localhost:8080/api/bank/bsi

const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080';

// Seeded BSI Virtual Accounts from seed-data.json
const BSI_VAS = [
    { vaNumber: '99012026001', amount: 2500000.00 },
    { vaNumber: '99012026002', amount: 3200000.00 },
    { vaNumber: '99012026003', amount: 50000.00 },
    { vaNumber: '99012026004', amount: 1000000.00 },
    { vaNumber: '99022026002', amount: 450000.00 },
    { vaNumber: '99032026001', amount: 100000.00 },
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
        http_req_failed: ['rate<0.01'], // <1% errors
        http_req_duration: ['p(99)<500'], // p99 < 500ms
    },
};

export default function () {
    const item = BSI_VAS[Math.floor(Math.random() * BSI_VAS.length)];
    const bankRef = `REF-${Date.now()}-${Math.floor(Math.random() * 1000000)}`;
    const txTime = '20260727103000';

    // BsiChecksum: SHA1(nomorPembayaran + null + tanggalTransaksi)
    const rawChecksum = item.vaNumber + 'null' + txTime;
    const checksum = crypto.sha1(rawChecksum, 'hex');

    const payload = JSON.stringify({
        action: 'payment',
        nomorPembayaran: item.vaNumber,
        nilai: item.amount,
        idTransaksi: bankRef,
        tanggalTransaksi: txTime,
        checksum: checksum
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(`${TARGET_URL}/api/bank/bsi`, payload, params);

    check(res, {
        'status is 200': (r) => r.status === 200,
        'response time < 50ms': (r) => r.timings.duration < 50,
    });

    sleep(0.01);
}
