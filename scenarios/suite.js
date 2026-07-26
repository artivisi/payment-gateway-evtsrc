import http from 'k6/http';
import { check, sleep } from 'k6';

// Unified k6 Load Test Suite Reusable for both CQRS and RDBMS Gateways
// Command: k6 run -e TARGET_URL=http://localhost:8080 -e SCENARIO=callback_peak scenarios/suite.js

const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080';

// Pre-seeded Virtual Accounts across multiple banks (MAYBANK, BSI, CIMB, BCA, BNI, BRI)
const SEEDED_VAS = [
    { chargeId: 'CHG-TAZKIA-2026-001', bankCode: 'MAYBANK', vaNumber: '88012026001', amount: 2500000.00 },
    { chargeId: 'CHG-TAZKIA-2026-001', bankCode: 'BSI', vaNumber: '99012026001', amount: 2500000.00 },
    { chargeId: 'CHG-TAZKIA-2026-001', bankCode: 'CIMB', vaNumber: '77012026001', amount: 2500000.00 },
    { chargeId: 'CHG-TAZKIA-2026-001', bankCode: 'BCA', vaNumber: '11012026001', amount: 2500000.00 },
    { chargeId: 'CHG-TAZKIA-2026-001', bankCode: 'BNI', vaNumber: '22012026001', amount: 2500000.00 },
    { chargeId: 'CHG-TAZKIA-2026-001', bankCode: 'BRI', vaNumber: '33012026001', amount: 2500000.00 },
    
    { chargeId: 'CHG-TAZKIA-2026-002', bankCode: 'MAYBANK', vaNumber: '88012026002', amount: 3200000.00 },
    { chargeId: 'CHG-TAZKIA-2026-002', bankCode: 'BSI', vaNumber: '99012026002', amount: 3200000.00 },
    { chargeId: 'CHG-TAZKIA-2026-002', bankCode: 'BCA', vaNumber: '11012026002', amount: 3200000.00 },
    
    { chargeId: 'CHG-TAZKIA-2026-003', bankCode: 'MAYBANK', vaNumber: '88012026003', amount: 50000.00 },
    { chargeId: 'CHG-TAZKIA-2026-003', bankCode: 'BSI', vaNumber: '99012026003', amount: 100000.00 },
    
    { chargeId: 'CHG-RSI-2026-001', bankCode: 'MAYBANK', vaNumber: '88022026001', amount: 1750000.00 },
    { chargeId: 'CHG-RSI-2026-001', bankCode: 'BCA', vaNumber: '11022026001', amount: 1750000.00 },
    { chargeId: 'CHG-RSI-2026-001', bankCode: 'BNI', vaNumber: '22022026001', amount: 1750000.00 },
    
    { chargeId: 'CHG-FOUNDATION-001', bankCode: 'MAYBANK', vaNumber: '88032026001', amount: 25000.00 },
    { chargeId: 'CHG-FOUNDATION-001', bankCode: 'BSI', vaNumber: '99032026001', amount: 50000.00 },
    { chargeId: 'CHG-FOUNDATION-001', bankCode: 'CIMB', vaNumber: '77032026001', amount: 100000.00 }
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
                { duration: '30s', target: 500 },  // Ramp to 500 TPS
                { duration: '1m', target: 1000 },  // Ramp to 1,000 TPS
                { duration: '1m', target: 2000 },  // Ramp to 2,000 TPS peak burst
                { duration: '30s', target: 0 },    // Ramp down
            ],
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'], // <1% errors
        http_req_duration: ['p(99)<500'], // p99 < 500ms
    },
};

export default function () {
    const item = SEEDED_VAS[Math.floor(Math.random() * SEEDED_VAS.length)];
    const bankRef = `REF-${Date.now()}-${Math.floor(Math.random() * 1000000)}`;
    
    const payload = JSON.stringify({
        chargeId: item.chargeId,
        bankCode: item.bankCode,
        vaNumber: item.vaNumber,
        bankReference: bankRef,
        amount: item.amount,
        paymentTimestamp: new Date().toISOString()
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(`${TARGET_URL}/api/v1/payments`, payload, params);

    check(res, {
        'status is 200 or 201': (r) => r.status === 200 || r.status === 201,
        'response time < 10ms': (r) => r.timings.duration < 10,
    });

    sleep(0.01);
}
