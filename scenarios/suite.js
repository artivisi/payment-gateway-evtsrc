import http from 'k6/http';
import { check, sleep } from 'k6';

// Unified k6 Load Test Suite Reusable for both CQRS and RDBMS Gateways
// Command: k6 run -e TARGET_URL=http://localhost:8080 -e SCENARIO=callback_peak scenarios/suite.js

const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const SCENARIO = __ENV.SCENARIO || 'callback_peak';

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
    const chargeId = `CHG-${Math.floor(Math.random() * 100000)}`;
    const bankRef = `REF-${Math.floor(Math.random() * 10000000)}`;
    
    const payload = JSON.stringify({
        chargeId: chargeId,
        bankCode: 'MAYBANK',
        vaNumber: '8801928371',
        bankReference: bankRef,
        amount: 500000.00,
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
