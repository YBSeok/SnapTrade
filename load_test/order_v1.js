import http from 'k6/http';
import { check } from 'k6';

export const options = {
    scenarios: {
        constant_tps_test: {
            executor: 'constant-arrival-rate',
            rate: 10000,
            timeUnit: '1s',
            duration: '30s',
            preAllocatedVUs: 500,
            maxVUs: 2000,
        },
    },
};

export default function () {
    const url = 'http://127.0.0.1:8080/api/v1/orders';

    const payload = JSON.stringify({
        userId: __VU,
        marketId: 1,
        side: 'BUY',
        orderType: 'LIMIT',
        timeInForce: 'GTC',
        price: 50000,
        quantity: 1
    });

    const params = {
        headers: { 'Content-Type': 'application/json' },
    };

    const res = http.post(url, payload, params);

    check(res, {
        'is status 202': (r) => r.status === 202,
    });
}