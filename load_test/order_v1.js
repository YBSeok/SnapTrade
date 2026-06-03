import http from 'k6/http';
import { check } from 'k6';

export const options = {
    vus: 100,
    iterations: 100,
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
        'is status 200': (r) => r.status === 200,
    });
}