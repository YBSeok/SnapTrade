import ws from 'k6/ws';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const tickerLatency = new Trend('ticker_latency_ms');
const klineLatency = new Trend('kline_latency_ms');
const messageCounter = new Counter('stomp_messages_received');
const errorRate = new Rate('stomp_errors');

export const options = {
    vus: 1000,
    duration: '60s',
};

export default function () {
    const url = 'ws://127.0.0.1:8080/ws';
    const params = { tags: { my_tag: 'public_ticker_test' } };

    const res = ws.connect(url, params, function (socket) {

        // 웹소켓 연결 성공 직후 STOMP CONNECT 프레임 전송
        socket.on('open', function () {
            const connectFrame = "CONNECT\naccept-version:1.1,1.2\nheart-beat:10000,10000\n\n\x00";
            socket.send(connectFrame);
        });

        // 서버로부터 메시지 수신 시 라우팅 및 지연 시간 측정 로직
        socket.on('message', function (msg) {
            if (msg === '\n') return;

            // 1. 연결 성공 시 모든 마켓(1~50) 구독
            if (msg.startsWith('CONNECTED')) {
                for (let i = 1; i <= 50; i++) {
                    socket.send(`SUBSCRIBE\nid:sub-t-${i}\ndestination:/topic/ticker/${i}\n\n\x00`);
                }
                // Kline 1번 마켓 구독
                socket.send(`SUBSCRIBE\nid:sub-k-1\ndestination:/topic/kline/1/1m\n\n\x00`);
            }

            // 2. 메시지 수신 시 라우팅
            if (msg.startsWith('MESSAGE')) {
                messageCounter.add(1);

                // 헤더 파싱을 통해 destination 확인
                const headerEnd = msg.indexOf('\n\n');
                const headers = msg.substring(0, headerEnd);
                const bodyStr = msg.substring(headerEnd + 2, msg.lastIndexOf('\x00'));

                try {
                    const payload = JSON.parse(bodyStr);
                    const now = Date.now();

                    // 경로별 지연 시간 측정
                    if (headers.includes('/topic/ticker/')) {
                        if (payload.timestamp) tickerLatency.add(now - payload.timestamp);
                    } else if (headers.includes('/topic/kline/')) {
                        if (payload.timestamp) klineLatency.add(now - payload.timestamp);
                    }
                } catch (e) {
                    // JSON 파싱 에러는 무시
                }
            }
        });

        socket.on('close', function () {
            // 소켓 종료
        });

        socket.on('error', function (e) {
            if (e.error() !== "websocket: close sent") {
                console.log('WebSocket connection error: ', e.error());
                errorRate.add(1);
            }
        });

    });

    check(res, { 'WebSocket handshake status is 101': (r) => r && r.status === 101 });
}