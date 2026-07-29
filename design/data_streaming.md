---
title: data-streaming
target-version: 0.1.0
---

# Data Streaming

This document describes how SnapTrade turns completed trades into live **ticker** and **kline (candlestick)** streams for clients.

## Summary

When a trade is matched and journaled, the engine publishes to Kafka topic `trade.completed`.  
`MarketDataService` consumes that topic asynchronously, updates in-memory market state, and broadcasts over **STOMP WebSocket** topics.

Clients subscribe to public channels such as:

- `/topic/ticker/{marketId}`
- `/topic/kline/{marketId}/{interval}`

Persistence of ticker / kline snapshots happens on a short schedule so the hot path stays memory-first.

### Goals

- Explain the path from trade → market buffers → WebSocket topics.
- Document which intervals are “hot” and how DB flush works.
- Clarify that this is **public** market data (not private user notifications).

### Non-Goals

- Private order / fill notifications (see [Notification](notification.md)).
- Full historical chart API design.
- Order book depth streaming (not implemented in this path).

## Design

### Overview

```
Publisher (Matching Engine)
        │
        ▼
 Kafka topic: trade.completed
        │
        ▼
 MarketDataService.onTradeCompleted()   (@KafkaListener)
        │
        ├── update tickerBuffer  ──► MarketBroadcastService.broadcastTicker
        │                              → /topic/ticker/{marketId}
        │
        └── update klineBuffer   ──► MarketBroadcastService.broadcastKline
                                       → /topic/kline/{marketId}/{interval}

 Scheduled flush (1s)  → UPSERT tickers (+ non-1s klines) to DB
 Scheduled cache (60s) → refresh 24h open price from past klines
```

### Main components

| Component | Package | Role |
|-----------|---------|------|
| `MarketDataService` | `market.service` | Consume trades, maintain buffers, schedule flush |
| `MarketBroadcastService` | `common.websocket` | STOMP `convertAndSend` for ticker / kline |
| `Ticker` / `Kline` | `market.domain` | In-memory / DB entities |
| `TickerBroadcastDTO` / `KlineBroadcastDTO` | `market.dto` | Wire payloads |
| `WebSocketConfig` | `common.websocket` | STOMP endpoint `/ws`, broker `/topic`, `/queue` |
| `TradeCompletedMessage` | `common.kafka` | Kafka payload from engine Publisher |

### WebSocket / STOMP setup

`WebSocketConfig`:

- Endpoint: `/ws`
- Application prefix: `/app`
- Broker destinations: `/topic`, `/queue`
- User destination prefix: `/user` (used by private notifications, not this doc)

Public market streams use **`/topic/...`** (broadcast to all subscribers).

### Ticker stream

On each trade:

1. Load or create `Ticker` for `marketId` in `tickerBuffer`.
2. Update last price, volume aggregates, and 24h change inputs.
3. Broadcast `TickerBroadcastDTO` to:

```
/topic/ticker/{marketId}
```

24h open price comes from `openPrice24hCache` (refreshed every 60 seconds from historical kline close). If missing, the first seen trade price is used as a fallback.

### Kline stream

“Hot” intervals updated on every trade:

- `1s` (`ChartInterval.SEC_1`)
- `1m` (`ChartInterval.MIN_1`)

For each hot interval:

1. Compute candle `openTimeMs` for the current trade time.
2. If no candle or the candle rolled over → start a new `Kline` (old candle may be queued for DB).
3. Else update OHLC / volume on the existing candle.
4. Broadcast to:

```
/topic/kline/{marketId}/{interval}
```

Example: `/topic/kline/1/1m`

### Persistence strategy

Market streaming is **memory-first**:

| Job | Interval | Behavior |
|-----|----------|----------|
| `flushMarketDataToDb` | 1 second | Batch UPSERT tickers; UPSERT klines except `1s` |
| `refreshOpenPrice24hCache` | 60 seconds | Find closest past close for 24h open |

`1s` candles are useful for live UI but are skipped in the DB flush to reduce write load.

### Relationship to other trade consumers

The same `trade.completed` topic is also consumed by:

- `AccountService` — balance settlement ([Ledger](ledger.md))
- `StopLimitTriggerEngine` — may enqueue stop orders
- (Separately) `order.lifecycle` drives private notifications

Market streaming must stay **independent**: a slow chart flush must not block matching, and must not depend on private notification delivery.

## Risks and Mitigation

| Risk | Mitigation |
|------|------------|
| Burst of trades floods WebSocket outbound | Async broadcast executor; consider coalescing later if needed |
| Client reconnect loses in-flight ticks | Client re-subscribes; REST/history APIs can backfill (future) |
| DB flush falls behind | In-memory buffers remain source for live stream; flush is best-effort snapshot |
| Wrong 24h change briefly after restart | Cache refresh job; fallback to first trade price |

## Design Decisions

| Decision | Reason |
|----------|--------|
| Drive streams from `trade.completed` | Single Kafka fan-out after journal; no extra coupling inside matching |
| STOMP topics per market / interval | Simple subscribe model for clients |
| Keep 1s candles in memory only | High write volume, low long-term value for persistence |
| Separate `MarketBroadcastService` | Keep public market send path distinct from private notifications |

## Alternatives Considered

| Alternative | Why not (for now) |
|-------------|-------------------|
| Push market data from inside Matching handler | Would mix SoT matching with I/O and fan-out latency |
| Raw WebSocket without STOMP | Possible, but STOMP already used for private user queues |
| Stream every chart interval live | Costly; only hot intervals are updated per trade |