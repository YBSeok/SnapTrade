---
title: matching-engine
target-version: 0.1.0
---

# Matching Engine

This document describes how SnapTrade accepts orders, matches them, journals the results, and updates the order read model.

## Summary

The matching engine is the core of SnapTrade. When a user places an order, the system must:

1. Validate and reserve balance quickly.
2. Match the order against the in-memory order book.
3. Persist the outcome so the system can recover after a restart.
4. Notify other parts of the system (balances, market data, user alerts) without slowing down matching.

We solve this with an **LMAX Disruptor pipeline** inside one JVM process, plus a small **async worker** for the `orders` read model.

### Goals

- Help new contributors understand the order path from HTTP to journal to side effects.
- Explain why matching, journaling, and projection are separated.
- Document recovery (event replay) and how clients cope with projection lag.

### Non-Goals

- This document does not describe Kafka or multi-process deployment (future option).
- This document does not cover stop-limit trigger details in depth.
- This document does not define frontend UX beyond the status polling contract.

## Design

### High-level flow

```
Client
  │  POST /api/v1/orders
  ▼
OrderController ──► OrderPreProcessor (validate + pre-trade hold)
  │
  ▼
EventSourcedMatchingEngine.placeOrder()
  │  returns orderId immediately (HTTP 202)
  ▼
┌─────────────────────────────────────────────┐
│  Disruptor (BusySpin × 3) — hot path only   │
│    1. Matching  → in-memory OrderBook       │
│    2. Journal   → INSERT trades +           │
│                   order_events (SoT)        │
│    3. Publisher → Kafka produce (async)     │
│                   (no orders UPSERT here)   │
└─────────────────────────────────────────────┘
         │
         ▼
┌──────────────── Kafka ────────────────┐
│  order.projection → ProjectionWorker  │
│                     → UPSERT orders   │
│  trade.completed  → Account / Market  │
│                     / StopTrigger     │
│  order.lifecycle  → Notification WS   │
└───────────────────────────────────────┘
```

Projection was **removed** from the Disruptor. Publisher only produces Kafka messages; `orders` UPSERT and other side effects run in independent consumer groups.
### Main components

| Component | Package | Role |
|-----------|---------|------|
| `OrderController` | `engine.controller` | HTTP entry: place, cancel, status poll |
| `OrderPreProcessor` | `engine.service` | Idempotency, tick/qty rules, pre-trade hold |
| `EventSourcedMatchingEngine` | `engine.service` | Disruptor pipeline + book recovery |
| `OrderBook` | `engine.domain` | In-memory price-time priority book |
| `EventExecutionRepository` | `engine.repository` | Journal INSERT + projection UPSERT |
| `OrderProjectionWorker` | `engine.service` | Kafka consumer for `orders` UPSERT |
| `OrderQueryService` | `engine.service` | Lightweight status read for polling |

### 1. Accepting an order

`POST /api/v1/orders` creates an `OrderTrace` and calls `OrderPreProcessor.validateAndEnqueue`.

Before the engine sees the order, the preprocessor:

1. Checks **idempotency** with `clientOrderId` (in-memory NX, 60s TTL).
2. Validates **tick size**, **min quantity**, **step size**, and **min notional** using `MarketMetadataCache`.
3. Performs a **pre-trade hold** via `InMemoryBalanceRepository` so the user cannot overspend while matching runs.

Then `EventSourcedMatchingEngine.placeOrder`:

1. Allocates a TSID `orderId` and a per-market `sequenceNo`.
2. Publishes a `PLACE` command onto the Disruptor ring buffer.
3. Returns `orderId` to the client immediately as **HTTP 202 Accepted**.

Matching is asynchronous relative to the HTTP response. The client receives an id it can poll.

Cancel uses `DELETE /api/v1/orders/{orderId}` and publishes a `CANCEL` command the same way.

### 2. Disruptor pipeline

On application ready, the engine warms sequence counters, **replays** `order_events` into memory, then starts the Disruptor.

Configuration (current):

- Ring buffer size: `65536`
- Producer type: `MULTI`
- Wait strategy: `BusySpinWaitStrategy` (low latency, higher CPU)

Handlers run in order:

#### Stage 1 — Matching (`MatchingEventHandler`)

Runs pure in-memory matching against `OrderBook`.

Important policies:

- **Price-time priority** (TreeMap of price levels + FIFO queues).
- **Post-only**: if the order would cross, reject it (`ORDER_REJECTED`).
- **FOK**: if liquidity is insufficient, cancel remainder (`ORDER_CANCELED`).
- **Self-trade prevention**: same `userId` on both sides → cancel taker remainder.
- **Market price band**: ±5% from best price for market orders.
- Residual quantity: MARKET/IOC cancels remainder; otherwise rests on the book (GTC-style).

Outputs on the engine event:

- `trades` — executed fills
- `orderEvents` — lifecycle records (`ORDER_PLACED`, `TRADE_MATCHED`, `ORDER_CANCELED`, `ORDER_REJECTED`)
- `modifiedOrders` — orders whose state changed

#### Stage 2 — Journal (`JournalEventHandler`)

Appends to the **source of truth** in batches:

- `INSERT` into `trades`
- `INSERT` into `order_events`

This stage stays inside the Disruptor because:

1. Append-only inserts are relatively cheap.
2. Durability of the truth must stay ordered with matching.
3. Recovery depends on this log.

#### Stage 3 — Publisher (`PublisherEventHandler`)

Does **not** write the `orders` table. It only publishes Kafka messages via `EngineKafkaPublisher`:

1. `order.projection` — immutable `OrderProjectionSnapshot` (key = orderId)
2. `trade.completed` — `TradeCompletedMessage` (key = marketId)
3. `order.lifecycle` — `OrderNotificationEvent` (key = userId)

Sends are asynchronous (no `.get()` on the Disruptor thread).

### 3. Projection outside the Disruptor

Updating `orders` is a **read-model** concern (CQRS-style). It was removed from the Disruptor hot path to avoid:

- Extra BusySpin core usage for DB UPDATE work.
- Coupling matching latency to projection I/O.

`OrderProjectionWorker` is now a **Kafka consumer** (`groupId = order-projection`):

- Consumes `order.projection`
- Calls `EventExecutionRepository.updateReadModels` (`INSERT ... ON DUPLICATE KEY UPDATE`)

Because projection can lag, `GET /orders/{id}` may briefly show stale data (or no row yet).

Independent consumer groups on `trade.completed` (`account-service`, `market-data-service`, `stop-trigger-service`) and `order.lifecycle` (`notification-service`) process the same journaled outcomes without sharing the engine process lifecycle beyond Kafka.

### 4. Status polling API

Clients should not wait on WebSocket alone to know when the read model is ready.

```
GET /api/v1/orders/{orderId}/status
```

Response body (success):

```json
{
  "orderId": 123,
  "status": "PARTIALLY_FILLED",
  "executedQty": 100000000,
  "origQty": 500000000
}
```

- **200** — projection row exists; use `status` / quantities.
- **404** — projection not written yet; keep a short loading state and re-poll.

Recommended client behavior:

1. Place order → receive `orderId`.
2. Poll `/status` briefly (bounded retries / timeout).
3. Treat WebSocket notifications as a **fast path**, not the source of truth.

### 5. Source of truth vs read model

| Store | Role |
|-------|------|
| `order_events` | Append-only journal of order lifecycle (**SoT**) |
| `trades` | Append-only journal of fills (**SoT**) |
| `orders` | Projected read model for queries / polling |

On restart, `recoverOrderBookState()`:

1. Loads all `order_events` ordered by id.
2. Rebuilds in-memory orders from `ORDER_PLACED` payloads.
3. Applies `TRADE_MATCHED` fills and removes finished orders on cancel/fill.
4. Puts remaining active orders back into `OrderBook`.

### Event and status vocabulary

`EventType`: `ORDER_PLACED`, `TRADE_MATCHED`, `ORDER_CANCELED`, `ORDER_REJECTED`

`OrderStatus`: `NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, `REJECTED`

## Risks and Mitigation

| Risk | Mitigation |
|------|------------|
| Projection lags behind matching | Lightweight `/status` polling; SoT remains journals |
| BusySpin uses dedicated CPU cores | Keep Disruptor stages to Matching → Journal → Publisher only; projection stays async |
| Process crash before journal flush | Ring events not yet journaled are lost; clients retry with idempotent `clientOrderId` |
| Self-trade / post-only edge cases | Explicit policies in `processMatch` |
| Publisher fails after journal | Journals are already durable; side effects can be replayed/rebuilt later (future: outbox) |

## Design Decisions

| Decision | Reason |
|----------|--------|
| Journal stays inside Disruptor | SoT append must stay ordered and durable with matching |
| Projection leaves Disruptor | Read model UPDATE is slower; eventual consistency is OK with polling |
| HTTP 202 + `orderId` | Client needs a stable key for polling before projection exists |
| Spring events after journal | Replaced by Kafka topics for replayable, independently scalable fan-out |
| Kafka after Publisher only | Matching → Journal stays in-process; bus starts where durability of SoT is already fixed |

## Alternatives Considered

| Alternative | Why not (for now) |
|-------------|-------------------|
| Write `orders` inside Disruptor | Extra BusySpin thread + UPDATE latency on the hot path |
| Kafka between Publisher and consumers | Adopted for post-trade fan-out (projection / account / market / notification) |
| Chronicle Queue instead of Disruptor | Better for cross-process IPC; unnecessary while matching stays one JVM |
| Wait for WebSocket before ending loading | Network-dependent; packet loss can hang UX |
