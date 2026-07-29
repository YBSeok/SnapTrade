# Design Document

## Contents

### Trading Core

- [Matching Engine](matching_engine.md): Order intake, Disruptor pipeline (Matching → Journal → Publisher), Kafka fan-out, status polling
- [Ledger](ledger.md): Account balances and append-only ledger for trades, deposits, and withdrawals

### Realtime Delivery

- [Data Streaming](data_streaming.md): Public ticker / kline STOMP streams driven by `trade.completed`
- [Notification](notification.md): Private per-user alerts on `/user/queue/notifications`

## How these pieces connect

```
POST /orders
  → Disruptor: Matching → Journal (SoT) → Publisher (Kafka produce only)
  → Kafka topics:
       ├─ order.projection → OrderProjectionWorker → orders ←── GET /orders/{id}/status
       ├─ trade.completed
       │     ├─ AccountService (ledger)
       │     ├─ MarketDataService → /topic/ticker|kline
       │     └─ StopLimitTriggerEngine
       └─ order.lifecycle → OrderNotificationListener → /user/queue/notifications

Wallet deposit/withdraw complete
  → PrivateNotificationEvent (Spring AFTER_COMMIT) → /user/queue/notifications
```
