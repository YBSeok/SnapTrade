---
title: notification
target-version: 0.1.0
---

# Notification

This document describes how SnapTrade sends **private, per-user** real-time alerts over WebSocket.

## Summary

Users need timely updates when:

- An order is placed, partially filled, fully filled, canceled, or rejected.
- A deposit or withdrawal completes.

SnapTrade delivers these as STOMP messages to:

```
/user/{userId}/queue/notifications
```

(Clients typically subscribe as `/user/queue/notifications` after CONNECT with JWT.)

Notifications are a **best-effort fast path**. Durable truth lives in journals / ledger / projection tables. Clients should still use REST (for example order status polling) when WebSocket delivery is delayed or lost.

### Goals

- Document the two publish paths (order lifecycle vs wallet).
- Define `NotificationType` and the message envelope.
- Clarify that notifications are not the source of truth.

### Non-Goals

- Public ticker / kline streams (see [Data Streaming](data_streaming.md)).
- Persistent notification inbox / unread history (not implemented yet).
- Push notifications (mobile FCM/APNs).

## Design

### Overview

There are **two** paths into the same private queue.

```
┌──────────────────────────────┐
│ Order path                   │
│ Publisher → Kafka            │
│   topic: order.lifecycle     │
│   → OrderNotificationListener│
│   → UserNotificationService  │
└──────────────┬───────────────┘
               │
               ▼
        /user/queue/notifications
               ▲
┌──────────────┴───────────────┐
│ Wallet path                  │
│ Deposit / Withdrawal complete│
│   → PrivateNotificationEvent │
│   → NotificationEventDispatcher│
│     (AFTER_COMMIT)           │
│   → UserNotificationService  │
└──────────────────────────────┘
```

### Main components

| Component | Package | Role |
|-----------|---------|------|
| `OrderNotificationEvent` | `common.event` | Enriched order lifecycle event from engine |
| `OrderNotificationListener` | `notification` | Maps engine events → user notification payload |
| `PrivateNotificationEvent` | `common.event` | Generic private notify (wallet today) |
| `NotificationEventDispatcher` | `common.websocket` | AFTER_COMMIT listener for private events |
| `UserNotificationService` | `common.websocket` | `convertAndSendToUser(..., "/queue/notifications")` |
| `NotificationType` | `common.event` | Client-facing type enum |
| `StompHeaderChannelInterceptor` | `common.websocket` | JWT principal for `/user/**` |

### Authentication

On STOMP `CONNECT`, the client sends JWT in the `Authorization` header.  
`StompHeaderChannelInterceptor` validates it and sets `Principal.getName()` to the string form of `userId`.

Subscribing to destinations under `/user/` requires an authenticated principal. This keeps private queues from being readable by other users.

### Message envelope

All private notifications share a common JSON shape:

```json
{
  "type": "NOTIFICATION",
  "notificationType": "ORDER_FILLED",
  "message": "주문이 전량 체결되었습니다.",
  "payload": {
    "orderId": 123,
    "marketId": 1,
    "status": "FILLED",
    "fillQty": 100,
    "fillPrice": 50000000,
    "executedQty": 100,
    "origQty": 100
  }
}
```

| Field | Meaning |
|-------|---------|
| `type` | Always `NOTIFICATION` for this channel |
| `notificationType` | Machine-readable enum name |
| `message` | Human-readable summary |
| `payload` | Structured details for UI updates |

### Notification types

| `NotificationType` | Source |
|--------------------|--------|
| `ORDER_PLACED` | Order placed on book / accepted into engine flow |
| `ORDER_PARTIALLY_FILLED` | `TRADE_MATCHED` while status is not fully filled |
| `ORDER_FILLED` | `TRADE_MATCHED` and status becomes `FILLED` |
| `ORDER_CANCELED` | Cancel path |
| `ORDER_REJECTED` | Reject path (e.g. post-only would cross) |
| `DEPOSIT_COMPLETED` | Wallet deposit credited |
| `WITHDRAWAL_COMPLETED` | Withdrawal finalized |

### Order path (detail)

After journal, `PublisherEventHandler` publishes `OrderNotificationEvent` to Kafka topic `order.lifecycle` with:

- `userId`, `orderId`, `marketId`
- `eventType`, `statusBefore`, `statusAfter`
- `fillQty`, `fillPrice`, `executedQty`, `origQty`

`OrderNotificationListener` maps:

| Engine `EventType` | Result |
|--------------------|--------|
| `ORDER_PLACED` | `ORDER_PLACED` |
| `ORDER_CANCELED` | `ORDER_CANCELED` |
| `ORDER_REJECTED` | `ORDER_REJECTED` |
| `TRADE_MATCHED` + `FILLED` | `ORDER_FILLED` |
| `TRADE_MATCHED` + otherwise | `ORDER_PARTIALLY_FILLED` |

Fill fields are included for filled / partially filled types so the UI can update progress without waiting for a full order DTO.

### Wallet path (detail)

- Deposit: after ledger write in `BalanceAdjustmentService.processDeposit`.
- Withdrawal: after ledger write in `WithdrawalService.completeWithdrawal` (not on request/lock alone).

These publish `PrivateNotificationEvent`, then `NotificationEventDispatcher` runs with:

```
@TransactionalEventListener(phase = AFTER_COMMIT)
```

So a rolled-back wallet transaction does not emit a user alert.

### Delivery semantics

| Layer | Role if packet is lost |
|-------|------------------------|
| Journals / ledger / `orders` projection | Still correct |
| WebSocket notification | User may miss toast; UI can catch up via REST |
| `GET /orders/{id}/status` | Recommended for order loading / confirmation |

Rule of thumb:

> Realtime = WebSocket for speed.  
> Certainty = REST / DB.  
> WebSocket alone must never be required for correctness.

## Risks and Mitigation

| Risk | Mitigation |
|------|------------|
| Notification packet loss | REST status polling / page refresh; optional future notification table |
| Sending alert before DB commit (wallet) | `AFTER_COMMIT` transactional event listener |
| Unauthorized subscribe to another user queue | JWT principal required for `/user/**` |
| Treating WS as SoT for loading UX | Poll `/orders/{id}/status`; WS only accelerates |

## Design Decisions

| Decision | Reason |
|----------|--------|
| One private destination `/queue/notifications` | Simple client subscribe; type field discriminates |
| Separate order event vs private event | Order path needs engine enrichment; wallet path needs TX commit |
| Partial fill notifications | Matches exchange UX (e.g. Upbit-style progress feedback) |
| Best-effort delivery | Avoid coupling money correctness to network reliability |

## Alternatives Considered

| Alternative | Why not (for now) |
|-------------|-------------------|
| Separate WebSocket endpoint for notifications | Unnecessary; STOMP already multiplexes destinations |
| Persist every notification before send | Good next step for unread history; not required for v0 |
| Drive UI loading solely from WS arrival | Fragile under packet loss / offline |
