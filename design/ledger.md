---
title: ledger
target-version: 0.1.0
---

# Ledger

This document describes how SnapTrade records balance changes for trades, deposits, and withdrawals.

## Summary

SnapTrade keeps user money in **accounts** (per user + asset) and writes an immutable **ledger** for every balance-affecting action.

After the matching engine journals a trade, it publishes to Kafka topic `trade.completed`. The ledger service consumes it asynchronously and:

1. Moves locked / available balances for buyer and seller.
2. Deducts trading fees.
3. Appends `account_ledger` rows so every change is auditable.

Wallet deposit and withdrawal flows also write ledger entries (and may notify the user).

### Goals

- Explain the account model (`total` / `available` / `locked`).
- Show how trade settlement maps to ledger entry types.
- Clarify the difference between **pre-trade hold** (engine, in-memory) and **settlement** (DB ledger).

### Non-Goals

- This document does not describe matching or fee formula derivation in depth.
- This document does not cover blockchain / on-chain confirmation internals for withdrawals.
- Double-entry accounting across multiple products is out of scope; the current model is per-account append-only entries.

## Design

### Overview

```
Matching Engine (Publisher)
        │
        ▼
 Kafka topic: trade.completed
        │
        ▼
 AccountService.onTradeCompleted()   (@KafkaListener)
        │
        ├── update Account balances
        └── INSERT AccountLedger rows
```

Wallet paths (separate from matching):

```
Deposit  → BalanceAdjustmentService.processDeposit()
             → ledger DEPOSIT + PrivateNotificationEvent

Withdraw → WithdrawalService.requestWithdrawal()
             → hold + ledger WITHDRAWAL_LOCK
         → WithdrawalService.completeWithdrawal()
             → deduct locked + ledger WITHDRAWAL_FILL + notification
```

### Main components

| Component | Package | Role |
|-----------|---------|------|
| `Account` | `account.domain` | Balance aggregate (`total`, `available`, `locked`) |
| `AccountLedger` | `account.domain` | Append-only history row |
| `LedgerEntryType` | `account.domain` | Classification of each entry |
| `AccountService` | `account.service` | Trade settlement listener |
| `BalanceAdjustmentService` | `wallet.service` | Deposit crediting |
| `WithdrawalService` | `wallet.service` | Withdraw request / complete |

### Account model

Each row in `accounts` is unique by `(user_id, asset_symbol)`.

| Field | Meaning |
|-------|---------|
| `totalBalance` | Overall balance for the asset |
| `availableBalance` | Spendable / withdrawable |
| `lockedBalance` | Reserved (open orders or pending withdrawal) |
| `@Version` | Optimistic locking for concurrent updates |

Common operations:

- `holdBalance` — move available → locked
- `deductLockedBalance` — remove from locked (and total) when fill/withdraw completes
- `addAvailableBalance` / `deductAvailableBalance` — credit or fee deduction

Invariant (intended):

```
totalBalance ≈ availableBalance + lockedBalance
```

### Ledger entry types

Enum `LedgerEntryType`:

| Type | Used today | Meaning |
|------|------------|---------|
| `TRADE_FILL` | Yes | Asset in/out from a trade |
| `TRADE_FEE` | Yes | Fee charged after a fill |
| `DEPOSIT` | Yes | Deposit credited |
| `WITHDRAWAL_LOCK` | Yes | Funds locked for a withdraw request |
| `WITHDRAWAL_FILL` | Yes | Locked funds finally removed on complete |
| `WITHDRAWAL` | Enum only | Reserved / unused in current code |
| `PROMOTION_BONUS` | Enum only | Reserved / unused in current code |

Each ledger row stores:

- `accountId`, `userId`, `assetSymbol`
- `entryType`, signed `amount`
- `balanceBefore`, `balanceAfter`
- `referenceId` (trade id, deposit id, or withdrawal id)

### Trade settlement

`AccountService.onTradeCompleted` runs after consuming `trade.completed`.

Market metadata (`MarketSpec`) supplies `baseAsset` and `quoteAsset`.

#### Buyer

1. Quote account: `TRADE_FILL` **−quoteQuantity**, `deductLockedBalance(quoteQuantity)`
2. Base account: `TRADE_FILL` **+quantity**, `addAvailableBalance(quantity)`
3. If fee > 0: base account `TRADE_FEE` **−fee**, `deductAvailableBalance(fee)`

#### Seller

1. Base account: `TRADE_FILL` **−quantity**, `deductLockedBalance(quantity)`
2. Quote account: `TRADE_FILL` **+quoteQuantity**, `addAvailableBalance(quoteQuantity)`
3. If fee > 0: quote account `TRADE_FEE` **−fee**, `deductAvailableBalance(fee)`

Fee amount:

- Maker user → `trade.makerFee`
- Otherwise → `trade.takerFee`

Settlement is **asynchronous** relative to matching. Journals (`trades` / `order_events`) are already durable before this listener runs.

### Deposit

`BalanceAdjustmentService.processDeposit`:

1. **Idempotency**: skip if a `DEPOSIT` ledger row already exists for this deposit id.
2. Credit `availableBalance`.
3. Write `DEPOSIT` ledger entry.
4. Publish `PrivateNotificationEvent(DEPOSIT_COMPLETED)` for WebSocket delivery.

### Withdrawal

Two steps:

1. **`requestWithdrawal`**
   - Hold funds on the account.
   - Create `withdrawals` row (`REQUESTED`).
   - Write `WITHDRAWAL_LOCK` ledger entry.

2. **`completeWithdrawal`**
   - Mark withdrawal `COMPLETED` with `txHash`.
   - `deductLockedBalance`.
   - Write `WITHDRAWAL_FILL`.
   - Publish `PrivateNotificationEvent(WITHDRAWAL_COMPLETED)`.

Amounts for withdrawal are converted with scale `10^8` (`movePointRight(8)`).

### Pre-trade hold vs ledger

Important distinction for newcomers:

| Layer | Where | Purpose |
|-------|-------|---------|
| Pre-trade hold | `InMemoryBalanceRepository` (engine) | Fast gate before matching |
| Settlement | DB `accounts` + `account_ledger` | Durable money movement after trade |

They are related conceptually (both prevent overspend) but are **not the same store** today. Settlement assumes the durable account state is updated correctly when trades complete.

## Risks and Mitigation

| Risk | Mitigation |
|------|------------|
| Async settlement lags behind matching | Journals are SoT for fills; ledger catches up via event |
| Duplicate deposit processing | Idempotent check on `(referenceId, DEPOSIT)` |
| Partial failure mid-settlement | `@Transactional` on `onTradeCompleted`; retry/replay can be added later |
| Confusion between in-memory hold and DB lock | Documented separation; long-term unification is a future improvement |
| Missing `accountingTaskExecutor` bean | Ensure async executor config exists in runtime; otherwise listener may not run as intended |

## Design Decisions

| Decision | Reason |
|----------|--------|
| Append-only `account_ledger` | Audit trail and easier debugging of balance history |
| Settle after `trade.completed` | Keep matching hot path free of DB account writes |
| Separate lock and fill for withdrawals | Request can fail independently of final chain confirmation |
| Fee as separate `TRADE_FEE` rows | Clear audit of fee vs notional transfer |

## Alternatives Considered

| Alternative | Why not (for now) |
|-------------|-------------------|
| Update balances inside Disruptor | Would couple matching to account DB latency |
| Full double-entry journal with contra accounts | Heavier model; current per-account signed amounts are enough |
| Saga across wallet + trading services | Needed when services split; monolith uses local transactions + events |
