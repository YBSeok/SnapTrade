# Ledger & Account Design

체결 이후 자산 이동을 이중 기록(잔고 + 원장)으로 보장하는 계정/원장 설계입니다.  
사전 주문 홀드는 인메모리, 최종 정산은 `Account` + `AccountLedger`로 분리됩니다.

## 1. 전체 아키텍처

```mermaid
flowchart TB
    subgraph TradePath["체결 정산 경로"]
        PH[PublisherEventHandler]
        TCE[TradeCompletedEvent]
        AS[AccountService.onTradeCompleted]
        Acc[(accounts)]
        Led[(account_ledger)]
        PH --> TCE --> AS
        AS --> Acc
        AS --> Led
    end

    subgraph WalletPath["입출금 경로"]
        WC[WalletController]
        BAS[BalanceAdjustmentService]
        WS[WithdrawalService]
        Dep[(deposits)]
        Wd[(withdrawals)]
        WC --> BAS --> Dep
        WC --> WS --> Wd
        BAS --> Acc
        BAS --> Led
        WS --> Acc
        WS --> Led
    end

    subgraph PreTrade["주문 사전 홀드"]
        OPP[OrderPreProcessor]
        MemBal[InMemoryBalanceRepository]
        OPP --> MemBal
    end

    MemBal -.->|최종 정산과 별도| Acc
```

## 2. 계정 모델

```mermaid
erDiagram
    ACCOUNT ||--o{ ACCOUNT_LEDGER : has
    ACCOUNT {
        long id
        long userId
        string assetSymbol
        long totalBalance
        long availableBalance
        long lockedBalance
        long version
    }
    ACCOUNT_LEDGER {
        long id
        long accountId
        long userId
        string entryType
        string assetSymbol
        long amount
        long balanceBefore
        long balanceAfter
        long referenceId
    }
```

### 잔고 연산

| Method | 효과 |
| :--- | :--- |
| `holdBalance(amount)` | available → locked |
| `releaseBalance(amount)` | locked → available |
| `deductLockedBalance(amount)` | total↓, locked↓ |
| `addAvailableBalance(amount)` | total↑, available↑ |
| `deductAvailableBalance(amount)` | total↓, available↓ |

* `(user_id, asset_symbol)` unique
* `@Version` 낙관적 락으로 동시 정산 충돌 감지

## 3. 원장 엔트리 타입

```mermaid
flowchart LR
    subgraph Deposit
        D[DEPOSIT]
    end
    subgraph Withdraw
        WL[WITHDRAWAL_LOCK]
        WF[WITHDRAWAL_FILL]
    end
    subgraph Trade
        TF[TRADE_FILL]
        FEE[TRADE_FEE]
    end
    subgraph Extra
        PB[PROMOTION_BONUS]
        W[WITHDRAWAL]
    end
```

| LedgerEntryType | 용도 |
| :--- | :--- |
| `DEPOSIT` | 입금 반영 |
| `WITHDRAWAL_LOCK` | 출금 요청 시 잠금 |
| `WITHDRAWAL_FILL` | 출금 완료 차감 |
| `TRADE_FILL` | 체결 자산 이동 |
| `TRADE_FEE` | 수수료 차감 |
| `PROMOTION_BONUS` | 보너스 |
| `WITHDRAWAL` | (레거시/일반) 출금 |

## 4. 체결 정산 시퀀스

```mermaid
sequenceDiagram
    participant PH as PublisherEventHandler
    participant Bus as ApplicationEventPublisher
    participant AS as AccountService
    participant Cache as MarketMetadataCache
    participant Acc as Account
    participant Led as AccountLedgerRepository

    PH->>Bus: TradeCompletedEvent(trade)
    Bus->>AS: onTradeCompleted (@Async accountingTaskExecutor)
    AS->>Cache: getSpec(marketId)
    AS->>AS: determineFee (makerFee vs takerFee)

    rect rgb(240,248,255)
        Note over AS,Led: processBuyer
        AS->>Led: TRADE_FILL(-quoteQuantity)
        AS->>Acc: quote.deductLockedBalance(quote)
        AS->>Led: TRADE_FILL(+quantity)
        AS->>Acc: base.addAvailableBalance(qty)
        AS->>Led: TRADE_FEE(-buyerFee) on base
        AS->>Acc: base.deductAvailableBalance(fee)
    end

    rect rgb(255,248,240)
        Note over AS,Led: processSeller
        AS->>Led: TRADE_FILL(-quantity)
        AS->>Acc: base.deductLockedBalance(qty)
        AS->>Led: TRADE_FILL(+quoteQuantity)
        AS->>Acc: quote.addAvailableBalance(quote)
        AS->>Led: TRADE_FEE(-sellerFee) on quote
        AS->>Acc: quote.deductAvailableBalance(fee)
    end
```

### 수수료 자산 규칙

* **Buyer fee** → base asset에서 차감
* **Seller fee** → quote asset에서 차감
* 원장 기록은 잔고 변경 **직전**에 수행 (`balanceAfter = balanceBefore + amount`)

## 5. 입금 플로우

```mermaid
sequenceDiagram
    participant C as Client
    participant WC as WalletController
    participant Dep as DepositRepository
    participant BAS as BalanceAdjustmentService
    participant Acc as Account
    participant Led as AccountLedger
    participant Bus as EventPublisher

    C->>WC: POST /api/v1/wallet/deposit
    WC->>Dep: save(Deposit CONFIRMED)
    WC->>BAS: processDeposit(deposit)
    BAS->>Led: existsByReferenceIdAndEntryType?
    alt 이미 처리됨
        BAS-->>WC: return
    else 신규
        BAS->>Acc: addAvailableBalance(amountAsLong)
        BAS->>Led: DEPOSIT(+amount, referenceId=depositId)
        BAS->>Bus: PrivateNotificationEvent(DEPOSIT_COMPLETED)
    end
```

## 6. 출금 플로우

```mermaid
sequenceDiagram
    participant C as Client
    participant WC as WalletController
    participant WS as WithdrawalService
    participant Acc as Account
    participant Led as AccountLedger
    participant Wd as WithdrawalRepository

    C->>WC: POST /api/v1/wallet/withdraw
    WC->>WS: requestWithdrawal
    WS->>WS: amount.movePointRight(8).longValue
    WS->>Acc: holdBalance(amount)
    WS->>Wd: save(REQUESTED)
    WS->>Led: WITHDRAWAL_LOCK(-amount)

    Note over WS: 운영/시스템 완료 처리
    WS->>Wd: complete(txHash) → COMPLETED
    WS->>Acc: deductLockedBalance(amount)
    WS->>Led: WITHDRAWAL_FILL(-amount)
    WS->>WS: PrivateNotificationEvent(WITHDRAWAL_COMPLETED)
```

## 7. Pre-trade Hold vs Ledger 경계

```mermaid
flowchart TB
    subgraph FastPath["주문 핫 패스"]
        OPP[OrderPreProcessor]
        Mem["InMemoryBalanceRepository<br/>availableKrw / holdKrw<br/>availableBtc / holdBtc"]
        Eng[Matching Engine]
        OPP -->|tryPreTradeHold| Mem
        OPP --> Eng
    end

    subgraph DurablePath["영속 정산"]
        TCE[TradeCompletedEvent]
        Acc[Account locked/available]
        Led[AccountLedger]
        TCE --> Acc
        TCE --> Led
    end

    Eng -.->|체결 후 비동기| TCE
```

| 구분 | 저장소 | 역할 |
| :--- | :--- | :--- |
| Pre-trade | `InMemoryBalanceRepository` | 주문 수락 전 빠른 홀드/검증 |
| Settlement | `Account` + `AccountLedger` | 체결·입출금의 최종 무결성 |

> 스케일 주의: 출금은 `movePointRight(8)`, 입금은 `BigDecimal.longValue()` 경로가 혼재하므로 운영 시 단위 통일이 필요함.

## 8. 불변성 규칙

1. `account_ledger`는 append-only (수정/삭제 없음)
2. 동일 `referenceId + entryType` 중복 적재 방지 (입금 idempotency)
3. 모든 잔고 변경은 대응 원장 row와 1:1로 묶임
4. 체결 정산은 `@Transactional` + 낙관적 락으로 원자성 보장
