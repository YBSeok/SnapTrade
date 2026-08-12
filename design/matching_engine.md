# Matching Engine Design

이벤트 소싱 + LMAX Disruptor 기반 인메모리 매칭 엔진 설계입니다.  
핫 패스는 `EventSourcedMatchingEngine`이며, 레거시 `AsyncMatchingEngine`은 비교용으로만 존재합니다.

## 1. 전체 아키텍처

```mermaid
flowchart TB
    subgraph Ingress
        OC[OrderController]
        OPP[OrderPreProcessor]
        Idem[InMemoryIdempotencyRepository]
        Hold[InMemoryBalanceRepository]
        Cache[MarketMetadataCache]
    end

    subgraph Disruptor["LMAX Disruptor RingBuffer 65536"]
        M[MatchingEventHandler]
        J[JournalEventHandler]
        P[PublisherEventHandler]
        M --> J --> P
    end

    subgraph Memory
        OB[(OrderBook per marketId)]
        Seq[sequenceGenerators]
    end

    subgraph Persist
        EvtRepo[EventExecutionRepository]
        DB[(order_events / trades)]
        Proj[OrderProjectionWorker]
        Orders[(orders read model)]
    end

    subgraph Downstream
        TradeEvt[TradeCompletedEvent]
        NotiEvt[OrderNotificationEvent]
    end

    OC -->|OrderTrace| OPP
    OPP --> Idem
    OPP --> Cache
    OPP --> Hold
    OPP -->|placeOrder / cancelOrder| Disruptor
    M --> OB
    M --> Seq
    J --> EvtRepo --> DB
    P --> Proj --> Orders
    P --> TradeEvt
    P --> NotiEvt
```

## 2. 주문 접수 시퀀스

```mermaid
sequenceDiagram
    participant C as Client
    participant OC as OrderController
    participant OPP as OrderPreProcessor
    participant Eng as EventSourcedMatchingEngine
    participant RB as RingBuffer
    participant MH as MatchingEventHandler
    participant JH as JournalEventHandler
    participant PH as PublisherEventHandler
    participant PW as OrderProjectionWorker

    C->>OC: POST /api/v1/orders
    OC->>OC: new OrderTrace(request)
    OC->>OPP: validateAndEnqueue(trace)
    OPP->>OPP: setNx(clientOrderId)
    OPP->>OPP: tick / minQty / minNotional 검증
    OPP->>OPP: tryPreTradeHold(quote or base)
    OPP->>Eng: placeOrder(trace)
    Eng->>RB: publishEvent(PLACE)
    RB->>MH: onEvent
    MH->>MH: processMatch → OrderBook fill
    MH->>JH: sequenced handoff
    JH->>JH: appendEvents (endOfBatch smart batch)
    JH->>PH: sequenced handoff
    PH->>PW: enqueue(OrderProjectionSnapshot)
    PH->>PH: TradeCompletedEvent / OrderNotificationEvent
    PW->>PW: updateReadModels UPSERT
    OC-->>C: OrderAcceptedResponse(orderId)
```

## 3. Disruptor 파이프라인 상세

```mermaid
flowchart LR
    subgraph Slot["EngineEvent Slot (pre-allocated)"]
        cmd[command PLACE/CANCEL]
        tr[trace]
        trades[trades]
        events[orderEvents]
        mods[modifiedOrders]
    end

    Pub[MULTI Producer<br/>CAS sequence] --> Slot
    Slot --> H1[MatchingEventHandler<br/>BusySpin]
    H1 --> H2[JournalEventHandler<br/>INSERT only]
    H2 --> H3[PublisherEventHandler<br/>fan-out]
    H3 --> Clear[event.clear]
```

| 설정 | 값 |
| :--- | :--- |
| bufferSize | 65,536 |
| ProducerType | `MULTI` |
| WaitStrategy | `BusySpinWaitStrategy` |
| ThreadFactory | `DaemonThreadFactory` |
| Chain | Matching → Journal → Publisher |

## 4. OrderBook 데이터 구조

```mermaid
flowchart TB
    subgraph Book["OrderBook"]
        Asks["asks: TreeMap&lt;Long, Queue&lt;Order&gt;&gt;<br/>오름차순 · best ask = first"]
        Bids["bids: TreeMap&lt;Long, Queue&lt;Order&gt;&gt;<br/>내림차순 · best bid = first"]
    end

    Asks --> QA["ArrayDeque FIFO<br/>동일 가격 시간 우선"]
    Bids --> QB["ArrayDeque FIFO<br/>동일 가격 시간 우선"]
```

* 가격·수량은 `long` 고정소수점 (스케일 `10^8`)
* 마켓별 `ConcurrentHashMap<Long, OrderBook>`에 보관
* Disruptor 단일 컨슈머이므로 OrderBook mutation은 실질 single-thread

## 5. 매칭 알고리즘 (`processMatch`)

```mermaid
flowchart TD
    Start[PLACE 수신] --> PostOnly{postOnly & cross?}
    PostOnly -->|yes| Reject[reject + ORDER_REJECTED]
    PostOnly -->|no| FOK{FOK?}
    FOK -->|liquidity 부족| CancelFOK[cancel residual]
    FOK -->|ok / not FOK| Band[Market: best ±5% band]
    Band --> PlaceEvt[executedQty==0 이면 ORDER_PLACED]
    PlaceEvt --> Loop{상대 best queue 존재?}
    Loop -->|no| Residual
    Loop -->|yes| Self{self-trade?}
    Self -->|yes| CancelSelf[cancel taker residual]
    Self -->|no| PriceOK{가격/밴드 조건}
    PriceOK -->|fail| Residual
    PriceOK -->|ok| Fill[fillQty = min<br/>Trade + TRADE_MATCHED x2]
    Fill --> Loop
    Residual{잔량 처리} -->|MARKET/IOC| CancelRes[ORDER_CANCELED]
    Residual -->|GTC LIMIT| AddBook[orderBook.addOrder]
```

## 6. 이벤트 소싱 & 복구

```mermaid
sequenceDiagram
    participant Boot as ApplicationReadyEvent
    participant Eng as EventSourcedMatchingEngine
    participant Repo as EventExecutionRepository
    participant Map as stateMap
    participant OB as OrderBook

    Boot->>Eng: onApplicationReady
    Eng->>Eng: preWarmSequences
    Eng->>Repo: findAllEventsOrderByIdAsc
    loop 각 OrderEvent
        Repo-->>Eng: event
        alt ORDER_PLACED
            Eng->>Map: reconstructForReplay
        else TRADE_MATCHED
            Eng->>Map: order.fill(fillQty, fillPrice)
        else ORDER_CANCELED
            Eng->>Map: remove(orderId)
        end
    end
    Eng->>OB: 활성 주문 addOrder
    Eng->>Eng: initDisruptor
```

### EventType

| 타입 | 의미 |
| :--- | :--- |
| `ORDER_PLACED` | 신규 주문 접수 (payload: marketId, userId, side, price, origQty) |
| `TRADE_MATCHED` | 체결 (fillQty, fillPrice) |
| `ORDER_CANCELED` | 취소 |
| `ORDER_REJECTED` | 거절 (post-only 등) |

## 7. 도메인 필드 (핫 패스)

| Entity | 핵심 필드 |
| :--- | :--- |
| `Order` | `price`, `origQty`, `executedQty`, `cumulativeQuoteQty` (`long`), `side`, `orderType`, `timeInForce`, `status`, `sequenceNo` |
| `Trade` | `price`, `quantity`, `quoteQuantity`, `makerFee`, `takerFee`, maker/taker/buyer/seller userId |
| `OrderEvent` | `orderId`, `tradeId`, `eventType`, `statusBefore/After`, `fillQty`, `fillPrice`, `payload` |
| Fee | `(price * qty) / 100_000_000L` → `(notional * feeRate) / 1_000_000L` |

## 8. CQRS 경계

```mermaid
flowchart LR
    subgraph Command
        Match[Matching]
        Journal[Journal INSERT]
    end

    subgraph Query
        Proj[OrderProjectionWorker]
        Qry[OrderQueryService]
        API[GET order status]
    end

    Match --> Journal
    Journal -.->|async snapshot| Proj
    Proj --> Qry --> API
```

* Journal은 `trades` / `order_events` INSERT만 수행 (`orders` 미기록)
* Read model은 `OrderProjectionWorker`가 last-write-wins UPSERT
* 조회 시 프로젝션 지연이면 일시적 404 가능
