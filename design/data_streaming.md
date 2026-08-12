# Market Data Streaming Design

체결 이벤트를 구독해 Ticker / Kline을 인메모리로 갱신하고, STOMP 토픽으로 실시간 방송하는 설계입니다.  
OrderBook depth는 매칭 엔진 내부 전용이며 WebSocket으로 송출하지 않습니다.

## 1. 전체 아키텍처

```mermaid
flowchart TB
    subgraph Source
        PH[PublisherEventHandler]
        Mock[MockTradeGenerator]
        TCE[TradeCompletedEvent]
        PH --> TCE
        Mock --> TCE
    end

    subgraph MarketData
        MDS[MarketDataService]
        TB[(tickerBuffer)]
        KB[(klineBuffer)]
        Pend[(pendingKlinesToDb)]
        MDS --> TB
        MDS --> KB
        MDS --> Pend
    end

    subgraph Broadcast
        MBS[MarketBroadcastService]
        STOMP[SimpMessagingTemplate]
        MBS --> STOMP
    end

    subgraph Persist
        Flush["@Scheduled 1s flush"]
        TJ[TickerJdbcRepository]
        KJ[KlineJdbcRepository]
        DB[(ticker / kline)]
        Flush --> TJ --> DB
        Flush --> KJ --> DB
    end

    TCE -->|@Async marketDataTaskExecutor| MDS
    MDS -->|@Async webSocketTaskExecutor| MBS
    Pend --> Flush
    TB --> Flush
```

## 2. STOMP / WebSocket 구성

```mermaid
flowchart LR
    Client -->|CONNECT /ws| WS[WebSocketConfig]
    WS --> Broker["Simple Broker<br/>/topic , /queue"]
    WS --> App["App prefix /app"]
    WS --> User["User prefix /user"]
    WS --> Out["Outbound pool<br/>core/max 8192 VT<br/>QueueDelayTaskDecorator"]

    Client -->|SUBSCRIBE| T1["/topic/ticker/{marketId}"]
    Client -->|SUBSCRIBE| T2["/topic/kline/{marketId}/{interval}"]
```

| 항목 | 값 |
| :--- | :--- |
| Endpoint | `/ws` |
| Allowed origins | `*` |
| Security | `/ws/**` permitAll |
| Outbound threads | virtual threads `ws-outbound-vt-*` |
| Queue delay metric | `websocket_outbound_queue_delay` (warn > 100ms) |

## 3. 체결 → 시세 방송 시퀀스

```mermaid
sequenceDiagram
    participant PH as PublisherEventHandler
    participant Bus as ApplicationEventPublisher
    participant MDS as MarketDataService
    participant Buf as ticker/kline Buffer
    participant MBS as MarketBroadcastService
    participant Cli as STOMP Clients

    PH->>Bus: TradeCompletedEvent(trade)
    Bus->>MDS: onTradeCompleted (@Async)
    MDS->>Buf: ticker.update(price, qty, quoteQty, open24h)
    MDS->>MBS: broadcastTicker(TickerBroadcastDTO)
    MBS->>Cli: /topic/ticker/{marketId}

    loop HOT_INTERVALS 1s, 1m
        MDS->>Buf: createNewKline or kline.update
        MDS->>MBS: broadcastKline(KlineBroadcastDTO)
        MBS->>Cli: /topic/kline/{marketId}/{interval}
    end
```

## 4. Ticker 갱신 로직

```mermaid
flowchart TD
    Trade[TradeCompletedEvent] --> Load{tickerBuffer hit?}
    Load -->|miss| DBLoad[loadTickerFromDbOrInit]
    Load -->|hit| Upd
    DBLoad --> Upd[ticker.update]
    Upd --> Vol[volume24h / quoteVolume24h / tradeCount24h++]
    Vol --> HL[high24h / low24h 갱신]
    HL --> Chg["priceChange / priceChangePct<br/>pct = change * 10000 / openPrice24h"]
    Chg --> DTO[TickerBroadcastDTO.from]
    DTO --> Pub["convertAndSend /topic/ticker/{id}"]
```

### TickerBroadcastDTO 필드

`marketId`, `lastPrice`, `priceChange`, `priceChangePct`, `high24h`, `low24h`, `volume24h`, `quoteVolume24h`, `tradeCount24h`, `timestamp`

## 5. Kline 버킷 처리

```mermaid
flowchart TD
    Trade[Trade] --> Key["KlineKey(marketId, interval)"]
    Key --> Exists{buffer에 candle?}
    Exists -->|no| New1[createNewKline]
    Exists -->|yes| Bucket{동일 openTime 버킷?}
    Bucket -->|rollover| FlushOld[pendingKlinesToDb.offer old]
    FlushOld --> New2[createNewKline]
    Bucket -->|same| Patch[existing.update OHLC + volume]
    New1 --> Cast[broadcastKline]
    New2 --> Cast
    Patch --> Cast
```

| Interval | 코드 | DB flush |
| :--- | :--- | :--- |
| 1초 | `ChartInterval.SEC_1` (`1s`) | 스킵 (메모리 전용) |
| 1분 | `ChartInterval.MIN_1` (`1m`) | batchUpsert |

### KlineBroadcastDTO 필드

`marketId`, `interval`, `openTimeMs`, `openPrice`, `highPrice`, `lowPrice`, `closePrice`, `volume`, `quoteVolume`, `timestamp`

## 6. 영속화 스케줄

```mermaid
sequenceDiagram
    participant Sch as Scheduler
    participant MDS as MarketDataService
    participant TJ as TickerJdbcRepository
    participant KJ as KlineJdbcRepository
    participant DB as DB

    loop every 1000ms
        Sch->>MDS: flushMarketDataToDb
        MDS->>TJ: batchUpsert(tickers)
        MDS->>KJ: batchUpsert(pending klines, skip 1s)
        TJ->>DB: UPSERT
        KJ->>DB: UPSERT
    end

    loop every 60000ms
        Sch->>MDS: refreshOpenPrice24hCache
        MDS->>DB: findClosestOldClosePrice
        MDS->>MDS: openPrice24hCache 갱신
    end
```

## 7. 데이터 경계 (OrderBook 미방송)

```mermaid
flowchart LR
    subgraph EngineOnly
        OB[OrderBook TreeMap]
        Match[MatchingEventHandler]
        Match --> OB
    end

    subgraph PublicStream
        Ticker[Ticker topic]
        Kline[Kline topic]
    end

    Match -->|Trade only| PublicStream
    OB -.->|No WS channel| X[미전송]
```

* 공개 스트림: **Ticker / Kline**
* OrderBook depth REST/WS API 없음
* 부하 테스트 구독 예: `/topic/ticker/{1..50}`, `/topic/kline/1/1m`

## 8. 비동기 실행 경계

| Executor | 담당 |
| :--- | :--- |
| `marketDataTaskExecutor` | `MarketDataService.onTradeCompleted` |
| `webSocketTaskExecutor` | `MarketBroadcastService` 송신 |
| STOMP outbound pool | 실제 소켓 write + queue delay 계측 |

매칭 핫 패스와 시세 가공/송신을 분리해, 방송 지연이 매칭 P99에 영향을 주지 않도록 설계합니다.
