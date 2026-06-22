package com.project.snaptrade.market.service;

import com.github.f4b6a3.tsid.TsidCreator;
import com.project.snaptrade.engine.domain.Trade;
import com.project.snaptrade.market.domain.Kline;
import com.project.snaptrade.market.domain.Ticker;
import com.project.snaptrade.market.dto.TradeCompletedEvent;
import com.project.snaptrade.market.repository.KlineRepository;
import com.project.snaptrade.market.repository.TickerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class MarketDataService {

    private final TickerRepository tickerRepository;
    private final KlineRepository klineRepository;
    private final WebSocketBroadcastService webSocketBroadcastService; // 웹소켓 전송용 컴포넌트

    private final Map<Long, Ticker> tickerBuffer = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Kline> klineBuffer = new ConcurrentHashMap<>();

    /**
     * 1. 매칭 엔진으로부터 체결 이벤트를 수신하여 인메모리에서 즉시 집계합니다.
     */
    @Async("marketDataTaskExecutor")
    @EventListener
    public void onTradeCompleted(TradeCompletedEvent event) {
        Trade trade = event.getTrade();

        // 래퍼 클래스 대신 원시 타입 사용
        long marketId = trade.getMarketId();
        long price = trade.getPrice();
        long quantity = trade.getQuantity();

        // [중요] 오버플로우 방지를 위해 엔진/정산 레이어에서 이미 안전하게 계산된 값을 그대로 가져옵니다.
        long quoteQuantity = trade.getQuoteQuantity();

        // 1. 객체 할당 제로(Zero-Allocation) 시간 계산
        long currentMs = System.currentTimeMillis();
        long openTimeMs = currentMs - (currentMs % 60000);

        // 1-1. Ticker 인메모리 갱신
        Ticker ticker = tickerBuffer.computeIfAbsent(marketId, this::loadTickerFromDbOrInit);
        updateTickerState(ticker, price, quantity, quoteQuantity);

        // 1-2. Kline 인메모리 갱신
        Kline kline = klineBuffer.compute(marketId, (id, existing) -> {
            if (existing == null || existing.getOpenTimeMs() != openTimeMs) {
                if (existing != null) {
                    // [선택] 완성된 이전 분봉을 DB 저장 큐(Async)로 이관
                }
                return createNewKline(marketId, openTimeMs, price, quantity, quoteQuantity);
            }

            // 동일한 1분봉 내에서의 갱신
            updateKlineState(existing, price, quantity, quoteQuantity);
            return existing;
        });

        // 1-3. 웹소켓 게이트웨이로 실시간 데이터 전송 (I/O)
        webSocketBroadcastService.broadcastTicker(ticker);
        webSocketBroadcastService.broadcastKline(kline);
    }

    private void updateTickerState(Ticker ticker, long price, long quantity, long quoteQuantity) {
        // Ticker 도메인 엔티티 내부에 원시 타입 기반의 update 로직이 구현되어 있어야 합니다.
        // 예시: ticker.update(price, quantity, quoteQuantity);
    }

    private Kline createNewKline(long marketId, long openTimeMs, long price, long volume, long quoteVolume) {
        return Kline.builder()
                .id(TsidCreator.getTsid().toLong())
                .marketId(marketId)
                .interval("1m") // 하드코딩 또는 상수 사용
                .openTimeMs(openTimeMs)
                .open(price)
                .high(price)
                .low(price)
                .close(price)
                .volume(volume)
                .quoteVolume(quoteVolume)
                .build();
    }

    private void updateKlineState(Kline kline, long price, long quantity, long quoteQuantity) {
        kline.update(price, quantity, quoteQuantity);
    }

    private Ticker loadTickerFromDbOrInit(Long marketId) {
        return tickerRepository.findByMarketId(marketId)
                .orElse(Ticker.builder().marketId(marketId).build());
    }

    /**
     * 2. 1초에 한 번씩 인메모리 버퍼의 데이터를 DB에 비동기로 밀어넣습니다. (Batch Upsert)
     */
    @Scheduled(fixedRate = 1000)
    public void flushMarketDataToDb() {
        if (!tickerBuffer.isEmpty()) {
            List<Ticker> tickersToSave = new ArrayList<>(tickerBuffer.values());
            // JdbcTemplate을 이용한 Bulk UPSERT 구현 필요 (JPA saveAll은 비효율적)
            // tickerRepository.batchUpsert(tickersToSave);
        }

        if (!klineBuffer.isEmpty()) {
            List<Kline> klinesToSave = new ArrayList<>(klineBuffer.values());
            // JdbcTemplate을 이용한 Bulk UPSERT 구현 필요
            // klineRepository.batchUpsert(klinesToSave);

            // 오래된 캐시 정리 (메모리 누수 방지)
            cleanUpOldKlines();
        }
    }

    private LocalDateTime getFloorMinute(LocalDateTime time) {
        return time.truncatedTo(ChronoUnit.MINUTES);
    }

    private void cleanUpOldKlines() {
        // 현재 시간보다 2분 이상 지난 Kline 캐시 제거 로직
    }
}
