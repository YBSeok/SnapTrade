package com.project.snaptrade.market.service;

import com.github.f4b6a3.tsid.TsidCreator;
import com.project.snaptrade.engine.domain.Trade;
import com.project.snaptrade.market.domain.Kline;
import com.project.snaptrade.market.domain.Ticker;
import com.project.snaptrade.market.domain.constant.ChartInterval;
import com.project.snaptrade.market.dto.TradeCompletedEvent;
import com.project.snaptrade.market.repository.KlineJdbcRepository;
import com.project.snaptrade.market.repository.KlineRepository;
import com.project.snaptrade.market.repository.TickerJdbcRepository;
import com.project.snaptrade.market.repository.TickerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Service
@RequiredArgsConstructor
public class MarketDataService {
    private final WebSocketBroadcastService webSocketBroadcastService;

    private final KlineRepository klineRepository;
    private final KlineJdbcRepository klineJdbcRepository;
    private final TickerRepository tickerRepository;
    private final TickerJdbcRepository tickerJdbcRepository;

    private record KlineKey(long marketId, ChartInterval interval) {}
    private final ConcurrentHashMap<KlineKey, Kline> klineBuffer = new ConcurrentHashMap<>();
    private static final List<ChartInterval> HOT_INTERVALS = List.of(ChartInterval.SEC_1, ChartInterval.MIN_1);

    private final Map<Long, Ticker> tickerBuffer = new ConcurrentHashMap<>();
    private final Map<Long, Long> openPrice24hCache = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<Kline> pendingKlinesToDb = new ConcurrentLinkedQueue<>();

    @Async("marketDataTaskExecutor")
    @EventListener
    public void onTradeCompleted(TradeCompletedEvent event) {
        Trade trade = event.getTrade();
        long marketId = trade.getMarketId();
        long price = trade.getPrice();
        long quantity = trade.getQuantity();
        long quoteQuantity = trade.getQuoteQuantity();
        long currentMs = System.currentTimeMillis();

        // 1. Ticker 갱신
        // O(1) 캐시에서 24시간 전 시가를 가져옵니다. (캐시에 없으면 현재가로 임시 세팅하여 등락률 0% 처리)
        long openPrice24h = openPrice24hCache.getOrDefault(marketId, price);

        Ticker ticker = tickerBuffer.computeIfAbsent(marketId, this::loadTickerFromDbOrInit);
        ticker.update(price, quantity, quoteQuantity, openPrice24h);

        webSocketBroadcastService.broadcastTicker(ticker);

        // 2. Kline 갱신 (1초, 1분봉)
        for (ChartInterval interval : HOT_INTERVALS) {
            KlineKey key = new KlineKey(marketId, interval);
            long openTimeMs = interval.getOpenTimeMs(currentMs);

            Kline kline = klineBuffer.compute(key, (k, existing) -> {
                if (existing == null) {
                    return createNewKline(marketId, interval.getCode(), openTimeMs, price, quantity, quoteQuantity);
                }

                if (existing.getOpenTimeMs() != openTimeMs) {
                    pendingKlinesToDb.offer(existing);
                    return createNewKline(marketId, interval.getCode(), openTimeMs, price, quantity, quoteQuantity);
                }

                existing.update(price, quantity, quoteQuantity);
                return existing;
            });

            // Kline은 1초봉, 1분봉이 생성/갱신될 때마다 각각 브로드캐스트합니다.
            webSocketBroadcastService.broadcastKline(kline);
        }
    }

    @Scheduled(fixedRate = 1000)
    public void flushMarketDataToDb() {
        if (!tickerBuffer.isEmpty()) {
            tickerJdbcRepository.batchUpsert(new ArrayList<>(tickerBuffer.values()));
        }

        List<Kline> klinesToSave = new ArrayList<>();

        for (Kline kline : klineBuffer.values()) {
            if (!ChartInterval.SEC_1.getCode().equals(kline.getInterval())) {
                klinesToSave.add(kline);
            }
        }

        Kline pending;
        while ((pending = pendingKlinesToDb.poll()) != null) {
            if (!ChartInterval.SEC_1.getCode().equals(pending.getInterval())) {
                klinesToSave.add(pending);
            }
        }

        if (!klinesToSave.isEmpty()) {
            klineJdbcRepository.batchUpsert(klinesToSave);
        }
    }

    @Scheduled(fixedRate = 60000)
    public void refreshOpenPrice24hCache() {
        if (tickerBuffer.isEmpty()) return;

        long currentMs = System.currentTimeMillis();
        long targetTimeMs = currentMs - (24 * 60 * 60 * 1000L);
        targetTimeMs = targetTimeMs - (targetTimeMs % 60000);

        for (Long marketId : tickerBuffer.keySet()) {
            klineRepository.findClosestOldClosePrice(marketId, targetTimeMs)
                    .ifPresent(pastPrice -> openPrice24hCache.put(marketId, pastPrice));
        }
    }

    private Kline createNewKline(long marketId, String intervalCode, long openTimeMs, long price, long volume, long quoteVolume) {
        return Kline.builder()
                .id(TsidCreator.getTsid().toLong())
                .marketId(marketId)
                .interval(intervalCode)
                .openTimeMs(openTimeMs)
                .open(price)
                .high(price)
                .low(price)
                .close(price)
                .volume(volume)
                .quoteVolume(quoteVolume)
                .build();
    }

    private Ticker loadTickerFromDbOrInit(Long marketId) {
        return tickerRepository.findByMarketId(marketId)
                .orElse(Ticker.builder().marketId(marketId).build());
    }
}
