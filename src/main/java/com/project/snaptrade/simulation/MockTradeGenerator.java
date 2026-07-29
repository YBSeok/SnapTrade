package com.project.snaptrade.simulation;

import com.project.snaptrade.common.kafka.EngineKafkaPublisher;
import com.project.snaptrade.engine.domain.Trade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class MockTradeGenerator {

    private final EngineKafkaPublisher engineKafkaPublisher;

    @Scheduled(fixedRateString = "${simulation.trade-interval-ms:100}")
    public void generateMockTrade() {
        long marketId = 1L;
        long price = ThreadLocalRandom.current().nextLong(90_000_000, 95_000_000);
        long quantity = ThreadLocalRandom.current().nextLong(1, 5);
        long quoteQuantity = price * quantity;

        Trade mockTrade = Trade.builder()
                .id(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE))
                .marketId(marketId)
                .makerOrderId(1001L)
                .takerOrderId(1002L)
                .makerUserId(10L)
                .takerUserId(20L)
                .buyerId(10L)
                .sellerId(20L)
                .price(price)
                .quantity(quantity)
                .quoteQuantity(quoteQuantity)
                .makerFee(0L)
                .takerFee(0L)
                .sequenceNo(System.currentTimeMillis())
                .build();

        if (log.isDebugEnabled()) {
            log.debug("Simulated Trade Created - Market: {}, Price: {}", marketId, price);
        }

        engineKafkaPublisher.publishTradeCompleted(mockTrade);
    }
}
