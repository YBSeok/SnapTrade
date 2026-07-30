package com.project.snaptrade.engine.service;

import com.project.snaptrade.common.kafka.KafkaTopics;
import com.project.snaptrade.common.kafka.TradeCompletedMessage;
import com.project.snaptrade.engine.domain.OrderTrace;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@RequiredArgsConstructor
public class StopLimitTriggerEngine {
    private final OrderPreProcessor orderPreProcessor;
    private final ConcurrentHashMap<Long, List<OrderTrace>> stopOrderBook = new ConcurrentHashMap<>();

    public void registerStopOrder(OrderTrace trace) {
        stopOrderBook.computeIfAbsent(trace.getRequestDto().getMarketId(), k -> new CopyOnWriteArrayList<>())
                .add(trace);
    }

    @KafkaListener(topics = KafkaTopics.TRADE_COMPLETED, groupId = "stop-trigger-service")
    public void onTradeCompleted(TradeCompletedMessage message) {
        long currentPrice = message.price();
        Long marketId = message.marketId();

        List<OrderTrace> stopOrders = stopOrderBook.get(marketId);
        if (stopOrders == null || stopOrders.isEmpty()) return;

        stopOrders.removeIf(stopOrderTrace -> {
            long triggerPrice = stopOrderTrace.getRequestDto().getTriggerPrice();
            boolean isStopDown = stopOrderTrace.getRequestDto().isStopDown();

            boolean isStopLossHit = (isStopDown && currentPrice <= triggerPrice) ||
                    (!isStopDown && currentPrice >= triggerPrice);

            if (isStopLossHit) {
                orderPreProcessor.validateAndEnqueue(stopOrderTrace);
                return true;
            }
            return false;
        });
    }
}
