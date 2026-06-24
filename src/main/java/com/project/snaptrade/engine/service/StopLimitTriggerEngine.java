package com.project.snaptrade.engine.service;

import com.project.snaptrade.engine.domain.OrderTrace;
import com.project.snaptrade.market.dto.TradeCompletedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
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

    @Async("triggerEngineTaskExecutor")
    @EventListener
    public void onTradeCompleted(TradeCompletedEvent event) {
        long currentPrice = event.getTrade().getPrice();
        Long marketId = event.getTrade().getMarketId();

        List<OrderTrace> stopOrders = stopOrderBook.get(marketId);
        if (stopOrders == null || stopOrders.isEmpty()) return;

        for (OrderTrace stopOrderTrace : stopOrders) {
            long triggerPrice = stopOrderTrace.getRequestDto().getTriggerPrice();
            boolean isStopLossHit = false;

            if (stopOrderTrace.getRequestDto().isStopDown() && currentPrice <= triggerPrice) {
                isStopLossHit = true;
            } else if (!stopOrderTrace.getRequestDto().isStopDown() && currentPrice >= triggerPrice) {
                isStopLossHit = true;
            }

            if (isStopLossHit) {
                stopOrders.remove(stopOrderTrace);
                orderPreProcessor.validateAndEnqueue(stopOrderTrace);
            }
        }
    }
}
