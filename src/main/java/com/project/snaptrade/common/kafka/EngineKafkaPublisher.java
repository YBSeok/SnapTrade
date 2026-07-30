package com.project.snaptrade.common.kafka;

import com.project.snaptrade.common.event.OrderNotificationEvent;
import com.project.snaptrade.engine.domain.OrderProjectionSnapshot;
import com.project.snaptrade.engine.domain.Trade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes post-journal engine events to Kafka.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EngineKafkaPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishTradeCompleted(Trade trade) {
        kafkaTemplate.send(
                KafkaTopics.TRADE_COMPLETED,
                String.valueOf(trade.getMarketId()),
                TradeCompletedMessage.from(trade)
        ).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish trade.completed tradeId={}", trade.getId(), ex);
            }
        });
    }

    public void publishOrderLifecycle(OrderNotificationEvent event) {
        kafkaTemplate.send(
                KafkaTopics.ORDER_LIFECYCLE,
                String.valueOf(event.userId()),
                event
        ).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish order.lifecycle orderId={}", event.orderId(), ex);
            }
        });
    }

    public void publishProjection(OrderProjectionSnapshot snapshot) {
        kafkaTemplate.send(
                KafkaTopics.ORDER_PROJECTION,
                String.valueOf(snapshot.id()),
                snapshot
        ).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish order.projection orderId={}", snapshot.id(), ex);
            }
        });
    }
}
