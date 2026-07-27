package com.project.snaptrade.notification;

import com.project.snaptrade.common.event.NotificationType;
import com.project.snaptrade.common.event.OrderNotificationEvent;
import com.project.snaptrade.common.websocket.UserNotificationService;
import com.project.snaptrade.engine.domain.constant.EventType;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderNotificationListener {

    private final UserNotificationService userNotificationService;

    @Async("webSocketTaskExecutor")
    @EventListener
    public void onOrderNotification(OrderNotificationEvent event) {
        NotificationType type = resolveType(event);
        if (type == null) return;

        Map<String, Object> payload = new HashMap<>();
        payload.put("orderId", event.orderId());
        payload.put("marketId", event.marketId());
        payload.put("status", event.statusAfter() != null ? event.statusAfter().name() : null);

        if (type == NotificationType.ORDER_FILLED || type == NotificationType.ORDER_PARTIALLY_FILLED) {
            payload.put("fillQty", event.fillQty());
            payload.put("fillPrice", event.fillPrice());
            payload.put("executedQty", event.executedQty());
            payload.put("origQty", event.origQty());
        }

        Map<String, Object> message = new HashMap<>();
        message.put("type", "NOTIFICATION");
        message.put("notificationType", type.name());
        message.put("message", buildMessage(type, event));
        message.put("payload", payload);

        userNotificationService.sendToUser(event.userId(), message);
    }

    private NotificationType resolveType(OrderNotificationEvent event) {
        return switch (event.eventType()) {
            case ORDER_PLACED -> NotificationType.ORDER_PLACED;
            case ORDER_CANCELED -> NotificationType.ORDER_CANCELED;
            case ORDER_REJECTED -> NotificationType.ORDER_REJECTED;
            case TRADE_MATCHED -> {
                if (event.statusAfter() == OrderStatus.FILLED) {
                    yield NotificationType.ORDER_FILLED;
                }
                yield NotificationType.ORDER_PARTIALLY_FILLED;
            }
        };
    }

    private String buildMessage(NotificationType type, OrderNotificationEvent event) {
        return switch (type) {
            case ORDER_PLACED -> "주문이 접수되었습니다.";
            case ORDER_FILLED -> "주문이 전량 체결되었습니다.";
            case ORDER_PARTIALLY_FILLED -> String.format(
                    "주문이 부분 체결되었습니다. (%d/%d)", event.executedQty(), event.origQty()
            );
            case ORDER_CANCELED -> "주문이 취소되었습니다.";
            case ORDER_REJECTED -> "주문이 거부되었습니다.";
            default -> "";
        };
    }
}
