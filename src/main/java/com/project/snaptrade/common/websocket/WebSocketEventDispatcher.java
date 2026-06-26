package com.project.snaptrade.common.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.snaptrade.common.event.MarketDataUpdatedEvent;
import com.project.snaptrade.common.event.PrivateNotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketEventDispatcher {

    private final ExchangeWebSocketHandler webSocketHandler;
    private final ObjectMapper objectMapper;

    @Async
    @EventListener
    public void handleMarketDataEvent(MarketDataUpdatedEvent event) {
        try {
            String channelKey = event.topic() + "_" + event.symbol();

            Map<String, Object> message = new HashMap<>();
            message.put("type", event.topic());
            message.put("symbol", event.symbol());
            message.put("payload", event.payload());

            webSocketHandler.broadcast(channelKey, objectMapper.writeValueAsString(message));
        } catch (JsonProcessingException e) {
            log.error("JSON serialization error for MarketData", e);
        }
    }

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handlePrivateNotificationEvent(PrivateNotificationEvent event) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("type", "NOTIFICATION");
            message.put("notificationType", event.notificationType());
            message.put("message", event.message());
            message.put("payload", event.payload());

            webSocketHandler.sendToUser(event.userId(), objectMapper.writeValueAsString(message));
        } catch (JsonProcessingException e) {
            log.error("JSON serialization error for PrivateNotification", e);
        }
    }
}
