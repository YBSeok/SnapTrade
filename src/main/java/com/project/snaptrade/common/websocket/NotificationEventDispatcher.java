package com.project.snaptrade.common.websocket;

import com.project.snaptrade.common.event.PrivateNotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationEventDispatcher {

    private final UserNotificationService userNotificationService;

    @Async("webSocketTaskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(PrivateNotificationEvent event) {
        Map<String, Object> message = new HashMap<>();
        message.put("type", "NOTIFICATION");
        message.put("notificationType", event.notificationType().name());
        message.put("message", event.message());
        message.put("payload", event.payload());

        userNotificationService.sendToUser(event.userId(), message);
    }
}
