package com.project.snaptrade.common.websocket;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserNotificationService {

    private final SimpMessagingTemplate messagingTemplate;

    @Async("webSocketTaskExecutor")
    public void sendToUser(Long userId, Object notificationData) {
        messagingTemplate.convertAndSendToUser(
                String.valueOf(userId),
                "/queue/notifications",
                notificationData
        );
    }
}
