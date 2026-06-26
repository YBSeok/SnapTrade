package com.project.snaptrade.common.websocket;

import com.project.snaptrade.market.domain.Kline;
import com.project.snaptrade.market.domain.Ticker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketBroadcastService {

    private final SimpMessagingTemplate messagingTemplate;

    @Async("webSocketTaskExecutor")
    public void broadcastTicker(Ticker ticker) {
        String destination = "/topic/ticker/" + ticker.getMarketId();
        messagingTemplate.convertAndSend(destination, ticker);
    }

    @Async("webSocketTaskExecutor")
    public void broadcastKline(Kline kline) {
        String destination = String.format("/topic/kline/%d/%s", kline.getMarketId(), kline.getInterval());
        messagingTemplate.convertAndSend(destination, kline);
    }

    @Async("webSocketTaskExecutor")
    public void sendPrivateNotification(Long userId, Object notificationData) {
        // /user/{userId}/queue/notifications
        messagingTemplate.convertAndSendToUser(
                String.valueOf(userId),
                "/queue/notifications",
                notificationData
        );
    }
}
