package com.project.snaptrade.common.websocket;

import com.project.snaptrade.market.domain.Kline;
import com.project.snaptrade.market.domain.Ticker;
import com.project.snaptrade.market.dto.KlineBroadcastDTO;
import com.project.snaptrade.market.dto.TickerBroadcastDTO;
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
    public void broadcastTicker(TickerBroadcastDTO dto) {
        messagingTemplate.convertAndSend("/topic/ticker/" + dto.getMarketId(), dto);
    }

    @Async("webSocketTaskExecutor")
    public void broadcastKline(KlineBroadcastDTO dto) {
        String destination = String.format("/topic/kline/%d/%s", dto.getMarketId(), dto.getInterval());
        messagingTemplate.convertAndSend(destination, dto);
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
