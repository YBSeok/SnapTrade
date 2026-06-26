package com.project.snaptrade.common.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.snaptrade.auth.security.JwtProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExchangeWebSocketHandler extends TextWebSocketHandler {

    private final ObjectMapper objectMapper;
    private final JwtProvider jwtProvider;

    private final ConcurrentHashMap<Long, Set<WebSocketSession>> userSessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<WebSocketSession>> topicSubscribers = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("New WebSocket connection established: {}", session.getId());
        session.getAttributes().put("connectedAt", System.currentTimeMillis());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        JsonNode jsonMessage = objectMapper.readTree(message.getPayload());
        String type = jsonMessage.get("type").asText();

        switch (type) {
            case "PING":
                session.sendMessage(new TextMessage("{\"type\":\"PONG\"}"));
                break;
            case "AUTH":
                handleAuth(session, jsonMessage.get("token").asText());
                break;
            case "SUBSCRIBE":
                handleSubscribe(session, jsonMessage);
                break;
            case "UNSUBSCRIBE":
                handleUnsubscribe(session, jsonMessage);
                break;
            default:
                log.warn("Unknown message type: {}", type);
        }
    }

    private void handleAuth(WebSocketSession session, String token) throws Exception {
        if (jwtProvider.validateToken(token)) {
            Long userId = jwtProvider.getUserIdFromToken(token);
            session.getAttributes().put("userId", userId);

            userSessions.computeIfAbsent(userId, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                    .add(session);

            session.sendMessage(new TextMessage("{\"type\":\"AUTH_SUCCESS\"}"));
            log.info("Session {} authenticated as User {}", session.getId(), userId);
        } else {
            session.sendMessage(new TextMessage("{\"type\":\"ERROR\", \"message\":\"Invalid Token\"}"));
        }
    }

    private void handleSubscribe(WebSocketSession session, JsonNode jsonMessage) throws Exception {
        String topic = jsonMessage.get("topic").asText();
        String symbol = jsonMessage.has("symbol") ? jsonMessage.get("symbol").asText() : "";
        String channelKey = topic + (symbol.isEmpty() ? "" : "_" + symbol);

        if (topic.equals("NOTIFICATION") || topic.equals("MY_ORDERS")) {
            if (!session.getAttributes().containsKey("userId")) {
                session.sendMessage(new TextMessage("{\"type\":\"ERROR\", \"message\":\"Auth required for " + topic + "\"}"));
                return;
            }
        }

        topicSubscribers.computeIfAbsent(channelKey, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                .add(session);

        Set<String> mySubscriptions = (Set<String>) session.getAttributes().computeIfAbsent("subscriptions",
                k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
        mySubscriptions.add(channelKey);

        log.info("Session {} subscribed to {}", session.getId(), channelKey);
    }

    private void handleUnsubscribe(WebSocketSession session, JsonNode jsonMessage) {
        String topic = jsonMessage.get("topic").asText();
        String symbol = jsonMessage.has("symbol") ? jsonMessage.get("symbol").asText() : "";
        String channelKey = topic + (symbol.isEmpty() ? "" : "_" + symbol);

        Set<WebSocketSession> subs = topicSubscribers.get(channelKey);
        if(subs != null) subs.remove(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        Long userId = (Long) session.getAttributes().get("userId");
        if (userId != null && userSessions.containsKey(userId)) {
            userSessions.get(userId).remove(session);
            if(userSessions.get(userId).isEmpty()) {
                userSessions.remove(userId);
            }
        }

        Set<String> mySubscriptions = (Set<String>) session.getAttributes().get("subscriptions");
        if (mySubscriptions != null) {
            for (String channelKey : mySubscriptions) {
                Set<WebSocketSession> subs = topicSubscribers.get(channelKey);
                if (subs != null) {
                    subs.remove(session);
                }
            }
        }
        log.info("WebSocket connection closed: {}", session.getId());
    }

    public void broadcast(String channelKey, String messagePayload) {
        Set<WebSocketSession> subscribers = topicSubscribers.get(channelKey);
        if (subscribers == null || subscribers.isEmpty()) return;

        TextMessage message = new TextMessage(messagePayload);
        for (WebSocketSession session : subscribers) {
            if (session.isOpen()) {
                try {
                    session.sendMessage(message);
                } catch (IOException e) {
                    log.error("Failed to send broadcast message to session {}", session.getId(), e);
                }
            }
        }
    }

    public void sendToUser(Long userId, String messagePayload) {
        Set<WebSocketSession> sessions = userSessions.get(userId);
        if (sessions == null || sessions.isEmpty()) return;

        TextMessage message = new TextMessage(messagePayload);
        for (WebSocketSession session : sessions) {
            if (session.isOpen()) {
                try {
                    session.sendMessage(message);
                } catch (IOException e) {
                    log.error("Failed to send private message to user {} session {}", userId, session.getId(), e);
                }
            }
        }
    }
}