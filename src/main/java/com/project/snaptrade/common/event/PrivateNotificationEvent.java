package com.project.snaptrade.common.event;

public record PrivateNotificationEvent(
        Long userId,
        String notificationType,
        String message,
        Object payload
) {}