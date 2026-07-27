package com.project.snaptrade.common.event;

public record PrivateNotificationEvent(
        Long userId,
        NotificationType notificationType,
        String message,
        Object payload
) {}