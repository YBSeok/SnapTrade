package com.project.snaptrade.common.event;

import com.project.snaptrade.engine.domain.constant.EventType;
import com.project.snaptrade.engine.domain.constant.OrderStatus;

public record OrderNotificationEvent(
        Long userId,
        Long orderId,
        Long marketId,
        EventType eventType,
        OrderStatus statusBefore,
        OrderStatus statusAfter,
        long fillQty,
        long fillPrice,
        long executedQty,
        long origQty
) {}
