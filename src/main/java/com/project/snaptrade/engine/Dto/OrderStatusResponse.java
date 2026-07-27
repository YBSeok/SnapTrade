package com.project.snaptrade.engine.Dto;

import com.project.snaptrade.engine.domain.constant.OrderStatus;

public record OrderStatusResponse(
        Long orderId,
        OrderStatus status,
        long executedQty,
        long origQty
) {}
