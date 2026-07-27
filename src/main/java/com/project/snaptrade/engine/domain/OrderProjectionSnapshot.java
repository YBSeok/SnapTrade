package com.project.snaptrade.engine.domain;

import com.project.snaptrade.engine.domain.constant.OrderSide;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import com.project.snaptrade.engine.domain.constant.OrderType;
import com.project.snaptrade.engine.domain.constant.TimeInForce;

public record OrderProjectionSnapshot(
        long id,
        long userId,
        long marketId,
        OrderSide side,
        OrderType orderType,
        TimeInForce timeInForce,
        long price,
        long origQty,
        long executedQty,
        long cumulativeQuoteQty,
        OrderStatus status,
        Long sequenceNo
) {
    public static OrderProjectionSnapshot from(Order order) {
        return new OrderProjectionSnapshot(
                order.getId(),
                order.getUserId(),
                order.getMarketId(),
                order.getSide(),
                order.getOrderType(),
                order.getTimeInForce(),
                order.getPrice(),
                order.getOrigQty(),
                order.getExecutedQty(),
                order.getCumulativeQuoteQty(),
                order.getStatus(),
                order.getSequenceNo()
        );
    }
}
