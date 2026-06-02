package com.project.snaptrade.engine.Dto;

import com.project.snaptrade.engine.domain.constant.OrderSide;
import com.project.snaptrade.engine.domain.constant.OrderType;
import com.project.snaptrade.engine.domain.constant.TimeInForce;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderRequestDto {
    private Long userId;
    private Long marketId;
    private OrderSide side;
    private OrderType orderType;
    private TimeInForce timeInForce;
    private BigDecimal price;
    private BigDecimal quantity;
}
