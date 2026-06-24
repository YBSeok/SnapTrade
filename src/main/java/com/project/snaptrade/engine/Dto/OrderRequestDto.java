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

    private String clientOrderId;

    private OrderSide side;
    private OrderType orderType;
    private TimeInForce timeInForce;
    private Long price;
    private Long quantity;

    private Long triggerPrice; // 스탑, 로스 가격
    private boolean stopDown; // true: 가격 하락 시 발동, false: 가격 상승 시 발동
}
