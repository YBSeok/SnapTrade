package com.project.snaptrade.engine.Dto;

import com.project.snaptrade.engine.domain.constant.OrderSide;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class CancelOrderRequestDto {
    private Long userId;
    private Long marketId;
    private OrderSide side;
    private long price;
}
