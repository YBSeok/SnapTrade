package com.project.snaptrade.market.dto;

import com.project.snaptrade.engine.domain.Trade;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

public record TradeCompletedEvent(Trade trade) {
}
