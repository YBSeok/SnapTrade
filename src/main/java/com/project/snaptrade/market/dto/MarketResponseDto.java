package com.project.snaptrade.market.dto;

import com.project.snaptrade.market.domain.Market;
import com.project.snaptrade.market.domain.MarketStatus;

import java.math.BigDecimal;

public record MarketResponseDto(
        Long id,
        String symbol,
        BigDecimal minNotional,
        MarketStatus status,
        String baseAsset,
        String quoteAsset,
        BigDecimal minPrice,
        BigDecimal tickSize,
        BigDecimal minQty,
        BigDecimal stepSize
) {
    public static MarketResponseDto from(Market market) {
        return new MarketResponseDto(
                market.getId(),
                market.getSymbol(),
                market.getMinNotional(),
                market.getStatus(),
                market.getBaseAsset(),
                market.getQuoteAsset(),
                market.getMinPrice(),
                market.getTickSize(),
                market.getMinQty(),
                market.getStepSize()
        );
    }
}
