package com.project.snaptrade.market.dto;

import com.project.snaptrade.market.domain.Market;
import com.project.snaptrade.market.domain.MarketStatus;
import lombok.Builder;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;

@Builder
public record MarketResponseDto(
        Long id,
        String symbol,
        MarketStatus status,
        BigDecimal minPrice,
        BigDecimal maxPrice,
        BigDecimal tickSize,
        BigDecimal minQty,
        BigDecimal maxQty,
        BigDecimal stepSize,
        BigDecimal minNotional,
        BigDecimal makerFeeRate,
        BigDecimal takerFeeRate,
        LocalDateTime createdAt
) {
    private static final BigDecimal SCALE_FACTOR = new BigDecimal("100000000");
    private static final BigDecimal FEE_SCALE_FACTOR = new BigDecimal("1000000");

    public static MarketResponseDto from(Market market) {
        return MarketResponseDto.builder()
                .id(market.getId())
                .symbol(market.getSymbol())
                .status(market.getStatus())

                .minPrice(scaleDown(market.getMinPrice(), SCALE_FACTOR))
                .maxPrice(scaleDown(market.getMaxPrice(), SCALE_FACTOR))
                .tickSize(scaleDown(market.getTickSize(), SCALE_FACTOR))

                .minQty(scaleDown(market.getMinQty(), SCALE_FACTOR))
                .maxQty(scaleDown(market.getMaxQty(), SCALE_FACTOR))
                .stepSize(scaleDown(market.getStepSize(), SCALE_FACTOR))

                .minNotional(scaleDown(market.getMinNotional(), SCALE_FACTOR))

                .makerFeeRate(scaleDown(market.getMakerFeeRate(), FEE_SCALE_FACTOR))
                .takerFeeRate(scaleDown(market.getTakerFeeRate(), FEE_SCALE_FACTOR))

                .createdAt(market.getCreatedAt())
                .build();
    }

    private static BigDecimal scaleDown(long value, BigDecimal factor) {
        return BigDecimal.valueOf(value).divide(factor, 8, RoundingMode.HALF_UP).stripTrailingZeros();
    }
}
