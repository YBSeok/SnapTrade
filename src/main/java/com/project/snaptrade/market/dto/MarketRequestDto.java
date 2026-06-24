package com.project.snaptrade.market.dto;

import com.project.snaptrade.market.domain.Market;
import com.project.snaptrade.market.domain.MarketStatus;

import java.math.BigDecimal;

public record MarketRequestDto(
        String baseAsset,
        String quoteAsset,
        MarketStatus status,

        BigDecimal minPrice,
        BigDecimal maxPrice,
        BigDecimal tickSize,

        BigDecimal minQty,
        BigDecimal maxQty,
        BigDecimal stepSize,

        BigDecimal minNotional,
        BigDecimal makerFeeRate,
        BigDecimal takerFeeRate
) {
    private static final BigDecimal SCALE_FACTOR = new BigDecimal("100000000");
    private static final BigDecimal FEE_SCALE_FACTOR = new BigDecimal("1000000");

    public Market toEntity() {
        return Market.builder()
                .baseAsset(this.baseAsset)
                .quoteAsset(this.quoteAsset)
                .status(this.status)

                .minPrice(scaleToLong(this.minPrice, SCALE_FACTOR))
                .maxPrice(scaleToLong(this.maxPrice, SCALE_FACTOR))
                .tickSize(scaleToLong(this.tickSize, SCALE_FACTOR))

                .minQty(scaleToLong(this.minQty, SCALE_FACTOR))
                .maxQty(scaleToLong(this.maxQty, SCALE_FACTOR))
                .stepSize(scaleToLong(this.stepSize, SCALE_FACTOR))

                .minNotional(scaleToLong(this.minNotional, SCALE_FACTOR))

                .makerFeeRate(scaleToLong(this.makerFeeRate, FEE_SCALE_FACTOR))
                .takerFeeRate(scaleToLong(this.takerFeeRate, FEE_SCALE_FACTOR))
                .build();
    }

    private long scaleToLong(BigDecimal value, BigDecimal factor) {
        if (value == null) return 0L;
        return value.multiply(factor).longValue();
    }
}
