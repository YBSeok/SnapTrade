package com.project.snaptrade.market.domain;

public record MarketSpec(
        Long marketId,
        String symbol,

        // 가격 제약
        long minPrice,
        long maxPrice,
        long tickSize,

        // 수량 제약
        long minQty,
        long maxQty,
        long stepSize,

        // 명목 금액
        long minNotional,

        // 수수료율
        long makerFeeRate,
        long takerFeeRate
) {}
