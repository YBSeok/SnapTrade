package com.project.snaptrade.market.dto;

import com.project.snaptrade.market.domain.MarketStatus;

import java.math.BigDecimal;

public record MarketRequestDto(
        String symbol,
        BigDecimal minNotional,
        MarketStatus status,
        String baseAsset,
        String quoteAsset,
        BigDecimal minPrice,
        BigDecimal tickSize,
        BigDecimal minQty,
        BigDecimal stepSize
) {}
