package com.project.snaptrade.market.dto;

import com.project.snaptrade.market.domain.Ticker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class TickerBroadcastDTO {
    private Long marketId;
    private long lastPrice;
    private long priceChange;
    private long priceChangePct;
    private long high24h;
    private long low24h;
    private long volume24h;
    private long quoteVolume24h;
    private long tradeCount24h;
    private long timestamp;

    public static TickerBroadcastDTO from(Ticker ticker) {
        return TickerBroadcastDTO.builder()
                .marketId(ticker.getMarketId())
                .lastPrice(ticker.getLastPrice())
                .priceChange(ticker.getPriceChange())
                .priceChangePct(ticker.getPriceChangePct())
                .high24h(ticker.getHigh24h())
                .low24h(ticker.getLow24h())
                .volume24h(ticker.getVolume24h())
                .quoteVolume24h(ticker.getQuoteVolume24h())
                .tradeCount24h(ticker.getTradeCount24h())
                .timestamp(System.currentTimeMillis())
                .build();
    }
}