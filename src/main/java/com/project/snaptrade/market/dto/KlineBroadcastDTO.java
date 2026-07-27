package com.project.snaptrade.market.dto;

import com.project.snaptrade.market.domain.Kline;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class KlineBroadcastDTO {
    private long marketId;
    private String interval;
    private long openTimeMs;
    private long openPrice;
    private long highPrice;
    private long lowPrice;
    private long closePrice;
    private long volume;
    private long quoteVolume;
    private long timestamp;

    public static KlineBroadcastDTO from(Kline k) {
        return KlineBroadcastDTO.builder()
                .marketId(k.getMarketId())
                .interval(k.getInterval())
                .openTimeMs(k.getOpenTimeMs())
                .openPrice(k.getOpenPrice())
                .highPrice(k.getHighPrice())
                .lowPrice(k.getLowPrice())
                .closePrice(k.getClosePrice())
                .volume(k.getVolume())
                .quoteVolume(k.getQuoteVolume())
                .timestamp(System.currentTimeMillis())
                .build();
    }
}
