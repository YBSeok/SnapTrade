package com.project.snaptrade.market.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "klines")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Kline {
    @Id
    private long id;

    @Column(name = "market_id", nullable = false)
    private long marketId;

    @Column(name = "`interval`", length = 10, nullable = false)
    private String interval;

    @Column(name = "open_time_ms", nullable = false)
    private long openTimeMs;

    @Column(nullable = false) private long open;
    @Column(nullable = false) private long low;
    @Column(nullable = false) private long high;
    @Column(nullable = false) private long close;

    @Column(nullable = false) private long volume = 0L;

    @Column(name = "quote_volume", nullable = false)
    private long quoteVolume = 0L;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Builder
    public Kline(long id, long marketId, String interval, long openTimeMs, long open, long low, long high, long close, long volume, long quoteVolume) {
        this.id = id;
        this.marketId = marketId;
        this.interval = interval;
        this.openTimeMs = openTimeMs;
        this.open = open;
        this.low = low;
        this.high = high;
        this.close = close;
        this.volume = volume;
        this.quoteVolume = quoteVolume;
    }

    public void update(long tradePrice, long tradeQty, long tradeQuoteQty) {
        if (tradePrice > this.high) this.high = tradePrice;
        if (tradePrice < this.low)  this.low = tradePrice;
        this.close = tradePrice;

        this.volume += tradeQty;
        this.quoteVolume += tradeQuoteQty;
    }
}