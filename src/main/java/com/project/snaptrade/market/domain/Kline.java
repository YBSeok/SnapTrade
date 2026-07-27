package com.project.snaptrade.market.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(
        name = "klines",
        uniqueConstraints = {
                @UniqueConstraint(name = "uk_klines_identity", columnNames = {"market_id", "interval", "open_time_ms"})
        }
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Kline {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "market_id", nullable = false)
    private long marketId;

    @Column(name = "`interval`", length = 10, nullable = false)
    private String interval;

    @Column(name = "open_time_ms", nullable = false)
    private long openTimeMs;

    @Column(nullable = false) private long openPrice;
    @Column(nullable = false) private long lowPrice;
    @Column(nullable = false) private long highPrice;
    @Column(nullable = false) private long closePrice;

    @Column(nullable = false) private long volume = 0L;

    @Column(name = "quote_volume", nullable = false)
    private long quoteVolume = 0L;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Builder
    public Kline(long id, long marketId, String interval, long openTimeMs, long openPrice, long lowPrice, long highPrice, long closePrice, long volume, long quoteVolume) {
        this.id = id;
        this.marketId = marketId;
        this.interval = interval;
        this.openTimeMs = openTimeMs;
        this.openPrice = openPrice;
        this.lowPrice = lowPrice;
        this.highPrice = highPrice;
        this.closePrice = closePrice;
        this.volume = volume;
        this.quoteVolume = quoteVolume;
    }

    public void update(long tradePrice, long tradeQty, long tradeQuoteQty) {
        if (tradePrice > this.highPrice) this.highPrice = tradePrice;
        if (tradePrice < this.lowPrice)  this.lowPrice = tradePrice;
        this.closePrice = tradePrice;

        this.volume += tradeQty;
        this.quoteVolume += tradeQuoteQty;
    }
}