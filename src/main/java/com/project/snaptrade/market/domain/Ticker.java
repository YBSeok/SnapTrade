package com.project.snaptrade.market.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "tickers")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Ticker {
    @Id
    private Long id;

    @Column(name = "market_id", nullable = false)
    private Long marketId;

    @Column(name = "last_price") private long lastPrice;
    @Column(name = "price_change") private long priceChange;
    @Column(name = "price_change_pct") private long priceChangePct;

    @Column(name = "high_24h") private long high24h;
    @Column(name = "low_24h") private long low24h;
    @Column(name = "volume_24h") private long volume24h;
    @Column(name = "quote_volume_24h") private long quoteVolume24h;

    @Column(name = "trade_count_24h")
    private long tradeCount24h;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Builder
    public Ticker(Long id, Long marketId, long lastPrice, long priceChange, long priceChangePct, long high24h, long low24h, long volume24h, long quoteVolume24h, long tradeCount24h) {
        this.id = id;
        this.marketId = marketId;
        this.lastPrice = lastPrice;
        this.priceChange = priceChange;
        this.priceChangePct = priceChangePct;
        this.high24h = high24h;
        this.low24h = low24h;
        this.volume24h = volume24h;
        this.quoteVolume24h = quoteVolume24h;
        this.tradeCount24h = tradeCount24h;
    }

    public void update(long price, long quantity, long quoteQuantity, long openPrice24h) {
        this.lastPrice = price;
        this.volume24h += quantity;
        this.quoteVolume24h += quoteQuantity;
        this.tradeCount24h++;

        if (price > this.high24h) {
            this.high24h = price;
        }

        if (this.low24h == 0 || price < this.low24h) {
            this.low24h = price;
        }

        if (openPrice24h > 0) {
            this.priceChange = price - openPrice24h;
            this.priceChangePct = (this.priceChange * 10000L) / openPrice24h;
        } else {
            this.priceChange = 0;
            this.priceChangePct = 0;
        }
    }
}