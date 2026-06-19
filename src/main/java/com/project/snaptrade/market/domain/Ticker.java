package com.project.snaptrade.market.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
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

    @Column(name = "last_price", precision = 36, scale = 18) private BigDecimal lastPrice;
    @Column(name = "price_change", precision = 36, scale = 18) private BigDecimal priceChange;
    @Column(name = "price_change_pct", precision = 36, scale = 18) private BigDecimal priceChangePct;

    @Column(name = "high_24h", precision = 36, scale = 18) private BigDecimal high24h;
    @Column(name = "low_24h", precision = 36, scale = 18) private BigDecimal low24h;
    @Column(name = "volume_24h", precision = 36, scale = 18) private BigDecimal volume24h;
    @Column(name = "quote_volume_24h", precision = 36, scale = 18) private BigDecimal quoteVolume24h;

    @Column(name = "trade_count_24h")
    private Long tradeCount24h;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Builder
    public Ticker(Long id, Long marketId, BigDecimal lastPrice, BigDecimal priceChange, BigDecimal priceChangePct, BigDecimal high24h, BigDecimal low24h, BigDecimal volume24h, BigDecimal quoteVolume24h, Long tradeCount24h) {
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
}