package com.project.snaptrade.market.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(
        name = "markets",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"base_asset", "quote_asset"})}
)
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Market {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Transient
    public String getSymbol() {
        return this.baseAsset + "/" + this.quoteAsset;
    }

    @Column(name = "base_asset", nullable = false, length = 20)
    private String baseAsset;

    @Column(name = "quote_asset", nullable = false, length = 20)
    private String quoteAsset;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private MarketStatus status;

    @Column(nullable = false)
    private long minPrice;

    @Column(nullable = false)
    private long maxPrice;

    @Column(nullable = false)
    private long tickSize;

    @Column(nullable = false)
    private long minQty;

    @Column(nullable = false)
    private long maxQty;

    @Column(nullable = false)
    private long stepSize;

    @Column(nullable = false)
    private long minNotional;

    @Column(nullable = false)
    private long makerFeeRate;

    @Column(nullable = false)
    private long takerFeeRate;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Transient
    public boolean isActive() {
        return this.status == MarketStatus.ACTIVE;
    }

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
    }
}