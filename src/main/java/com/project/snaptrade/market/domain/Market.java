package com.project.snaptrade.market.domain;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "markets")
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Market {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 20)
    private String symbol;

    @Column(nullable = false, precision = 36, scale = 18)
    private BigDecimal minNotional;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private MarketStatus status;

    @Column(nullable = false, length = 20)
    private String baseAsset;

    @Column(nullable = false, length = 20)
    private String quoteAsset;

    @Column(nullable = false, precision = 36, scale = 18)
    private BigDecimal minPrice;

    @Column(nullable = false, precision = 36, scale = 18)
    private BigDecimal tickSize;

    @Column(nullable = false, precision = 36, scale = 18)
    private BigDecimal minQty;

    @Column(nullable = false, precision = 36, scale = 18)
    private BigDecimal stepSize;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
    }
}
