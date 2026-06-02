package com.project.snaptrade.engine.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "trades")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Trade {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "market_id", nullable = false) private Long marketId;
    @Column(name = "maker_order_id", nullable = false) private Long makerOrderId;
    @Column(name = "taker_order_id", nullable = false) private Long takerOrderId;

    @Column(name = "buyer_id", nullable = false) private Long buyerId;
    @Column(name = "seller_id", nullable = false) private Long sellerId;

    @Column(precision = 36, scale = 18, nullable = false) private BigDecimal price;
    @Column(precision = 36, scale = 18, nullable = false) private BigDecimal quantity;

    @Column(name = "quote_quantity", precision = 36, scale = 18, nullable = false)
    private BigDecimal quoteQuantity; // 체결 대금 (price * quantity)

    @Column(name = "maker_fee", precision = 36, scale = 18) private BigDecimal makerFee;
    @Column(name = "taker_fee", precision = 36, scale = 18) private BigDecimal takerFee;

    @Column(name = "sequence_no") private Long sequenceNo;

    @CreationTimestamp
    @Column(name = "traded_at", updatable = false) private LocalDateTime tradedAt;

    @Builder
    public Trade(Long marketId, Long makerOrderId, Long takerOrderId, Long buyerId, Long sellerId, BigDecimal price, BigDecimal quantity, BigDecimal makerFee, BigDecimal takerFee) {
        this.marketId = marketId;
        this.makerOrderId = makerOrderId;
        this.takerOrderId = takerOrderId;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
        this.price = price;
        this.quantity = quantity;
        this.quoteQuantity = price.multiply(quantity);
        this.makerFee = makerFee;
        this.takerFee = takerFee;
    }
}