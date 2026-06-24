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
    @Id
    private long id;

    @Column(name = "market_id", nullable = false) private Long marketId;
    @Column(name = "maker_order_id", nullable = false) private Long makerOrderId;
    @Column(name = "taker_order_id", nullable = false) private Long takerOrderId;

    @Column(name = "maker_user_id", nullable = false) private Long makerUserId;
    @Column(name = "taker_user_id", nullable = false) private Long takerUserId;

    @Column(name = "buyer_id", nullable = false) private Long buyerId;
    @Column(name = "seller_id", nullable = false) private Long sellerId;

    @Column(nullable = false) private long price;
    @Column(nullable = false) private long quantity;
    @Column(name = "quote_quantity", nullable = false)
    private long quoteQuantity;

    @Column(name = "maker_fee", nullable = false) private long makerFee = 0L;
    @Column(name = "taker_fee", nullable = false) private long takerFee = 0L;

    @Column(name = "sequence_no") private Long sequenceNo;

    @CreationTimestamp
    @Column(name = "traded_at", updatable = false) private LocalDateTime tradedAt;

    @Builder
    public Trade(long id, long marketId, long makerOrderId, long takerOrderId,
                 Long makerUserId, Long takerUserId, // 생성자 파라미터 추가
                 long buyerId, long sellerId, long price, long quantity,
                 long quoteQuantity, long makerFee, long takerFee, Long sequenceNo) {
        this.id = id;
        this.marketId = marketId;
        this.makerOrderId = makerOrderId;
        this.takerOrderId = takerOrderId;
        this.makerUserId = makerUserId;
        this.takerUserId = takerUserId;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
        this.price = price;
        this.quantity = quantity;
        this.quoteQuantity = quoteQuantity;
        this.makerFee = makerFee;
        this.takerFee = takerFee;
        this.sequenceNo = sequenceNo;
    }
}