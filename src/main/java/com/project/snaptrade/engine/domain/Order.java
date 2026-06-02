package com.project.snaptrade.engine.domain;

import com.project.snaptrade.engine.domain.constant.OrderSide;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import com.project.snaptrade.engine.domain.constant.OrderType;
import com.project.snaptrade.engine.domain.constant.TimeInForce;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false) private Long userId;
    @Column(name = "market_id", nullable = false) private Long marketId;

    @Enumerated(EnumType.STRING)
    @Column(length = 10, nullable = false) private OrderSide side;

    @Enumerated(EnumType.STRING)
    @Column(name = "order_type", length = 20, nullable = false) private OrderType orderType;

    @Column(precision = 36, scale = 18, nullable = false) private BigDecimal price;

    @Enumerated(EnumType.STRING)
    @Column(name = "time_in_force", length = 20, nullable = false) private TimeInForce timeInForce;

    @Column(name = "orig_qty", precision = 36, scale = 18, nullable = false)
    private BigDecimal origQty;

    @Column(name = "excuted_qty", precision = 36, scale = 18, nullable = false)
    private BigDecimal executedQty = BigDecimal.ZERO;

    @Column(name = "cumulative_quote_qty", precision = 36, scale = 18, nullable = false)
    private BigDecimal cumulativeQuoteQty = BigDecimal.ZERO;

    @Enumerated(EnumType.STRING)
    @Column(length = 20, nullable = false) private OrderStatus status = OrderStatus.NEW;

    @Column(name = "sequence_no") private Long sequenceNo;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false) private LocalDateTime createdAt;
    @UpdateTimestamp
    @Column(name = "updated_at") private LocalDateTime updatedAt;

    @Builder
    public Order(Long userId, Long marketId, OrderSide side, OrderType orderType, BigDecimal price, TimeInForce timeInForce, BigDecimal origQty) {
        this.userId = userId;
        this.marketId = marketId;
        this.side = side;
        this.orderType = orderType;
        this.price = price;
        this.timeInForce = timeInForce;
        this.origQty = origQty;
    }

    public BigDecimal getRemainingQty() {
        return this.origQty.subtract(this.executedQty);
    }

    public void fill(BigDecimal fillQty, BigDecimal fillPrice) {
        this.executedQty = this.executedQty.add(fillQty);
        this.cumulativeQuoteQty = this.cumulativeQuoteQty.add(fillQty.multiply(fillPrice));
        this.status = this.executedQty.compareTo(this.origQty) >= 0 ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
    }
}
