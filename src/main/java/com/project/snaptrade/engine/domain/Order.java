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
    private Long id;

    @Column(name = "user_id", nullable = false) private Long userId;
    @Column(name = "market_id", nullable = false) private Long marketId;

    @Enumerated(EnumType.STRING)
    @Column(length = 10, nullable = false) private OrderSide side;

    @Enumerated(EnumType.STRING)
    @Column(name = "order_type", length = 20, nullable = false) private OrderType orderType;

    @Column(nullable = false) private long price;

    @Enumerated(EnumType.STRING)
    @Column(name = "time_in_force", length = 20, nullable = false) private TimeInForce timeInForce;

    @Column(name = "orig_qty", nullable = false)
    private long origQty;

    @Column(name = "executed_qty", nullable = false)
    private long executedQty = 0L;

    @Column(name = "cumulative_quote_qty", nullable = false)
    private long cumulativeQuoteQty = 0L;

    @Enumerated(EnumType.STRING)
    @Column(length = 20, nullable = false) private OrderStatus status = OrderStatus.NEW;

    @Column(name = "sequence_no") private Long sequenceNo;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false) private LocalDateTime createdAt;
    @UpdateTimestamp
    @Column(name = "updated_at") private LocalDateTime updatedAt;

    @Builder
    public Order(Long id, Long userId, Long marketId, OrderSide side, OrderType orderType, long price, TimeInForce timeInForce, long origQty) {
        this.id = id;
        this.userId = userId;
        this.marketId = marketId;
        this.side = side;
        this.orderType = orderType;
        this.price = price;
        this.timeInForce = timeInForce;
        this.origQty = origQty;
    }

    public long getRemainingQty() {
        return this.origQty - this.executedQty;
    }

    public void fill(long fillQty, long fillPrice) {
        this.executedQty += fillQty;
        this.cumulativeQuoteQty += (fillQty * fillPrice);
        this.status = this.executedQty >= this.origQty ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
    }

    public void assignSequenceNo(Long sequenceNo) {
        this.sequenceNo = sequenceNo;
    }

    public static Order reconstructForReplay(Long id, Long userId, Long marketId, OrderSide side, long price, long origQty) {
        Order order = new Order();
        order.id = id;
        order.userId = userId;
        order.marketId = marketId;
        order.side = side;
        order.price = price;
        order.origQty = origQty;

        order.executedQty = 0L;
        order.cumulativeQuoteQty = 0L;
        order.status = OrderStatus.NEW;

        order.orderType = OrderType.LIMIT;
        order.timeInForce = TimeInForce.GTC;

        return order;
    }
}
