package com.project.snaptrade.engine.domain;

import com.project.snaptrade.engine.domain.constant.EventType;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "order_events")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderEvent {
    @Id
    private long id;

    @Column(name = "order_id", nullable = false) private Long orderId;
    @Column(name = "trade_id") private Long tradeId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", length = 30, nullable = false) private EventType eventType;

    @Enumerated(EnumType.STRING)
    @Column(name = "status_before", length = 20) private OrderStatus statusBefore;

    @Enumerated(EnumType.STRING)
    @Column(name = "status_after", length = 20) private OrderStatus statusAfter;

    @Column(name = "fill_qty", nullable = false)
    private long fillQty = 0L;

    @Column(name = "fill_price", nullable = false)
    private long fillPrice = 0L;

    @Column(columnDefinition = "TEXT") private String payload; // JSON 직렬화 데이터 보관용

    @CreationTimestamp
    @Column(name = "occured_at", updatable = false) private LocalDateTime occurredAt;

    @Builder
    public OrderEvent(long id, long orderId, Long tradeId, EventType eventType, OrderStatus statusBefore, OrderStatus statusAfter, long fillQty, long fillPrice, String payload) {
        this.id = id;
        this.orderId = orderId;
        this.tradeId = tradeId;
        this.eventType = eventType;
        this.statusBefore = statusBefore;
        this.statusAfter = statusAfter;
        this.fillQty = fillQty;
        this.fillPrice = fillPrice;
        this.payload = payload;
    }
}
