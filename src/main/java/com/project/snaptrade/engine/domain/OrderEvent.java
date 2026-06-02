package com.project.snaptrade.engine.domain;

import com.project.snaptrade.engine.domain.constant.EventType;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "order_events")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class OrderEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false) private Long orderId;
    @Column(name = "trade_id") private Long tradeId; // 단순 주문 생성/취소일 경우 null 허용

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", length = 30, nullable = false) private EventType eventType;

    @Enumerated(EnumType.STRING)
    @Column(name = "status_before", length = 20) private OrderStatus statusBefore;

    @Enumerated(EnumType.STRING)
    @Column(name = "status_after", length = 20) private OrderStatus statusAfter;

    @Column(name = "fill_qty", precision = 36, scale = 18) private BigDecimal fillQty;
    @Column(name = "fill_price", precision = 36, scale = 18) private BigDecimal fillPrice;

    @Column(columnDefinition = "TEXT") private String payload; // JSON 직렬화 데이터 보관용

    @CreationTimestamp
    @Column(name = "occured_at", updatable = false) private LocalDateTime occurredAt;

    @Builder
    public OrderEvent(Long orderId, Long tradeId, EventType eventType, OrderStatus statusBefore, OrderStatus statusAfter, BigDecimal fillQty, BigDecimal fillPrice, String payload) {
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
