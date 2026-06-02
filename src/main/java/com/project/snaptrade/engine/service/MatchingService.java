package com.project.snaptrade.engine.service;

import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.domain.Order;
import com.project.snaptrade.engine.domain.OrderEvent;
import com.project.snaptrade.engine.domain.Trade;
import com.project.snaptrade.engine.domain.constant.EventType;
import com.project.snaptrade.engine.domain.constant.OrderSide;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import com.project.snaptrade.engine.domain.constant.TimeInForce;
import com.project.snaptrade.engine.repository.OrderEventRepository;
import com.project.snaptrade.engine.repository.OrderRepository;
import com.project.snaptrade.engine.repository.TradeRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class MatchingService {

    private final OrderRepository orderRepository;
    private final TradeRepository tradeRepository;
    private final OrderEventRepository eventRepository;

    private final List<OrderStatus> ACTIVE_STATUSES = List.of(OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED);

    // 💥 [병목 지점 1] 매칭부터 로그 기록까지 단일 트랜잭션으로 묶여 톰캣 스레드가 블로킹됨
    @Transactional
    public void processOrder(OrderRequestDto request) {

        // 1. Taker(신규 진입자) 주문 생성 및 저장
        Order takerOrder = Order.builder()
                .userId(request.getUserId())
                .marketId(request.getMarketId())
                .side(request.getSide())
                .orderType(request.getOrderType())
                .price(request.getPrice())
                .timeInForce(request.getTimeInForce())
                .origQty(request.getQuantity())
                .build();

        orderRepository.save(takerOrder);

        // 이벤트 기록 (주문 접수)
        saveEvent(takerOrder, null, EventType.ORDER_PLACED, null, OrderStatus.NEW, BigDecimal.ZERO, BigDecimal.ZERO);

        BigDecimal remainingQty = takerOrder.getOrigQty();

        // 💥 [병목 지점 2] 체결 잔량이 남을 때까지 반복하는 Loop.
        // 루프 1회당 비관적 락(SELECT FOR UPDATE) 1회, UPDATE 2회, INSERT 3회의 DB I/O가 동기식으로 발생
        while (remainingQty.compareTo(BigDecimal.ZERO) > 0) {

            // 호가창 탐색 및 Row Lock 획득
            Optional<Order> makerOrderOpt = findMatchingOrder(request.getMarketId(), request.getSide(), request.getPrice());

            if (makerOrderOpt.isEmpty()) {
                break; // 매칭 대상 호가가 없으면 루프 탈출
            }

            Order makerOrder = makerOrderOpt.get();
            OrderStatus makerStatusBefore = makerOrder.getStatus();
            OrderStatus takerStatusBefore = takerOrder.getStatus();

            // 체결 수량 및 가격 산정 (가격은 먼저 대기하고 있던 Maker 기준)
            BigDecimal fillQty = remainingQty.min(makerOrder.getRemainingQty());
            BigDecimal fillPrice = makerOrder.getPrice();

            // 1) 양측 주문 상태 업데이트 (UPDATE)
            makerOrder.fill(fillQty, fillPrice);
            takerOrder.fill(fillQty, fillPrice);

            // 2) 체결 원장 기록 (INSERT)
            Trade trade = Trade.builder()
                    .marketId(request.getMarketId())
                    .makerOrderId(makerOrder.getId())
                    .takerOrderId(takerOrder.getId())
                    .buyerId(request.getSide() == OrderSide.BUY ? takerOrder.getUserId() : makerOrder.getUserId())
                    .sellerId(request.getSide() == OrderSide.SELL ? takerOrder.getUserId() : makerOrder.getUserId())
                    .price(fillPrice)
                    .quantity(fillQty)
                    .makerFee(BigDecimal.ZERO) // 실무는 정책에 따라 계산
                    .takerFee(BigDecimal.ZERO)
                    .build();
            tradeRepository.save(trade);

            // 3) 체결 이벤트 로그 양측 기록 (INSERT x 2)
            saveEvent(makerOrder, trade.getId(), EventType.TRADE_MATCHED, makerStatusBefore, makerOrder.getStatus(), fillQty, fillPrice);
            saveEvent(takerOrder, trade.getId(), EventType.TRADE_MATCHED, takerStatusBefore, takerOrder.getStatus(), fillQty, fillPrice);

            remainingQty = remainingQty.subtract(fillQty);
        }

        // IOC (Immediate-Or-Cancel) 처리 로직: 남은 잔량은 즉시 취소
        if (remainingQty.compareTo(BigDecimal.ZERO) > 0 && request.getTimeInForce() == TimeInForce.IOC) {
            OrderStatus statusBefore = takerOrder.getStatus();
            // 취소 처리를 위한 강제 상태 변경 (Entity 내부 메서드로 분리하는 것이 이상적)
            takerOrder = orderRepository.save(takerOrder);
            // 쿼리로 강제 취소 업데이트를 하거나 더티 체킹 활용
            saveEvent(takerOrder, null, EventType.ORDER_CANCELED, statusBefore, OrderStatus.CANCELED, BigDecimal.ZERO, BigDecimal.ZERO);
        }
    }

    // 💥 [병목 지점 3] 비관적 락(Pessimistic Lock)이 걸린 DB 쿼리 호출
    private Optional<Order> findMatchingOrder(Long marketId, OrderSide takerSide, BigDecimal takerPrice) {
        if (takerSide == OrderSide.BUY) {
            return orderRepository.findTopByMarketIdAndSideAndPriceLessThanEqualAndStatusInOrderByPriceAscCreatedAtAsc(
                    marketId, OrderSide.SELL, takerPrice, ACTIVE_STATUSES);
        } else {
            return orderRepository.findTopByMarketIdAndSideAndPriceGreaterThanEqualAndStatusInOrderByPriceDescCreatedAtAsc(
                    marketId, OrderSide.BUY, takerPrice, ACTIVE_STATUSES);
        }
    }

    private void saveEvent(Order order, Long tradeId, EventType eventType, OrderStatus statusBefore, OrderStatus statusAfter, BigDecimal fillQty, BigDecimal fillPrice) {
        eventRepository.save(OrderEvent.builder()
                .orderId(order.getId())
                .tradeId(tradeId)
                .eventType(eventType)
                .statusBefore(statusBefore)
                .statusAfter(statusAfter)
                .fillQty(fillQty)
                .fillPrice(fillPrice)
                .payload("{}")
                .build());
    }
}
