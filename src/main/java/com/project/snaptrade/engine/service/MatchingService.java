package com.project.snaptrade.engine.service;

import com.project.snaptrade.engine.Dto.MatchResult;
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
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class MatchingService {

    private final OrderRepository orderRepository;
    private final TradeRepository tradeRepository;
    private final OrderEventRepository eventRepository;

    private final org.springframework.beans.factory.ObjectProvider<MatchingService> matchingServiceProvider;

    private final List<OrderStatus> ACTIVE_STATUSES = List.of(OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED);

    public void processOrder(OrderRequestDto request) {
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

        saveEvent(takerOrder, null, EventType.ORDER_PLACED, null, OrderStatus.NEW, BigDecimal.ZERO, BigDecimal.ZERO);

        BigDecimal remainingQty = takerOrder.getOrigQty();

        while (remainingQty.compareTo(BigDecimal.ZERO) > 0) {
            MatchResult result = matchingServiceProvider.getObject().processSingleMatch(request, takerOrder, remainingQty);

            if (result == null) break;

            remainingQty = remainingQty.subtract(result.fillQty());
        }

        if (remainingQty.compareTo(BigDecimal.ZERO) > 0 && request.getTimeInForce() == TimeInForce.IOC) {
            OrderStatus statusBefore = takerOrder.getStatus();
            takerOrder = orderRepository.save(takerOrder);
            saveEvent(takerOrder, null, EventType.ORDER_CANCELED, statusBefore, OrderStatus.CANCELED, BigDecimal.ZERO, BigDecimal.ZERO);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public MatchResult processSingleMatch(OrderRequestDto request, Order takerOrder, BigDecimal remainingQty) {
        Optional<Order> makerOrderOpt = findMatchingOrder(request.getMarketId(), request.getSide(), request.getPrice());

        if (makerOrderOpt.isEmpty()) return null;

        Order makerOrder = makerOrderOpt.get();
        OrderStatus makerStatusBefore = makerOrder.getStatus();
        OrderStatus takerStatusBefore = takerOrder.getStatus();

        BigDecimal fillQty = remainingQty.min(makerOrder.getRemainingQty());
        BigDecimal fillPrice = makerOrder.getPrice();

        makerOrder.fill(fillQty, fillPrice);
        takerOrder.fill(fillQty, fillPrice);

        Trade trade = Trade.builder()
                .marketId(request.getMarketId())
                .makerOrderId(makerOrder.getId())
                .takerOrderId(takerOrder.getId())
                .buyerId(request.getSide() == OrderSide.BUY ? takerOrder.getUserId() : makerOrder.getUserId())
                .sellerId(request.getSide() == OrderSide.SELL ? takerOrder.getUserId() : makerOrder.getUserId())
                .price(fillPrice)
                .quantity(fillQty)
                .makerFee(BigDecimal.ZERO)
                .takerFee(BigDecimal.ZERO)
                .build();
        tradeRepository.save(trade);

        saveEvent(makerOrder, trade.getId(), EventType.TRADE_MATCHED, makerStatusBefore, makerOrder.getStatus(), fillQty, fillPrice);
        saveEvent(takerOrder, trade.getId(), EventType.TRADE_MATCHED, takerStatusBefore, takerOrder.getStatus(), fillQty, fillPrice);

        return new MatchResult(fillQty, fillPrice);
    }

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

