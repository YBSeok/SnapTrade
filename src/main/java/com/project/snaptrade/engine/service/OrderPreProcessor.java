package com.project.snaptrade.engine.service;

import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.domain.OrderTrace;
import com.project.snaptrade.engine.domain.constant.OrderType;
import com.project.snaptrade.engine.repository.InMemoryBalanceRepository;
import com.project.snaptrade.engine.repository.InMemoryIdempotencyRepository;
import com.project.snaptrade.market.domain.MarketSpec;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderPreProcessor {

    private final InMemoryIdempotencyRepository idempotencyRepository;
    private final InMemoryBalanceRepository balanceRepository;
    private final EventSourcedMatchingEngine matchingEngine;

    private final MarketMetadataCache marketCache;

    public void validateAndEnqueue(OrderTrace trace) {
        OrderRequestDto request = trace.getRequestDto();

        // 멱등성 검증
        if (!idempotencyRepository.setNx(request.getClientOrderId(), 60L)) {
            throw new IllegalArgumentException("중복된 주문 요청입니다. ClientOrderId: " + request.getClientOrderId());
        }

        MarketSpec spec = marketCache.getSpec(request.getMarketId());
        // 호가 단위 검증
        if (request.getOrderType() == OrderType.LIMIT && request.getPrice() % spec.tickSize() != 0) {
            throw new IllegalArgumentException(
                    String.format("호가 단위 규칙 위반입니다. (마켓: %s, 단위: %d)", spec.symbol(), spec.tickSize())
            );
        }
        // 최소 주문 수량 검증
        if (request.getQuantity() < spec.minQty()) {
            throw new IllegalArgumentException(
                    String.format("최소 주문 수량 미달입니다. (최소: %d)", spec.minQty())
            );
        }
        // stepSize (수량 증분 단위) 검증
        if (request.getQuantity() % spec.stepSize() != 0) {
            throw new IllegalArgumentException(
                    String.format("주문 수량 단위 규칙 위반입니다. (단위: %d)", spec.stepSize())
            );
        }
        // 최소 명목 금액 검증 (Min Notional)
        if (request.getOrderType() == OrderType.LIMIT) {
            long notional = request.getPrice() * (request.getQuantity() / 100000000L);
            if (notional < spec.minNotional()) {
                throw new IllegalArgumentException(
                        String.format("최소 주문 금액 미달입니다. (최소: %d)", spec.minNotional())
                );
            }
        }
        // 사전 잔고 동결 (Pre-Trade Hold) 및 가용 잔고 검증
        boolean holdSuccess = balanceRepository.tryPreTradeHold(
                request.getUserId(),
                request.getMarketId(),
                request.getSide(),
                request.getPrice(),
                request.getQuantity()
        );
        if (!holdSuccess) {
            throw new IllegalStateException("가용 잔고가 부족합니다.");
        }

        matchingEngine.placeOrder(trace);
    }
}
