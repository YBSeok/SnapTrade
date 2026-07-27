package com.project.snaptrade.engine.controller;

import com.project.snaptrade.common.response.CommonSuccessDto;
import com.project.snaptrade.engine.Dto.CancelOrderRequestDto;
import com.project.snaptrade.engine.Dto.OrderAcceptedResponse;
import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.Dto.OrderStatusResponse;
import com.project.snaptrade.engine.domain.Order;
import com.project.snaptrade.engine.domain.OrderTrace;
import com.project.snaptrade.engine.service.EventSourcedMatchingEngine;
import com.project.snaptrade.engine.service.OrderPreProcessor;
import com.project.snaptrade.engine.service.OrderQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderPreProcessor orderPreProcessor;
    private final EventSourcedMatchingEngine asyncMatchingEngine;
    private final OrderQueryService orderQueryService;

    @PostMapping
    public ResponseEntity<CommonSuccessDto<OrderAcceptedResponse>> placeOrder(@RequestBody OrderRequestDto request) {
        OrderTrace trace = new OrderTrace(request);

        Long orderId = orderPreProcessor.validateAndEnqueue(trace);

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(CommonSuccessDto.of(
                        new OrderAcceptedResponse(orderId),
                        HttpStatus.ACCEPTED,
                        "Order accepted for processing."
                ));
    }

    /**
     * Projection lag 동안 클라이언트가 짧게 폴링하는 경량 상태 API.
     * Projection에 아직 없으면 404 → 로딩 유지 후 재폴링.
     */
    @GetMapping("/{orderId}/status")
    public ResponseEntity<CommonSuccessDto<OrderStatusResponse>> getOrderStatus(@PathVariable Long orderId) {
        return orderQueryService.findStatus(orderId)
                .map(status -> ResponseEntity.ok(CommonSuccessDto.ok(status)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{orderId}")
    public ResponseEntity<CommonSuccessDto<Void>> cancelOrder(
            @PathVariable Long orderId,
            @RequestBody CancelOrderRequestDto request) {

        Order targetOrder = Order.builder()
                .id(orderId)
                .userId(request.getUserId())
                .marketId(request.getMarketId())
                .side(request.getSide())
                .price(request.getPrice())
                .build();

        OrderTrace trace = new OrderTrace(null);
        trace.setOrder(targetOrder);
        asyncMatchingEngine.cancelOrder(trace);

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(CommonSuccessDto.of(null, HttpStatus.ACCEPTED, "Cancel request accepted for processing."));
    }
}
