package com.project.snaptrade.engine.controller;

import com.project.snaptrade.common.response.CommonSuccessDto;
import com.project.snaptrade.engine.Dto.CancelOrderRequestDto;
import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.domain.Order;
import com.project.snaptrade.engine.domain.OrderTrace;
import com.project.snaptrade.engine.service.EventSourcedMatchingEngine;
import com.project.snaptrade.engine.service.OrderPreProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderPreProcessor orderPreProcessor;
    private final EventSourcedMatchingEngine asyncMatchingEngine;

    @PostMapping
    public ResponseEntity<CommonSuccessDto<Void>> placeOrder(@RequestBody OrderRequestDto request) {
        OrderTrace trace = new OrderTrace(request);

        orderPreProcessor.validateAndEnqueue(trace);

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(CommonSuccessDto.of(null, HttpStatus.ACCEPTED, "Order accepted for processing."));
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

