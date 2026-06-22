package com.project.snaptrade.engine.controller;

import com.project.snaptrade.common.response.CommonSuccessDto;
import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.domain.OrderTrace;
import com.project.snaptrade.engine.service.EventSourcedMatchingEngine;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final EventSourcedMatchingEngine asyncMatchingEngine;

    @PostMapping
    public ResponseEntity<CommonSuccessDto<Void>> placeOrder(@RequestBody OrderRequestDto request) {
        OrderTrace trace = new OrderTrace(request);
        asyncMatchingEngine.placeOrder(trace);

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(CommonSuccessDto.of(null, HttpStatus.ACCEPTED, "Order accepted for processing."));
    }
}
