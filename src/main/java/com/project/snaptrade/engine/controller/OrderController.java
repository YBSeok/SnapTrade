package com.project.snaptrade.engine.controller;

import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.service.MatchingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final MatchingService matchingService;

    @PostMapping
    public ResponseEntity<String> placeOrder(@RequestBody OrderRequestDto request) {
        matchingService.processOrder(request);
        return ResponseEntity.ok("Order processed successfully.");
    }
}
