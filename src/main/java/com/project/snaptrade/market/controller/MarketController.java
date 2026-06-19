package com.project.snaptrade.market.controller;

import com.project.snaptrade.market.dto.MarketRequestDto;
import com.project.snaptrade.market.dto.MarketResponseDto;
import com.project.snaptrade.market.service.MarketService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/markets")
@RequiredArgsConstructor
public class MarketController {
    private final MarketService marketService;

    @PostMapping
    public ResponseEntity<MarketResponseDto> create(@RequestBody MarketRequestDto request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(marketService.createMarket(request));
    }

    @GetMapping
    public ResponseEntity<List<MarketResponseDto>> findAll() {
        return ResponseEntity.ok(marketService.getAllMarkets());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        marketService.deleteMarket(id);
        return ResponseEntity.noContent().build();
    }
}
