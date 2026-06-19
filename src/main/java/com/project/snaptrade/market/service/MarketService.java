package com.project.snaptrade.market.service;

import com.project.snaptrade.market.domain.Market;
import com.project.snaptrade.market.dto.MarketRequestDto;
import com.project.snaptrade.market.dto.MarketResponseDto;
import com.project.snaptrade.market.repository.MarketRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class MarketService {
    private final MarketRepository marketRepository;

    @Transactional
    public MarketResponseDto createMarket(MarketRequestDto dto) {
        Market market = Market.builder()
                .symbol(dto.symbol())
                .minNotional(dto.minNotional())
                .status(dto.status())
                .baseAsset(dto.baseAsset())
                .quoteAsset(dto.quoteAsset())
                .minPrice(dto.minPrice())
                .tickSize(dto.tickSize())
                .minQty(dto.minQty())
                .stepSize(dto.stepSize())
                .build();
        return MarketResponseDto.from(marketRepository.save(market));
    }

    public List<MarketResponseDto> getAllMarkets() {
        return marketRepository.findAll().stream()
                .map(MarketResponseDto::from)
                .toList();
    }

    @Transactional
    public void deleteMarket(Long id) {
        marketRepository.deleteById(id);
    }
}
