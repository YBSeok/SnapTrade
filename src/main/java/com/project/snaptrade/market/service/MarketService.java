package com.project.snaptrade.market.service;

import com.project.snaptrade.engine.service.MarketMetadataCache;
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
    private final MarketMetadataCache marketCache;

    @Transactional
    public MarketResponseDto createMarket(MarketRequestDto dto) {
        Market market = dto.toEntity();
        Market savedMarket = marketRepository.save(market);
        marketCache.refreshCache();
        return MarketResponseDto.from(savedMarket);
    }

    public List<MarketResponseDto> getAllMarkets() {
        return marketRepository.findAll().stream()
                .map(MarketResponseDto::from)
                .toList();
    }

    @Transactional
    public void deleteMarket(Long id) {
        marketRepository.deleteById(id);
        marketCache.refreshCache();
    }
}
