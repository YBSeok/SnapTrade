package com.project.snaptrade.engine.service;

import com.project.snaptrade.market.domain.Market;
import com.project.snaptrade.market.domain.MarketSpec;
import com.project.snaptrade.market.domain.MarketStatus;
import com.project.snaptrade.market.repository.MarketRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class MarketMetadataCache {

    private final MarketRepository marketRepository;
    private final Map<Long, MarketSpec> specCache = new ConcurrentHashMap<>();

    @EventListener(ApplicationReadyEvent.class)
    public void warmUpCache() {
        loadMetadataFromDb();
    }

    public void refreshCache() {
        log.info("Admin triggered Market Metadata Cache refresh.");
        loadMetadataFromDb();
    }

    private void loadMetadataFromDb() {
        log.info("Loading active markets from Database...");
        List<Market> activeMarkets = marketRepository.findByStatus(MarketStatus.ACTIVE);

        if (activeMarkets.isEmpty()) {
            log.warn("No active markets found in the database!");
            return;
        }

        for (Market market : activeMarkets) {
            MarketSpec spec = new MarketSpec(
                    market.getId(),
                    market.getSymbol(),
                    market.getMinPrice(),
                    market.getMaxPrice(),
                    market.getTickSize(),
                    market.getMinQty(),
                    market.getMaxQty(),
                    market.getStepSize(),
                    market.getMinNotional(),
                    market.getMakerFeeRate(),
                    market.getTakerFeeRate()
            );

            specCache.put(market.getId(), spec);
            log.debug("Loaded Market Spec: {}", spec);
        }

        log.info("Market Metadata Cache initialized. Loaded {} active markets.", specCache.size());
    }

    public MarketSpec getSpec(Long marketId) {
        MarketSpec spec = specCache.get(marketId);
        if (spec == null) {
            throw new IllegalArgumentException("지원하지 않거나 현재 비활성화된 마켓입니다. Market ID: " + marketId);
        }
        return spec;
    }
}
