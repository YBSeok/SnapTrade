package com.project.snaptrade.market.repository;

import com.project.snaptrade.market.domain.Market;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface MarketRepository extends JpaRepository<Market, Long> {
    Optional<Market> findBySymbol(String symbol);
}
