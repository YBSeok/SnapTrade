package com.project.snaptrade.market.repository;

import com.project.snaptrade.market.domain.Ticker;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TickerRepository extends JpaRepository<Ticker, Long> {
    Optional<Ticker> findByMarketId(Long marketId);
}
