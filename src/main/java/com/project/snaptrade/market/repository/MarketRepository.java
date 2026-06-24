package com.project.snaptrade.market.repository;

import com.project.snaptrade.market.domain.Market;
import com.project.snaptrade.market.domain.MarketStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface MarketRepository extends JpaRepository<Market, Long> {
    List<Market> findByStatus(MarketStatus status);
}
