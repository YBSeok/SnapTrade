package com.project.snaptrade.engine.repository;

import com.project.snaptrade.engine.domain.Order;
import com.project.snaptrade.engine.domain.Trade;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TradeRepository extends JpaRepository<Trade, Long> {
}
