package com.project.snaptrade.engine.repository;

import com.project.snaptrade.engine.domain.Order;
import com.project.snaptrade.engine.domain.Trade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface TradeRepository extends JpaRepository<Trade, Long> {

    @Query("SELECT MAX(t.sequenceNo) FROM Trade t")
    Long findMaxSequenceNo();
}
