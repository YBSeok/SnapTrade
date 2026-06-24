package com.project.snaptrade.market.repository;

import com.project.snaptrade.market.domain.Kline;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface KlineRepository extends JpaRepository<Kline, Long> {
    @Query(value = "SELECT close_price FROM kline " +
            "WHERE market_id = :marketId " +
            "AND interval_type = '1m' " +
            "AND open_time_ms <= :targetTimeMs " +
            "ORDER BY open_time_ms DESC LIMIT 1",
            nativeQuery = true)
    Optional<Long> findClosestOldClosePrice(@Param("marketId") Long marketId,
                                            @Param("targetTimeMs") long targetTimeMs);
}
