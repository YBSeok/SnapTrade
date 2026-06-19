package com.project.snaptrade.engine.repository;

import com.project.snaptrade.engine.domain.Order;
import com.project.snaptrade.engine.domain.constant.OrderSide;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

public interface OrderRepository extends JpaRepository<Order, Long> {

    List<Order> findByStatusInOrderByCreatedAtAsc(List<OrderStatus> statuses);

    @Query("SELECT COALESCE(MAX(o.sequenceNo), 0) FROM Order o WHERE o.marketId = :marketId")
    Long findMaxSequenceNoByMarketId(@Param("marketId") Long marketId);

    // BUY 요청 시 가장 싼 SELL 호가 탐색
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Order> findTopByMarketIdAndSideAndPriceLessThanEqualAndStatusInOrderByPriceAscCreatedAtAsc(
            Long marketId, OrderSide side, BigDecimal price, List<OrderStatus> statuses);

    // SELL 요청 시 가장 비싼 BUY 호가 탐색
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Order> findTopByMarketIdAndSideAndPriceGreaterThanEqualAndStatusInOrderByPriceDescCreatedAtAsc(
            Long marketId, OrderSide side, BigDecimal price, List<OrderStatus> statuses);
}