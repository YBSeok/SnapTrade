package com.project.snaptrade.market.repository;

import com.project.snaptrade.market.domain.Kline;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KlineRepository extends JpaRepository<Kline, Long> {
}
