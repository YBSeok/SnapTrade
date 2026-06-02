package com.project.snaptrade.engine.repository;

import com.project.snaptrade.engine.domain.OrderEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderEventRepository extends JpaRepository<OrderEvent, Long> {
}
