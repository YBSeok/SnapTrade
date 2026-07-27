package com.project.snaptrade.engine.service;

import com.project.snaptrade.engine.Dto.OrderStatusResponse;
import com.project.snaptrade.engine.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class OrderQueryService {

    private final OrderRepository orderRepository;

    /**
     * Projection에 아직 없으면 empty.
     * 클라이언트는 empty(404)를 "아직 로딩"으로 보고 짧게 재폴링하면 된다.
     */
    @Transactional(readOnly = true)
    public Optional<OrderStatusResponse> findStatus(Long orderId) {
        return orderRepository.findById(orderId)
                .map(order -> new OrderStatusResponse(
                        order.getId(),
                        order.getStatus(),
                        order.getExecutedQty(),
                        order.getOrigQty()
                ));
    }
}
