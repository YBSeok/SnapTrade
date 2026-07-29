package com.project.snaptrade.engine.service;

import com.project.snaptrade.common.kafka.KafkaTopics;
import com.project.snaptrade.engine.domain.OrderProjectionSnapshot;
import com.project.snaptrade.engine.repository.EventExecutionRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Consumes order.projection and UPSERTs the orders read model.
 * Replaces the previous in-memory BlockingQueue worker.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProjectionWorker {

    private final EventExecutionRepository eventExecutionRepository;
    private final MeterRegistry meterRegistry;

    private Timer projectionDbIoTimer;

    @PostConstruct
    void initMetrics() {
        projectionDbIoTimer = Timer.builder("latency.projection.db")
                .description("Projection DB UPDATE Latency (kafka consumer)")
                .tag("application", "snaptrade-engine")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .register(meterRegistry);
    }

    @KafkaListener(
            topics = KafkaTopics.ORDER_PROJECTION,
            groupId = "order-projection",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onProjection(OrderProjectionSnapshot snapshot) {
        try {
            projectionDbIoTimer.record(() ->
                    eventExecutionRepository.updateReadModels(List.of(snapshot))
            );
        } catch (Exception e) {
            log.error("OrderProjectionWorker failed for orderId={}", snapshot.id(), e);
            throw e;
        }
    }

    /**
     * Optional batch entry used if a batch listener factory is wired later.
     * Last-write-wins per orderId within the batch.
     */
    void applyBatch(List<OrderProjectionSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return;
        }
        Map<Long, OrderProjectionSnapshot> byOrderId = new HashMap<>();
        for (OrderProjectionSnapshot snapshot : snapshots) {
            byOrderId.put(snapshot.id(), snapshot);
        }
        List<OrderProjectionSnapshot> toWrite = new ArrayList<>(byOrderId.values());
        projectionDbIoTimer.record(() -> eventExecutionRepository.updateReadModels(toWrite));
    }
}
