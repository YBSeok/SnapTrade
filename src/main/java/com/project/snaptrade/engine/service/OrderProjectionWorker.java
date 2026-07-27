package com.project.snaptrade.engine.service;

import com.project.snaptrade.engine.domain.OrderProjectionSnapshot;
import com.project.snaptrade.engine.repository.EventExecutionRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * orders read-model Projection을 Disruptor 밖에서 처리한다.
 * 단일 워커 스레드 + 배치로 orderId 단위 last-write-wins를 유지한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProjectionWorker {

    private static final int BATCH_SIZE = 1000;

    private final EventExecutionRepository eventExecutionRepository;
    private final MeterRegistry meterRegistry;

    private final BlockingQueue<List<OrderProjectionSnapshot>> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    private Thread workerThread;
    private Timer projectionDbIoTimer;

    @PostConstruct
    void start() {
        projectionDbIoTimer = Timer.builder("latency.projection.db")
                .description("Projection DB UPDATE Latency (async worker)")
                .tag("application", "snaptrade-engine")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .register(meterRegistry);

        workerThread = new Thread(this::runLoop, "order-projection-worker");
        workerThread.setDaemon(true);
        workerThread.start();
        log.info("OrderProjectionWorker started");
    }

    @PreDestroy
    void stop() {
        running.set(false);
        if (workerThread != null) {
            workerThread.interrupt();
        }
    }

    public void enqueue(List<OrderProjectionSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return;
        }
        if (!queue.offer(List.copyOf(snapshots))) {
            log.error("OrderProjectionWorker queue rejected {} snapshots", snapshots.size());
        }
    }

    private void runLoop() {
        final List<List<OrderProjectionSnapshot>> buffer = new ArrayList<>(BATCH_SIZE);
        final Map<Long, OrderProjectionSnapshot> batchByOrderId = new HashMap<>(BATCH_SIZE);

        while (running.get()) {
            try {
                List<OrderProjectionSnapshot> first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }

                buffer.clear();
                batchByOrderId.clear();
                buffer.add(first);
                queue.drainTo(buffer, BATCH_SIZE - 1);

                for (List<OrderProjectionSnapshot> batch : buffer) {
                    for (OrderProjectionSnapshot snapshot : batch) {
                        batchByOrderId.put(snapshot.id(), snapshot);
                    }
                }

                if (!batchByOrderId.isEmpty()) {
                    List<OrderProjectionSnapshot> toWrite = new ArrayList<>(batchByOrderId.values());
                    projectionDbIoTimer.record(() -> eventExecutionRepository.updateReadModels(toWrite));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("OrderProjectionWorker failed", e);
            }
        }
    }
}
