package com.project.snaptrade.common.websocket;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.TaskDecorator;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class QueueDelayTaskDecorator implements TaskDecorator {

    private final MeterRegistry meterRegistry;

    @Override
    public Runnable decorate(Runnable runnable) {
        long enqueueTime = System.currentTimeMillis();

        return () -> {
            long queueingDelay = System.currentTimeMillis() - enqueueTime;

            Timer.builder("websocket_outbound_queue_delay")
                    .description("WebSocket Outbound Queueing Delay")
                    .register(meterRegistry)
                    .record(queueingDelay, TimeUnit.MILLISECONDS);

            if (queueingDelay > 100) {
                log.warn("WebSocket Outbound Queue Delay: {} ms", queueingDelay);
            }

            runnable.run();
        };
    }
}
