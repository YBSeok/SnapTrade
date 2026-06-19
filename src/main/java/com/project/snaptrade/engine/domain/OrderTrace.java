package com.project.snaptrade.engine.domain;

import com.project.snaptrade.engine.Dto.OrderRequestDto;
import lombok.Getter;
import lombok.Setter;

@Getter
public class OrderTrace {
    private final OrderRequestDto requestDto;

    @Setter
    private Order order;

    private final long ingressTs;
    private long engineEnterTs;
    private long matchStartTs;
    private long matchEndTs;
    private long persistEnterTs;
    private long dbWriteStartTs;
    private long dbWriteEndTs;

    public OrderTrace(OrderRequestDto requestDto) {
        this.requestDto = requestDto;
        this.ingressTs = System.nanoTime();
    }

    public void markEngineEnter() { this.engineEnterTs = System.nanoTime(); }
    public void markMatchStart() { this.matchStartTs = System.nanoTime(); }
    public void markMatchEnd() { this.matchEndTs = System.nanoTime(); }
    public void markPersistEnter() { this.persistEnterTs = System.nanoTime(); }
    public void markDbWriteStart() { this.dbWriteStartTs = System.nanoTime(); }
    public void markDbWriteEnd() { this.dbWriteEndTs = System.nanoTime(); }
}
