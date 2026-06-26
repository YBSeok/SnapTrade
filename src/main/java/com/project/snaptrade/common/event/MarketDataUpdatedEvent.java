package com.project.snaptrade.common.event;

public record MarketDataUpdatedEvent(
        String topic,       // 예: "MARKET_DATA"
        String symbol,      // 예: "BTC/KRW"
        Object payload      // 호가창 데이터 또는 체결 내역 객체
) {}
