package com.project.snaptrade.common.kafka;

import com.project.snaptrade.engine.domain.Trade;

/**
 * Kafka-friendly trade payload. Avoids publishing the JPA {@link Trade} entity directly.
 */
public record TradeCompletedMessage(
        long id,
        long marketId,
        long makerOrderId,
        long takerOrderId,
        long makerUserId,
        long takerUserId,
        long buyerId,
        long sellerId,
        long price,
        long quantity,
        long quoteQuantity,
        long makerFee,
        long takerFee,
        Long sequenceNo
) {
    public static TradeCompletedMessage from(Trade trade) {
        return new TradeCompletedMessage(
                trade.getId(),
                trade.getMarketId(),
                trade.getMakerOrderId(),
                trade.getTakerOrderId(),
                trade.getMakerUserId(),
                trade.getTakerUserId(),
                trade.getBuyerId(),
                trade.getSellerId(),
                trade.getPrice(),
                trade.getQuantity(),
                trade.getQuoteQuantity(),
                trade.getMakerFee(),
                trade.getTakerFee(),
                trade.getSequenceNo()
        );
    }

    public Trade toTrade() {
        return Trade.builder()
                .id(id)
                .marketId(marketId)
                .makerOrderId(makerOrderId)
                .takerOrderId(takerOrderId)
                .makerUserId(makerUserId)
                .takerUserId(takerUserId)
                .buyerId(buyerId)
                .sellerId(sellerId)
                .price(price)
                .quantity(quantity)
                .quoteQuantity(quoteQuantity)
                .makerFee(makerFee)
                .takerFee(takerFee)
                .sequenceNo(sequenceNo)
                .build();
    }
}
