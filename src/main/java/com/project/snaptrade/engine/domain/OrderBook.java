package com.project.snaptrade.engine.domain;

import com.project.snaptrade.engine.domain.constant.OrderSide;
import java.math.BigDecimal;
import java.util.*;

public class OrderBook {
    private final TreeMap<Long, Queue<Order>> asks = new TreeMap<>();
    private final TreeMap<Long, Queue<Order>> bids = new TreeMap<>(Comparator.reverseOrder());

    public void addOrder(Order order) {
        if (order.getSide() == OrderSide.SELL) {
            asks.computeIfAbsent(order.getPrice(), k -> new ArrayDeque<>()).add(order);
        } else {
            bids.computeIfAbsent(order.getPrice(), k -> new ArrayDeque<>()).add(order);
        }
    }

    public Queue<Order> getBestPriceQueue(OrderSide targetSide) {
        if (targetSide == OrderSide.SELL) {
            return asks.isEmpty() ? null : asks.firstEntry().getValue();
        } else {
            return bids.isEmpty() ? null : bids.firstEntry().getValue();
        }
    }

    public void removePriceLevel(OrderSide side, long price) {
        if (side == OrderSide.SELL) {
            asks.remove(price);
        } else {
            bids.remove(price);
        }
    }
}
