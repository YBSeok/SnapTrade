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

    public boolean removeOrder(Order order) {
        TreeMap<Long, Queue<Order>> targetBook = (order.getSide() == OrderSide.SELL) ? asks : bids;
        long price = order.getPrice();

        Queue<Order> queue = targetBook.get(price);
        if (queue == null || queue.isEmpty()) {
            return false;
        }

        boolean removed = queue.remove(order);

        if (removed && queue.isEmpty()) {
            targetBook.remove(price);
        }

        return removed;
    }

    public boolean hasEnoughLiquidity(OrderSide opponentSide, long requiredQty, long limitPrice, boolean isMarketOrder) {
        // 상대편 트리를 가져옵니다. (asks는 오름차순, bids는 내림차순으로 이미 정렬되어 있음)
        TreeMap<Long, Queue<Order>> targetBook = (opponentSide == OrderSide.SELL) ? asks : bids;
        long accumulatedQty = 0L;

        // 트리의 최우선 호가부터 순차적으로 탐색
        for (Map.Entry<Long, Queue<Order>> entry : targetBook.entrySet()) {
            long priceLevel = entry.getKey();

            // 지정가 주문일 경우, 가격 조건이 불리해지는 노드에 도달하면 탐색을 즉시 중단합니다.
            if (!isMarketOrder) {
                // 상대편이 매도(SELL) 호가인데, 내가 원하는 매수 지정가보다 비싸면 중단
                if (opponentSide == OrderSide.SELL && priceLevel > limitPrice) break;
                // 상대편이 매수(BUY) 호가인데, 내가 원하는 매도 지정가보다 싸면 중단
                if (opponentSide == OrderSide.BUY && priceLevel < limitPrice) break;
            }

            // 해당 가격대의 큐에 있는 주문들의 잔여 물량을 누적합니다.
            Queue<Order> queue = entry.getValue();
            for (Order order : queue) {
                accumulatedQty += order.getRemainingQty();
                if (accumulatedQty >= requiredQty) {
                    return true;
                }
            }
        }

        return false; // 끝까지 탐색했으나 요구 수량을 채우지 못함
    }
}
