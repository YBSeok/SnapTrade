package com.project.snaptrade.engine.service;

import com.github.f4b6a3.tsid.TsidCreator;
import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.domain.*;
import com.project.snaptrade.engine.domain.constant.EventType;
import com.project.snaptrade.engine.domain.constant.OrderSide;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import com.project.snaptrade.engine.repository.ExecutionRepository;
import com.project.snaptrade.engine.repository.OrderRepository;
import com.project.snaptrade.engine.repository.TradeRepository;
import com.project.snaptrade.market.domain.Market;
import com.project.snaptrade.market.repository.MarketRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class AsyncMatchingEngine {
    private static final Logger log = LoggerFactory.getLogger(AsyncMatchingEngine.class);
    private record MatchPayload(
            Trade trade,
            Order makerOrder,
            Order takerOrder,
            List<OrderEvent> events,
            List<OrderTrace> traces
    ) {}

    private static final List<OrderStatus> ACTIVE_STATUSES = List.of(OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED);

    private final BlockingQueue<OrderTrace> orderQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<MatchPayload> persistenceQueue = new LinkedBlockingQueue<>();

    private final OrderRepository orderRepository;
    private final MarketRepository marketRepository;
    private final ExecutionRepository executionRepository;
    private final TradeRepository tradeRepository;

    private final ConcurrentHashMap<Long, AtomicLong> sequenceGenerators = new ConcurrentHashMap<>();
    private final AtomicLong tradeSequenceGenerator = new AtomicLong(0L);
    private final ConcurrentHashMap<Long, OrderBook> orderBooks = new ConcurrentHashMap<>();

    private final MeterRegistry meterRegistry;

    private Timer gatewayTimer;
    private Timer engineQueueTimer;
    private Timer coreMatchingTimer;
    private Timer persistenceQueueTimer;
    private Timer dbIoTimer;

    private void preWarmSequences() {
        List<Market> markets = marketRepository.findAll();
        for (Market market : markets) {
            Long maxSeq = orderRepository.findMaxSequenceNoByMarketId(market.getId());
            sequenceGenerators.put(market.getId(), new AtomicLong(maxSeq != null ? maxSeq : 0L));
        }

        Long maxTradeSeq = tradeRepository.findMaxSequenceNo();
        tradeSequenceGenerator.set(maxTradeSeq != null ? maxTradeSeq : 0L);
    }

    private Timer registerTimer(String name, String description) {
        return Timer.builder(name)
                .description(description)
                .tag("application", "snaptrade-engine")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("System initializing: Pre-warming markets...");
        preWarmSequences();

        Gauge.builder("engine.persistence.queue.size", persistenceQueue, BlockingQueue::size)
                .register(meterRegistry);

        this.gatewayTimer         = registerTimer("latency.gateway", "HTTP ingress to engine enter");
        this.engineQueueTimer = registerTimer("latency.engine.queue", "...");
        this.coreMatchingTimer = registerTimer("latency.core.matching", "...");
        this.persistenceQueueTimer = registerTimer("latency.persistence.queue", "...");
        this.dbIoTimer = registerTimer("engine.persistence.db.latency", "...");

        recoverOrderBookState();
        startMatchingThread();
        startPersistenceWorker();
    }

    private void recoverOrderBookState() {
        List<Order> openOrders = orderRepository.findByStatusInOrderByCreatedAtAsc(ACTIVE_STATUSES);

        for (Order order : openOrders) {
            Long marketId = order.getMarketId();

            OrderBook orderBook = orderBooks.computeIfAbsent(marketId, id -> new OrderBook());
            orderBook.addOrder(order);
        }
    }

    public void placeOrder(OrderTrace trace) {
        trace.markEngineEnter();
        gatewayTimer.record(Duration.ofNanos(trace.getEngineEnterTs() - trace.getIngressTs()));
        OrderRequestDto request = trace.getRequestDto();
        Long marketId = request.getMarketId();

        Order newOrder = Order.builder()
                .id(TsidCreator.getTsid().toLong())
                .userId(request.getUserId())
                .marketId(marketId)
                .side(request.getSide())
                .orderType(request.getOrderType())
                .timeInForce(request.getTimeInForce())
                .price(request.getPrice())
                .origQty(request.getQuantity())
                .build();

        AtomicLong seqGen = sequenceGenerators.get(marketId);
        if (seqGen == null) {
            throw new IllegalStateException("Market ID " + marketId + " not pre-warmed.");
        }
        newOrder.assignSequenceNo(seqGen.incrementAndGet());

        // 4. Trace에 엔티티 주입
        trace.setOrder(newOrder);

        // 5. 큐 전달 (Trace 객체를 큐에 넣음으로써 전체 라이프사이클 추적 가능)
        orderQueue.offer(trace);
    }

    // ==========================================
    // 단일 스레드 매칭 루프
    // ==========================================
    private void startMatchingThread() {
        new Thread(() -> {
            while (true) {
                try {
                    OrderTrace trace = orderQueue.take();

                    engineQueueTimer.record(Duration.ofNanos(System.nanoTime() - trace.getEngineEnterTs()));

                    trace.markMatchStart();
                    processMatch(trace);
                    trace.markMatchEnd();

                    coreMatchingTimer.record(Duration.ofNanos(trace.getMatchEndTs() - trace.getMatchStartTs()));

                } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
        }, "Engine-Core-Thread").start();
    }

    private void processMatch(OrderTrace takerTrace) {
        Order takerOrder = takerTrace.getOrder();

        Long marketId = takerOrder.getMarketId();
        OrderBook orderBook = orderBooks.computeIfAbsent(marketId, id -> new OrderBook());

        long remainingQty = takerOrder.getOrigQty();
        OrderSide opponentSide = takerOrder.getSide() == OrderSide.BUY ? OrderSide.SELL : OrderSide.BUY;

        List<OrderEvent> events = new ArrayList<>();

        takerTrace.markPersistEnter();

        if (takerOrder.getExecutedQty() == 0L) {
            events.add(createOrderEvent(takerOrder, null, EventType.ORDER_PLACED, null, OrderStatus.NEW, 0L, 0L));
        }

        while (remainingQty > 0L) {
            Queue<Order> bestPriceQueue = orderBook.getBestPriceQueue(opponentSide);
            if (bestPriceQueue == null || bestPriceQueue.isEmpty()) break;

            Order makerOrder = bestPriceQueue.peek();

            if (takerOrder.getSide() == OrderSide.BUY && takerOrder.getPrice() < makerOrder.getPrice()) break;
            if (takerOrder.getSide() == OrderSide.SELL && takerOrder.getPrice() > makerOrder.getPrice()) break;

            long fillQty = Math.min(remainingQty, makerOrder.getRemainingQty());
            long fillPrice = makerOrder.getPrice();

            OrderStatus makerStatusBefore = makerOrder.getStatus();
            OrderStatus takerStatusBefore = takerOrder.getStatus();

            makerOrder.fill(fillQty, fillPrice);
            takerOrder.fill(fillQty, fillPrice);
            remainingQty -= fillQty;

            if (makerOrder.getRemainingQty() == 0L) {
                bestPriceQueue.poll();
                if (bestPriceQueue.isEmpty()) orderBook.removePriceLevel(opponentSide, makerOrder.getPrice());
            }

            Long buyerId = takerOrder.getSide() == OrderSide.BUY ? takerOrder.getUserId() : makerOrder.getUserId();
            Long sellerId = takerOrder.getSide() == OrderSide.SELL ? takerOrder.getUserId() : makerOrder.getUserId();

            long quoteQuantity = fillPrice * fillQty; // 도메인 배수 검토 필요

            Trade trade = Trade.builder()
                    .id(TsidCreator.getTsid().toLong())
                    .marketId(marketId)
                    .makerOrderId(makerOrder.getId())
                    .takerOrderId(takerOrder.getId())
                    .buyerId(buyerId)
                    .sellerId(sellerId)
                    .price(fillPrice)
                    .quantity(fillQty)
                    .quoteQuantity(quoteQuantity)
                    .makerFee(0L)
                    .takerFee(0L)
                    .sequenceNo(tradeSequenceGenerator.incrementAndGet())
                    .build();

            events.add(createOrderEvent(makerOrder, trade.getId(), EventType.TRADE_MATCHED, makerStatusBefore, makerOrder.getStatus(), fillQty, fillPrice));
            events.add(createOrderEvent(takerOrder, trade.getId(), EventType.TRADE_MATCHED, takerStatusBefore, takerOrder.getStatus(), fillQty, fillPrice));

            persistenceQueue.offer(new MatchPayload(
                    trade, makerOrder, takerOrder,
                    new ArrayList<>(events),
                    List.of(takerTrace)
            ));
            events.clear();
        }

        if (remainingQty > 0L) {
            orderBook.addOrder(takerOrder);
            persistenceQueue.offer(new MatchPayload(null, null, takerOrder, events, List.of(takerTrace)));
        }
    }

    private OrderEvent createOrderEvent(Order order, Long tradeId, EventType eventType, OrderStatus statusBefore, OrderStatus statusAfter, long fillQty, long fillPrice) {
        String payload;
        if (eventType == EventType.ORDER_PLACED) {
            payload = "{\"marketId\":" + order.getMarketId() +
                    ",\"userId\":" + order.getUserId() +
                    ",\"side\":\"" + order.getSide().name() + "\"" +
                    ",\"price\":\"" + order.getPrice() + "\"" +
                    ",\"origQty\":\"" + order.getOrigQty() + "\"}";
        } else {
            payload = "{\"marketId\":" + order.getMarketId() + "}";
        }

        return OrderEvent.builder()
                .id(TsidCreator.getTsid().toLong())
                .orderId(order.getId())
                .tradeId(tradeId)
                .eventType(eventType)
                .statusBefore(statusBefore)
                .statusAfter(statusAfter)
                .fillQty(fillQty)
                .fillPrice(fillPrice)
                .payload(payload)
                .build();
    }

    // ==========================================
    // 비동기 DB 저장 워커 스레드
    // ==========================================
    private void startPersistenceWorker() {
        new Thread(() -> {
            final int BATCH_SIZE = 1000;

            final List<MatchPayload> buffer = new ArrayList<>(BATCH_SIZE);
            final List<Trade> trades = new ArrayList<>(BATCH_SIZE);
            final Map<Long, Order> orderMap = new HashMap<>(BATCH_SIZE);
            final List<OrderEvent> events = new ArrayList<>(BATCH_SIZE * 2);

            while (true) {
                try {
                    MatchPayload firstPayload = persistenceQueue.take();
                    if (!firstPayload.traces().isEmpty()) {
                        persistenceQueueTimer.record(Duration.ofNanos(System.nanoTime() - firstPayload.traces().get(0).getPersistEnterTs()));
                    }

                    buffer.add(firstPayload);
                    persistenceQueue.drainTo(buffer, BATCH_SIZE - 1);

                    trades.clear();
                    orderMap.clear();
                    events.clear();

                    for (MatchPayload payload : buffer) {
                        if (payload.trade() != null) trades.add(payload.trade());
                        if (payload.makerOrder() != null) orderMap.put(payload.makerOrder().getId(), payload.makerOrder());
                        if (payload.takerOrder() != null) orderMap.put(payload.takerOrder().getId(), payload.takerOrder());
                        if (payload.events() != null) events.addAll(payload.events());

                        if (!payload.traces().isEmpty()) {
                            payload.traces().get(0).markDbWriteStart();
                        }
                    }

                    List<Order> orders = new ArrayList<>(orderMap.values());
                    dbIoTimer.record(() -> executionRepository.saveExecutions(trades, orders, events));

                    for (MatchPayload payload : buffer) {
                        if (!payload.traces().isEmpty()) {
                            payload.traces().get(0).markDbWriteEnd();
                        }
                    }

                    buffer.clear();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Persistence worker encountered an error", e);
                }
            }
        }, "Persistence-Worker-Thread").start();
    }
}
