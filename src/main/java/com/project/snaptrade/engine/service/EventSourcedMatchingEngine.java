package com.project.snaptrade.engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.f4b6a3.tsid.TsidCreator;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.domain.*;
import com.project.snaptrade.engine.domain.constant.EventType;
import com.project.snaptrade.engine.domain.constant.OrderSide;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import com.project.snaptrade.engine.repository.EventExecutionRepository;
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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class EventSourcedMatchingEngine {
    private static final Logger log = LoggerFactory.getLogger(EventSourcedMatchingEngine.class);
    private record MatchPayload(
            Trade trade,
            Order makerOrder,
            Order takerOrder,
            List<OrderEvent> events,
            List<OrderTrace> traces,
            long projectionEnterTs
    ) {
        public MatchPayload(Trade trade, Order makerOrder, Order takerOrder, List<OrderEvent> events, List<OrderTrace> traces) {
            this(trade, makerOrder, takerOrder, events, traces, System.nanoTime());
        }
    }
    public static class EngineEvent {
        public OrderTrace trace;
        public final List<Trade> trades = new ArrayList<>(16);
        public final List<OrderEvent> orderEvents = new ArrayList<>(32);
        public final List<Order> modifiedOrders = new ArrayList<>(16);
        public long matchEndTs;
        public long journalEndTs;

        public void clear() {
            trace = null;
            trades.clear();
            orderEvents.clear();
            modifiedOrders.clear();
            matchEndTs = 0L;
            journalEndTs = 0L;
        }
    }

    private final ObjectMapper objectMapper;

    private final BlockingQueue<OrderTrace> orderQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<MatchPayload> journalQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<MatchPayload> projectionQueue = new LinkedBlockingQueue<>();

    private Disruptor<EngineEvent> disruptor;
    private RingBuffer<EngineEvent> ringBuffer;

    private final OrderRepository orderRepository;
    private final MarketRepository marketRepository;
    private final EventExecutionRepository eventExecutionRepository;
    private final TradeRepository tradeRepository;

    private final ConcurrentHashMap<Long, AtomicLong> sequenceGenerators = new ConcurrentHashMap<>();
    private final AtomicLong tradeSequenceGenerator = new AtomicLong(0L);
    private final ConcurrentHashMap<Long, OrderBook> orderBooks = new ConcurrentHashMap<>();

    private final MeterRegistry meterRegistry;

    private Timer gatewayTimer;
    private Timer engineQueueTimer;
    private Timer coreMatchingTimer;
    private Timer journalDbIoTimer;
    private Timer projectionDbIoTimer;

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
        recoverOrderBookState();

        this.gatewayTimer = registerTimer("latency.gateway", "HTTP ingress to engine enter");
        this.engineQueueTimer = registerTimer("latency.engine.queue", "RingBuffer pending latency");
        this.coreMatchingTimer = registerTimer("latency.core.matching", "Pure matching execution time");
        this.journalDbIoTimer = registerTimer("latency.journal.db", "Journal DB INSERT Latency");
        this.projectionDbIoTimer = registerTimer("latency.projection.db", "Projection DB UPDATE Latency");

        initDisruptor();

        Gauge.builder("engine.ringbuffer.remaining", ringBuffer, RingBuffer::remainingCapacity)
                .description("Remaining capacity of LMAX Disruptor RingBuffer")
                .tag("application", "snaptrade-engine")
                .register(meterRegistry);
    }

    private void recoverOrderBookState() {
        log.info("System initializing: Replaying events to reconstruct OrderBooks...");
        long startTime = System.nanoTime();

        // 1. 모든 이벤트를 시간순(TSID 생성순)으로 조회
        // TSID는 시간 정렬을 보장하므로 ORDER BY id ASC를 사용하면 완벽한 시계열 재현이 가능합니다.
        List<OrderEvent> allEvents = eventExecutionRepository.findAllEventsOrderByIdAsc();

        // 임시 상태 저장소 (Replay 중인 주문들)
        Map<Long, Order> stateMap = new HashMap<>();

        // 2. 이벤트 스트림 순차 재수행 (Replay)
        for (OrderEvent event : allEvents) {
            Long orderId = event.getOrderId();

            switch (event.getEventType()) {
                case ORDER_PLACED:
                    // payload를 파싱하여 초기 Order 객체 재구성
                    Order newOrder = reconstructOrderFromEvent(orderId, event);
                    stateMap.put(orderId, newOrder);
                    break;

                case TRADE_MATCHED:
                    Order existingOrder = stateMap.get(orderId);
                    if (existingOrder != null) {
                        // 기존 상태에 체결량과 가격을 적용하여 상태 전진(Forwarding)
                        existingOrder.fill(event.getFillQty(), event.getFillPrice());

                        // 완전히 체결되거나 취소된 주문은 호가창에 올릴 필요가 없으므로 메모리에서 제거
                        if (existingOrder.getStatus() == OrderStatus.FILLED || existingOrder.getStatus() == OrderStatus.CANCELED) {
                            stateMap.remove(orderId);
                        }
                    }
                    break;

                case ORDER_CANCELED: // 취소 이벤트가 존재할 경우
                    stateMap.remove(orderId);
                    break;
            }
        }

        // 3. 재수행 완료 후, stateMap에 남아있는 데이터가 곧 현재 '활성화된 주문(Active Orders)'임
        for (Order activeOrder : stateMap.values()) {
            Long marketId = activeOrder.getMarketId();
            OrderBook orderBook = orderBooks.computeIfAbsent(marketId, id -> new OrderBook());
            orderBook.addOrder(activeOrder);
        }

        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        log.info("OrderBook reconstruction complete. Replayed {} events. Active orders loaded: {}. Took {} ms",
                allEvents.size(), stateMap.size(), durationMs);
    }

    private Order reconstructOrderFromEvent(Long orderId, OrderEvent event) {
        try {
            JsonNode payload = objectMapper.readTree(event.getPayload());

            return Order.reconstructForReplay(
                    orderId,
                    payload.get("userId").asLong(),
                    payload.get("marketId").asLong(),
                    OrderSide.valueOf(payload.get("side").asText()),
                    payload.get("price").asLong(),
                    payload.get("origQty").asLong()
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse order event payload for replay", e);
        }
    }

    private void initDisruptor() {
        int bufferSize = 65536;

        disruptor = new Disruptor<>(
                EngineEvent::new,
                bufferSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        disruptor.handleEventsWith(new MatchingEventHandler())
                .then(new JournalEventHandler())
                .then(new ProjectionEventHandler());

        ringBuffer = disruptor.start();
        log.info("LMAX Disruptor Pipeline started successfully.");
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
        if (seqGen == null) throw new IllegalStateException("Market ID " + marketId + " not pre-warmed.");
        newOrder.assignSequenceNo(seqGen.incrementAndGet());

        trace.setOrder(newOrder);

        ringBuffer.publishEvent((event, sequence) -> {
            event.clear();
            event.trace = trace;
        });
    }

    // ==========================================
    // 1. Matching Worker
    // ==========================================
//    private void startMatchingThread() {
//        new Thread(() -> {
//            while (true) {
//                try {
//                    OrderTrace trace = orderQueue.take();
//                    engineQueueTimer.record(Duration.ofNanos(System.nanoTime() - trace.getEngineEnterTs()));
//
//                    trace.markMatchStart();
//                    processMatch(trace);
//                    trace.markMatchEnd();
//
//                    coreMatchingTimer.record(Duration.ofNanos(trace.getMatchEndTs() - trace.getMatchStartTs()));
//                } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
//            }
//        }, "Engine-Core-Thread").start();
//    }

    private class MatchingEventHandler implements EventHandler<EngineEvent> {
        @Override
        public void onEvent(EngineEvent event, long sequence, boolean endOfBatch) {
            OrderTrace trace = event.trace;
            engineQueueTimer.record(Duration.ofNanos(System.nanoTime() - trace.getEngineEnterTs()));

            long matchStart = System.nanoTime();
            processMatch(event);
            coreMatchingTimer.record(Duration.ofNanos(System.nanoTime() - matchStart));

            event.matchEndTs = System.nanoTime();
        }
    }

    private void processMatch(EngineEvent event) {
        Order takerOrder = event.trace.getOrder();
        Long marketId = takerOrder.getMarketId();
        OrderBook orderBook = orderBooks.computeIfAbsent(marketId, id -> new OrderBook());

        long remainingQty = takerOrder.getOrigQty();
        OrderSide opponentSide = takerOrder.getSide() == OrderSide.BUY ? OrderSide.SELL : OrderSide.BUY;

        if (takerOrder.getExecutedQty() == 0L) {
            event.orderEvents.add(createOrderEvent(takerOrder, null, EventType.ORDER_PLACED, null, OrderStatus.NEW, 0L, 0L));
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

            event.modifiedOrders.add(makerOrder);

            Long buyerId = takerOrder.getSide() == OrderSide.BUY ? takerOrder.getUserId() : makerOrder.getUserId();
            Long sellerId = takerOrder.getSide() == OrderSide.SELL ? takerOrder.getUserId() : makerOrder.getUserId();

            Trade trade = Trade.builder()
                    .id(TsidCreator.getTsid().toLong())
                    .marketId(marketId)
                    .makerOrderId(makerOrder.getId())
                    .takerOrderId(takerOrder.getId())
                    .buyerId(buyerId)
                    .sellerId(sellerId)
                    .quoteQuantity(fillPrice * fillQty)
                    .price(fillPrice)
                    .quantity(fillQty)
                    .sequenceNo(tradeSequenceGenerator.incrementAndGet())
                    .build();

            event.trades.add(trade);
            event.orderEvents.add(createOrderEvent(makerOrder, trade.getId(), EventType.TRADE_MATCHED, makerStatusBefore, makerOrder.getStatus(), fillQty, fillPrice));
            event.orderEvents.add(createOrderEvent(takerOrder, trade.getId(), EventType.TRADE_MATCHED, takerStatusBefore, takerOrder.getStatus(), fillQty, fillPrice));
        }

        if (remainingQty > 0L) {
            orderBook.addOrder(takerOrder);
        }
        event.modifiedOrders.add(takerOrder);
    }

    private OrderEvent createOrderEvent(Order order, Long tradeId, EventType eventType,
                                        OrderStatus statusBefore, OrderStatus statusAfter, Long fillQty, Long fillPrice) {

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
    // 2. Journal Worker
    // ==========================================
//    private void startJournalWorker() {
//        new Thread(() -> {
//            final int BATCH_SIZE = 1000;
//            final List<MatchPayload> buffer = new ArrayList<>(BATCH_SIZE);
//            final List<Trade> trades = new ArrayList<>(BATCH_SIZE);
//            final List<OrderEvent> events = new ArrayList<>(BATCH_SIZE * 2);
//
//            while (true) {
//                try {
//                    MatchPayload firstPayload = journalQueue.take();
//                    if (!firstPayload.traces().isEmpty()) {
//                        journalQueueTimer.record(Duration.ofNanos(System.nanoTime() - firstPayload.traces().get(0).getPersistEnterTs()));
//                    }
//
//                    buffer.add(firstPayload);
//                    journalQueue.drainTo(buffer, BATCH_SIZE - 1);
//
//                    trades.clear();
//                    events.clear();
//
//                    for (MatchPayload payload : buffer) {
//                        if (payload.trade() != null) trades.add(payload.trade());
//                        if (payload.events() != null) events.addAll(payload.events());
//                        if (!payload.traces().isEmpty()) payload.traces().get(0).markDbWriteStart();
//                    }
//
//                    journalDbIoTimer.record(() -> eventExecutionRepository.appendEvents(trades, events));
//
//                    for (MatchPayload payload : buffer) {
//                        if (!payload.traces().isEmpty()) payload.traces().get(0).markDbWriteEnd();
//
//                        projectionQueue.offer(new MatchPayload(
//                                payload.trade(),
//                                payload.makerOrder(),
//                                payload.takerOrder(),
//                                payload.events(),
//                                payload.traces(),
//                                System.nanoTime()
//                        ));
//                    }
//
//                    buffer.clear();
//
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt(); break;
//                } catch (Exception e) {
//                    log.error("Journal worker encountered an error. Events were NOT passed to Projection.", e);
//                }
//            }
//        }, "Journal-Worker-Thread").start();
//    }
    private class JournalEventHandler implements EventHandler<EngineEvent> {
        private final List<Trade> batchTrades = new ArrayList<>(1000);
        private final List<OrderEvent> batchEvents = new ArrayList<>(2000);

        @Override
        public void onEvent(EngineEvent event, long sequence, boolean endOfBatch) {
            batchTrades.addAll(event.trades);
            batchEvents.addAll(event.orderEvents);

            // 스마트 배칭(Smart Batching): 디스럽터 링버퍼가 밀려있다면 IO 트랜잭션을 묶어서 일괄 처리
            if (endOfBatch && (!batchTrades.isEmpty() || !batchEvents.isEmpty())) {
                journalDbIoTimer.record(() -> eventExecutionRepository.appendEvents(batchTrades, batchEvents));
                batchTrades.clear();
                batchEvents.clear();
            }
            event.journalEndTs = System.nanoTime();
        }
    }

    // ==========================================
    // 3. Projection Worker
    // ==========================================
//    private void startProjectionWorker() {
//        new Thread(() -> {
//            final int BATCH_SIZE = 1000;
//            final List<MatchPayload> buffer = new ArrayList<>(BATCH_SIZE);
//            final Map<Long, Order> orderMap = new HashMap<>(BATCH_SIZE);
//
//            while (true) {
//                try {
//                    MatchPayload firstPayload = projectionQueue.take();
//
//                    projectionQueueTimer.record(Duration.ofNanos(System.nanoTime() - firstPayload.projectionEnterTs()));
//
//                    buffer.add(firstPayload);
//                    projectionQueue.drainTo(buffer, BATCH_SIZE - 1);
//
//                    orderMap.clear();
//
//                    for (MatchPayload payload : buffer) {
//                        if (payload.makerOrder() != null) orderMap.put(payload.makerOrder().getId(), payload.makerOrder());
//                        if (payload.takerOrder() != null) orderMap.put(payload.takerOrder().getId(), payload.takerOrder());
//                    }
//
//                    List<Order> ordersToUpdate = new ArrayList<>(orderMap.values());
//
//                    if (!ordersToUpdate.isEmpty()) {
//                        projectionDbIoTimer.record(() -> eventExecutionRepository.updateReadModels(ordersToUpdate));
//                    }
//
//                    buffer.clear();
//
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt(); break;
//                } catch (Exception e) {
//                    log.error("Projection worker encountered an error", e);
//                }
//            }
//        }, "Projection-Worker-Thread").start();
//    }
    private class ProjectionEventHandler implements EventHandler<EngineEvent> {
        private final Map<Long, Order> batchOrderMap = new HashMap<>(1000);

        @Override
        public void onEvent(EngineEvent event, long sequence, boolean endOfBatch) {
            for (Order order : event.modifiedOrders) {
                batchOrderMap.put(order.getId(), order);
            }

            if (endOfBatch && !batchOrderMap.isEmpty()) {
                List<Order> ordersToUpdate = new ArrayList<>(batchOrderMap.values());
                projectionDbIoTimer.record(() -> eventExecutionRepository.updateReadModels(ordersToUpdate));
                batchOrderMap.clear();
            }
        }
    }
}