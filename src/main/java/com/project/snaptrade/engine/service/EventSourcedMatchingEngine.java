package com.project.snaptrade.engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.f4b6a3.tsid.TsidCreator;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.project.snaptrade.common.event.OrderNotificationEvent;
import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.domain.*;
import com.project.snaptrade.engine.domain.constant.*;
import com.project.snaptrade.engine.repository.EventExecutionRepository;
import com.project.snaptrade.engine.repository.ExecutionRepository;
import com.project.snaptrade.engine.repository.OrderRepository;
import com.project.snaptrade.engine.repository.TradeRepository;
import com.project.snaptrade.market.domain.Market;
import com.project.snaptrade.market.domain.MarketSpec;
import com.project.snaptrade.market.dto.TradeCompletedEvent;
import com.project.snaptrade.market.repository.MarketRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
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
    public enum EngineCommand {
        PLACE, CANCEL
    }
    public static class EngineEvent {
        public EngineCommand command;
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

    private final ApplicationEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final MarketMetadataCache marketCache;
    private final OrderProjectionWorker orderProjectionWorker;

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

        initDisruptor();

        Gauge.builder("engine.ringbuffer.remaining", ringBuffer, RingBuffer::remainingCapacity)
                .description("Remaining capacity of LMAX Disruptor RingBuffer")
                .tag("application", "snaptrade-engine")
                .register(meterRegistry);
    }

    private void recoverOrderBookState() {
        log.info("System initializing: Replaying events to reconstruct OrderBooks...");
        long startTime = System.nanoTime();

        List<OrderEvent> allEvents = eventExecutionRepository.findAllEventsOrderByIdAsc();
        Map<Long, Order> stateMap = new HashMap<>();

        for (OrderEvent event : allEvents) {
            Long orderId = event.getOrderId();

            switch (event.getEventType()) {
                case ORDER_PLACED:
                    Order newOrder = reconstructOrderFromEvent(orderId, event);
                    stateMap.put(orderId, newOrder);
                    break;

                case TRADE_MATCHED:
                    Order existingOrder = stateMap.get(orderId);
                    if (existingOrder != null) {
                        existingOrder.fill(event.getFillQty(), event.getFillPrice());
                        if (existingOrder.getStatus() == OrderStatus.FILLED || existingOrder.getStatus() == OrderStatus.CANCELED) {
                            stateMap.remove(orderId);
                        }
                    }
                    break;

                case ORDER_CANCELED:
                    stateMap.remove(orderId);
                    break;
            }
        }

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
                .then(new PublisherEventHandler());

        ringBuffer = disruptor.start();
        log.info("LMAX Disruptor Pipeline started successfully. (Matching → Journal → Publisher; Projection is async)");
    }

    public Long placeOrder(OrderTrace trace) {
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
            event.command = EngineCommand.PLACE;
            event.trace = trace;
        });

        return newOrder.getId();
    }

    public void cancelOrder(OrderTrace trace) {
        trace.markEngineEnter();
        ringBuffer.publishEvent((event, sequence) -> {
            event.clear();
            event.command = EngineCommand.CANCEL;
            event.trace = trace;
        });
    }

    private class MatchingEventHandler implements EventHandler<EngineEvent> {
        @Override
        public void onEvent(EngineEvent event, long sequence, boolean endOfBatch) {
            OrderTrace trace = event.trace;
            engineQueueTimer.record(Duration.ofNanos(System.nanoTime() - trace.getEngineEnterTs()));

            long matchStart = System.nanoTime();

            // [수정] 명령어 타입에 따른 라우팅 분기
            if (event.command == EngineCommand.PLACE) {
                processMatch(event);
            } else if (event.command == EngineCommand.CANCEL) {
                processCancel(event);
            }

            coreMatchingTimer.record(Duration.ofNanos(System.nanoTime() - matchStart));
            event.matchEndTs = System.nanoTime();
        }
    }

    private void processMatch(EngineEvent event) {
        Order takerOrder = event.trace.getOrder();
        Long marketId = takerOrder.getMarketId();
        MarketSpec spec = marketCache.getSpec(marketId);
        OrderBook orderBook = orderBooks.computeIfAbsent(marketId, id -> new OrderBook());

        long remainingQty = takerOrder.getOrigQty();
        OrderSide opponentSide = takerOrder.getSide() == OrderSide.BUY ? OrderSide.SELL : OrderSide.BUY;

        boolean isMarketOrder = takerOrder.getOrderType() == OrderType.MARKET;

        boolean isPostOnly = takerOrder.isPostOnly();
        boolean isFok = takerOrder.getTimeInForce() == TimeInForce.FOK;
        boolean isIoc = takerOrder.getTimeInForce() == TimeInForce.IOC;

        Queue<Order> initialBestQueue = orderBook.getBestPriceQueue(opponentSide);

        // Post-Only 정책 검증
        if (isPostOnly && initialBestQueue != null && !initialBestQueue.isEmpty()) {
            Order bestMaker = initialBestQueue.peek();
            if ((takerOrder.getSide() == OrderSide.BUY && takerOrder.getPrice() >= bestMaker.getPrice()) ||
                    (takerOrder.getSide() == OrderSide.SELL && takerOrder.getPrice() <= bestMaker.getPrice())) {

                takerOrder.reject();
                event.orderEvents.add(createOrderEvent(takerOrder, null, EventType.ORDER_REJECTED, null, OrderStatus.REJECTED, 0L, 0L));
                event.modifiedOrders.add(takerOrder);
                return;
            }
        }

        // FOK(Fill-Or-Kill) 검증
        if (isFok && !orderBook.hasEnoughLiquidity(opponentSide, remainingQty, takerOrder.getPrice(), isMarketOrder)) {
            takerOrder.cancel(remainingQty);
            event.orderEvents.add(createOrderEvent(takerOrder, null, EventType.ORDER_CANCELED, null, OrderStatus.CANCELED, 0L, 0L));
            event.modifiedOrders.add(takerOrder);
            return;
        }

        // 시장가 가격 보호 대역선 계산 (최우선 호가 대비 ±5%)
        long priceBandLimit = 0L;
        if (initialBestQueue != null && !initialBestQueue.isEmpty()) {
            long bestPrice = initialBestQueue.peek().getPrice();
            priceBandLimit = takerOrder.getSide() == OrderSide.BUY ? (long)(bestPrice * 1.05) : (long)(bestPrice * 0.95);
        }

        if (takerOrder.getExecutedQty() == 0L) {
            event.orderEvents.add(createOrderEvent(takerOrder, null, EventType.ORDER_PLACED, null, OrderStatus.NEW, 0L, 0L));
        }

        while (remainingQty > 0L) {
            Queue<Order> bestPriceQueue = orderBook.getBestPriceQueue(opponentSide);
            if (bestPriceQueue == null || bestPriceQueue.isEmpty()) break;

            Order makerOrder = bestPriceQueue.peek();

            // 자전 거래 방지
            if (makerOrder.getUserId().equals(takerOrder.getUserId())) {
                OrderStatus takerStatusBefore = takerOrder.getStatus();
                takerOrder.cancel(remainingQty);
                event.orderEvents.add(createOrderEvent(takerOrder, null, EventType.ORDER_CANCELED, takerStatusBefore, takerOrder.getStatus(), 0L, 0L));
                remainingQty = 0L;
                break;
            }

            // 시장가 가격 보호 대역 돌파 검사
            if (isMarketOrder && priceBandLimit > 0L) {
                if (takerOrder.getSide() == OrderSide.BUY && makerOrder.getPrice() > priceBandLimit) break;
                if (takerOrder.getSide() == OrderSide.SELL && makerOrder.getPrice() < priceBandLimit) break;
            }

            // 지정가 조건 검사
            if (!isMarketOrder) {
                if (takerOrder.getSide() == OrderSide.BUY && takerOrder.getPrice() < makerOrder.getPrice()) break;
                if (takerOrder.getSide() == OrderSide.SELL && takerOrder.getPrice() > makerOrder.getPrice()) break;
            }

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

            long takerFee = calculateFee(fillPrice, fillQty, spec.takerFeeRate());
            long makerFee = calculateFee(fillPrice, fillQty, spec.makerFeeRate());

            Long buyerId = takerOrder.getSide() == OrderSide.BUY ? takerOrder.getUserId() : makerOrder.getUserId();
            Long sellerId = takerOrder.getSide() == OrderSide.SELL ? takerOrder.getUserId() : makerOrder.getUserId();

            Long makerUserId = makerOrder.getUserId();
            Long takerUserId = takerOrder.getUserId();

            Trade trade = Trade.builder()
                    .id(TsidCreator.getTsid().toLong())
                    .marketId(marketId)
                    .makerOrderId(makerOrder.getId())
                    .takerOrderId(takerOrder.getId())
                    .makerUserId(makerUserId)
                    .takerUserId(takerUserId)
                    .buyerId(buyerId)
                    .sellerId(sellerId)
                    .quoteQuantity(fillPrice * fillQty)
                    .price(fillPrice)
                    .quantity(fillQty)
                    .takerFee(takerFee)
                    .makerFee(makerFee)
                    .sequenceNo(tradeSequenceGenerator.incrementAndGet())
                    .build();

            event.trades.add(trade);
            event.orderEvents.add(createOrderEvent(makerOrder, trade.getId(), EventType.TRADE_MATCHED, makerStatusBefore, makerOrder.getStatus(), fillQty, fillPrice));
            event.orderEvents.add(createOrderEvent(takerOrder, trade.getId(), EventType.TRADE_MATCHED, takerStatusBefore, takerOrder.getStatus(), fillQty, fillPrice));
        }

        // 잔여 수량 처리 및 TIF(IOC) 조건 반영
        if (remainingQty > 0L) {
            if (isMarketOrder || isIoc) {
                OrderStatus statusBeforeCancel = takerOrder.getStatus();
                takerOrder.cancel(remainingQty);
                event.orderEvents.add(createOrderEvent(takerOrder, null, EventType.ORDER_CANCELED, statusBeforeCancel, takerOrder.getStatus(), 0L, 0L));
            } else {
                orderBook.addOrder(takerOrder);
            }
        }
        event.modifiedOrders.add(takerOrder);
    }

    private void processCancel(EngineEvent event) {
        Order targetOrder = event.trace.getOrder();
        Long marketId = targetOrder.getMarketId();
        OrderBook orderBook = orderBooks.get(marketId);

        if (orderBook != null) {
            boolean removed = orderBook.removeOrder(targetOrder);

            if (removed) {
                OrderStatus statusBefore = targetOrder.getStatus();
                targetOrder.cancel(targetOrder.getRemainingQty());

                event.modifiedOrders.add(targetOrder);
                event.orderEvents.add(createOrderEvent(targetOrder, null, EventType.ORDER_CANCELED, statusBefore, targetOrder.getStatus(), 0L, 0L));
            }
        }
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

    private long calculateFee(long price, long qty, long feeRate) {
        long notional = (price * qty) / 100_000_000L;
        return (notional * feeRate) / 1_000_000L;
    }

    private class JournalEventHandler implements EventHandler<EngineEvent> {
        private final List<Trade> batchTrades = new ArrayList<>(1000);
        private final List<OrderEvent> batchEvents = new ArrayList<>(2000);

        @Override
        public void onEvent(EngineEvent event, long sequence, boolean endOfBatch) {
            batchTrades.addAll(event.trades);
            batchEvents.addAll(event.orderEvents);

            if (endOfBatch && (!batchTrades.isEmpty() || !batchEvents.isEmpty())) {
                journalDbIoTimer.record(() -> eventExecutionRepository.appendEvents(batchTrades, batchEvents));
                batchTrades.clear();
                batchEvents.clear();
            }
            event.journalEndTs = System.nanoTime();
        }
    }

    private class PublisherEventHandler implements EventHandler<EngineEvent> {
        @Override
        public void onEvent(EngineEvent event, long sequence, boolean endOfBatch) {
            try {
                // 알림용 orderId→Order 조회맵 + Projection용 불변 스냅샷 준비
                Map<Long, Order> orderMap = new HashMap<>();
                List<OrderProjectionSnapshot> projectionSnapshots = new ArrayList<>(event.modifiedOrders.size());
                for (Order order : event.modifiedOrders) {
                    orderMap.put(order.getId(), order);
                    projectionSnapshots.add(OrderProjectionSnapshot.from(order));
                }

                // 1) Read Model: orders 테이블 UPSERT
                if (!projectionSnapshots.isEmpty()) {
                    orderProjectionWorker.enqueue(projectionSnapshots);
                }

                // 2) 체결 후처리: 잔고 정산 / 시세·차트 / 스탑트리거
                for (Trade trade : event.trades) {
                    eventPublisher.publishEvent(new TradeCompletedEvent(trade));
                }

                // 3) 개인화 알림: PLACED / FILLED / PARTIAL / CANCELED / REJECTED → WS
                for (OrderEvent oe : event.orderEvents) {
                    Order order = orderMap.get(oe.getOrderId());
                    if (order != null) {
                        eventPublisher.publishEvent(new OrderNotificationEvent(
                                order.getUserId(),
                                order.getId(),
                                order.getMarketId(),
                                oe.getEventType(),
                                oe.getStatusBefore(),
                                oe.getStatusAfter(),
                                oe.getFillQty(),
                                oe.getFillPrice(),
                                order.getExecutedQty(),
                                order.getOrigQty()
                        ));
                    }
                }
            } catch (Exception e) {
                log.error("[Engine-Publisher] Failed to publish events at sequence: {}", sequence, e);
            } finally {
                event.clear();
            }
        }
    }
}