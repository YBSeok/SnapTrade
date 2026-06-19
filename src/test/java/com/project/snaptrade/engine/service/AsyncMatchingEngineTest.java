package com.project.snaptrade.engine.service;

import com.project.snaptrade.engine.Dto.OrderRequestDto;
import com.project.snaptrade.engine.domain.Order;
import com.project.snaptrade.engine.domain.Trade;
import com.project.snaptrade.engine.domain.constant.OrderSide;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import com.project.snaptrade.engine.domain.constant.OrderType;
import com.project.snaptrade.engine.domain.constant.TimeInForce;
import com.project.snaptrade.engine.repository.OrderRepository;
import com.project.snaptrade.engine.repository.TradeRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AsyncMatchingEngineTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private TradeRepository tradeRepository;

    @InjectMocks
    private AsyncMatchingEngine matchingEngine;

    private static final long MARKET_ID = 1L;

    @BeforeEach
    void setUp() {
        lenient().when(orderRepository.findByStatusInOrderByCreatedAtAsc(anyList())).thenReturn(List.of());
    }

    @AfterEach
    void tearDown() {
        BlockingQueue<?> orderQueue = (BlockingQueue<?>) ReflectionTestUtils.getField(matchingEngine, "orderQueue");
        orderQueue.clear();
    }

    @Test
    @DisplayName("Edge Case 1: Cold Start 상태 복구 - DB의 미체결 주문이 메모리에 정상 적재되어야 함")
    void testColdStartRecovery() {
        // Given: DB에 기존 미체결 매도 주문 존재
        Order openAskOrder = createOrder(2L, OrderSide.SELL, "100.0", "10.0");
        when(orderRepository.findByStatusInOrderByCreatedAtAsc(anyList())).thenReturn(List.of(openAskOrder));

        // When: 엔진 초기화
        matchingEngine.init();

        // Then: 엔진이 초기화된 후 새로운 매수 주문이 들어왔을 때, 복구된 주문과 체결되는지 확인
        OrderRequestDto buyRequest = createRequest(1L, OrderSide.BUY, "100.0", "10.0");
        matchingEngine.placeOrder(buyRequest);

        // 비동기 영속화 워커가 Trade를 저장하는지 최대 1초간 대기하며 검증
        verify(tradeRepository, timeout(1000).times(1)).save(any(Trade.class));
    }

    @Test
    @DisplayName("Edge Case 2: 매칭 실패 - 상대방 호가가 내 가격 조건과 맞지 않으면 호가창에 등재만 됨")
    void testNoMatchPriceCondition() {
        matchingEngine.init();

        // Given: 매도 호가 100원
        matchingEngine.placeOrder(createRequest(1L, OrderSide.SELL, "100.0", "10.0"));

        // When: 매수 주문이 90원으로 들어옴 (매도 호가보다 낮음)
        matchingEngine.placeOrder(createRequest(2L, OrderSide.BUY, "90.0", "10.0"));

        // Then: 체결(Trade)이 발생하지 않아야 함
        verify(tradeRepository, after(500).never()).save(any());
    }

    @Test
    @DisplayName("Edge Case 3: 완벽한 일치 (Full Fill) - 수량과 가격이 정확히 일치")
    void testExactMatch() {
        matchingEngine.init();

        // Given: 매도 호가 100원, 10개
        matchingEngine.placeOrder(createRequest(1L, OrderSide.SELL, "100.0", "10.0"));

        // When: 매수 호가 100원, 10개
        matchingEngine.placeOrder(createRequest(2L, OrderSide.BUY, "100.0", "10.0"));

        // Then: 정확히 1건의 Trade가 발생하고, 수량은 10개여야 함
        ArgumentCaptor<Trade> tradeCaptor = ArgumentCaptor.forClass(Trade.class);
        verify(tradeRepository, timeout(1000).times(1)).save(tradeCaptor.capture());

        Trade trade = tradeCaptor.getValue();
        assertEquals(new BigDecimal("100.0"), trade.getPrice());
        assertEquals(new BigDecimal("10.0"), trade.getQuantity());
    }

    @Test
    @DisplayName("Edge Case 4: Maker 부분 체결 (Taker Qty < Maker Qty) - Maker 잔량이 호가창에 남아야 함")
    void testPartialMatchMakerLeft() {
        matchingEngine.init();

        // Given: 매도 호가 100원, 10개 (Maker)
        matchingEngine.placeOrder(createRequest(1L, OrderSide.SELL, "100.0", "10.0"));

        // When: 매수 호가 100원, 4개 (Taker)
        matchingEngine.placeOrder(createRequest(2L, OrderSide.BUY, "100.0", "4.0"));

        // Then: 4개 체결 확인
        ArgumentCaptor<Trade> tradeCaptor = ArgumentCaptor.forClass(Trade.class);
        verify(tradeRepository, timeout(1000).times(1)).save(tradeCaptor.capture());
        assertEquals(new BigDecimal("4.0"), tradeCaptor.getValue().getQuantity());

        // 추가 검증: 매도 잔량(6개)을 소진하기 위한 6개짜리 새로운 매수 주문 시 체결되어야 함
        matchingEngine.placeOrder(createRequest(3L, OrderSide.BUY, "100.0", "6.0"));
        verify(tradeRepository, timeout(1000).times(2)).save(any(Trade.class));
    }

    @Test
    @DisplayName("Edge Case 5: Taker 부분 체결 후 호가창 잔류 (Taker Qty > Maker Qty)")
    void testPartialMatchTakerLeft() {
        matchingEngine.init();

        // Given: 매도 호가 100원, 5개 (Maker)
        matchingEngine.placeOrder(createRequest(1L, OrderSide.SELL, "100.0", "5.0"));

        // When: 매수 호가 100원, 15개 (Taker) -> 5개 체결 후 10개는 호가창에 매수 대기로 남아야 함
        matchingEngine.placeOrder(createRequest(2L, OrderSide.BUY, "100.0", "15.0"));

        // Then: 1차 체결 (5개) 확인
        ArgumentCaptor<Trade> tradeCaptor = ArgumentCaptor.forClass(Trade.class);
        verify(tradeRepository, timeout(1000).times(1)).save(tradeCaptor.capture());
        assertEquals(new BigDecimal("5.0"), tradeCaptor.getValue().getQuantity());

        // 추가 검증: 호가창에 남은 매수 잔량(10개)이 다음 매도 주문(10개)과 즉시 체결되어야 함
        matchingEngine.placeOrder(createRequest(3L, OrderSide.SELL, "100.0", "10.0"));
        verify(tradeRepository, timeout(1000).times(2)).save(any(Trade.class));
    }

    @Test
    @DisplayName("Edge Case 6: 다중 호가 탐색 (가격 및 시간 우선순위 보장 검증)")
    void testMultipleMakersPriceAndTimePriority() {
        matchingEngine.init();

        // Given: 다양한 매도 호가 적재
        // 1. 110원 (비쌈)
        matchingEngine.placeOrder(createRequest(1L, OrderSide.SELL, "110.0", "10.0"));
        // 2. 100원 (가장 쌈 - 첫 번째 등록)
        matchingEngine.placeOrder(createRequest(2L, OrderSide.SELL, "100.0", "5.0"));
        // 3. 100원 (가장 쌈 - 두 번째 등록, 시간 우선순위 밀림)
        matchingEngine.placeOrder(createRequest(3L, OrderSide.SELL, "100.0", "5.0"));
        // 4. 105원
        matchingEngine.placeOrder(createRequest(4L, OrderSide.SELL, "105.0", "10.0"));

        try { Thread.sleep(100); } catch (InterruptedException e) {}

        // When: 110원 이하로 15개 매수 요청 (Taker)
        matchingEngine.placeOrder(createRequest(5L, OrderSide.BUY, "110.0", "15.0"));

        // Then: 체결은 가장 싼 100원(2번, 3번) -> 105원(4번) 순으로 이루어져야 함
        ArgumentCaptor<Trade> tradeCaptor = ArgumentCaptor.forClass(Trade.class);
        // 총 3번의 체결 이벤트 발생 (100원 5개, 100원 5개, 105원 5개)
        verify(tradeRepository, timeout(1000).times(3)).save(tradeCaptor.capture());

        List<Trade> trades = tradeCaptor.getAllValues();
        assertEquals(3, trades.size());

        // 1차 체결: 100원, 5개
        assertEquals(new BigDecimal("100.0"), trades.get(0).getPrice());
        assertEquals(new BigDecimal("5.0"), trades.get(0).getQuantity());

        // 2차 체결: 100원, 5개 (시간 우선순위)
        assertEquals(new BigDecimal("100.0"), trades.get(1).getPrice());
        assertEquals(new BigDecimal("5.0"), trades.get(1).getQuantity());

        // 3차 체결: 105원, 5개 (잔량)
        assertEquals(new BigDecimal("105.0"), trades.get(2).getPrice());
        assertEquals(new BigDecimal("5.0"), trades.get(2).getQuantity());
    }

    // --- Helper Methods 수정됨 ---
    private Order createOrder(Long userId, OrderSide side, String price, String qty) {
        Order order = Order.builder()
                .userId(userId)
                .marketId(MARKET_ID)
                .side(side)
                .orderType(OrderType.LIMIT)
                .timeInForce(TimeInForce.GTC)
                .price(new BigDecimal(price))
                .origQty(new BigDecimal(qty))
                .build();

        // Order 엔티티 구조에 맞게 executedQty를 0으로 초기화 (remainingQty 필드 삭제)
        ReflectionTestUtils.setField(order, "executedQty", BigDecimal.ZERO);
        ReflectionTestUtils.setField(order, "cumulativeQuoteQty", BigDecimal.ZERO);
        ReflectionTestUtils.setField(order, "status", OrderStatus.NEW);

        // Trade 객체 생성 시 NullPointerException 방지를 위해 가짜 ID 주입
        ReflectionTestUtils.setField(order, "id", userId);

        return order;
    }

    private OrderRequestDto createRequest(Long userId, OrderSide side, String price, String qty) {
        OrderRequestDto dto = new OrderRequestDto();
        ReflectionTestUtils.setField(dto, "userId", userId);
        ReflectionTestUtils.setField(dto, "marketId", MARKET_ID);
        ReflectionTestUtils.setField(dto, "side", side);
        ReflectionTestUtils.setField(dto, "orderType", OrderType.LIMIT);
        ReflectionTestUtils.setField(dto, "timeInForce", TimeInForce.GTC);
        ReflectionTestUtils.setField(dto, "price", new BigDecimal(price));
        ReflectionTestUtils.setField(dto, "quantity", new BigDecimal(qty));
        return dto;
    }
}