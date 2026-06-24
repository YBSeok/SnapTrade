package com.project.snaptrade.engine.repository;

import com.project.snaptrade.engine.domain.constant.OrderSide;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryBalanceRepository {
    private static class UserBalance {
        long availableKrw; // 가용 원화
        long holdKrw;      // 동결 원화
        long availableBtc; // 가용 코인 (스케일링된 정수)
        long holdBtc;      // 동결 코인

        public UserBalance(long krw, long btc) {
            this.availableKrw = krw;
            this.availableBtc = btc;
        }
    }

    private final Map<Long, UserBalance> balanceCache = new ConcurrentHashMap<>();

    public void initUserBalance(Long userId, long krw, long btc) {
        balanceCache.put(userId, new UserBalance(krw, btc));
    }

    public boolean tryPreTradeHold(Long userId, Long marketId, OrderSide side, long price, long quantity) {
        // compute 메서드는 해당 Key에 대해 단일 스레드 접근을 보장하여 Race Condition을 방지합니다.
        UserBalance finalState = balanceCache.compute(userId, (id, balance) -> {
            if (balance == null) return null; // 유저 잔고 정보가 없으면 실패 처리

            if (side == OrderSide.BUY) {
                // 매수: 원화(KRW)가 필요함. (수수료 계산 등 추가 필요 시 여기서 반영)
                long requiredKrw = price * (quantity / 100000000L);
                if (balance.availableKrw >= requiredKrw) {
                    balance.availableKrw -= requiredKrw;
                    balance.holdKrw += requiredKrw;
                    return balance; // 상태 전이 성공
                }
            } else if (side == OrderSide.SELL) {
                // 매도: 코인(BTC)이 필요함.
                long requiredBtc = quantity;
                if (balance.availableBtc >= requiredBtc) {
                    balance.availableBtc -= requiredBtc;
                    balance.holdBtc += requiredBtc;
                    return balance; // 상태 전이 성공
                }
            }

            return balance;
        });

        return finalState != null;
    }
}
