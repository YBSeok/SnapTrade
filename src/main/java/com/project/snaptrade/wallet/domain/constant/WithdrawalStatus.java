package com.project.snaptrade.wallet.domain.constant;

public enum WithdrawalStatus {
    REQUESTED,    // 사용자가 출금 요청함
    PROCESSING,   // 시스템에서 출금 트랜잭션 생성 중
    COMPLETED,    // 블록체인 전송 완료
    REJECTED,     // 시스템 정책에 의해 거절됨
    FAILED        // 블록체인 전송 실패
}
