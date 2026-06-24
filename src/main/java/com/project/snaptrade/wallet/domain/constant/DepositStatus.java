package com.project.snaptrade.wallet.domain.constant;

public enum DepositStatus {
    PENDING,    // 입금 대기
    CONFIRMED,  // 입금 완료
    REJECTED,   // 입금 거절
    CANCELLED   // 취소
}