package com.project.snaptrade.account.domain;

public enum LedgerEntryType {
    DEPOSIT,         // 외부 입금
    WITHDRAWAL,      // 외부 출금
    TRADE_FILL,      // 매매 체결 (자산 획득 또는 매각)
    TRADE_FEE,       // 매매 수수료 차감
    PROMOTION_BONUS,  // 이벤트 지급

    WITHDRAWAL_LOCK,
    WITHDRAWAL_FILL,
}
