package com.project.snaptrade.wallet.service;

import com.project.snaptrade.account.domain.Account;
import com.project.snaptrade.account.domain.AccountLedger;
import com.project.snaptrade.account.domain.LedgerEntryType;
import com.project.snaptrade.account.repository.AccountLedgerRepository;
import com.project.snaptrade.account.repository.AccountRepository;
import com.project.snaptrade.wallet.domain.Withdrawal;
import com.project.snaptrade.wallet.domain.constant.WithdrawalStatus;
import com.project.snaptrade.wallet.repository.WithdrawalRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
public class WithdrawalService {

    private final AccountRepository accountRepository;
    private final AccountLedgerRepository ledgerRepository;
    private final WithdrawalRepository withdrawalRepository;

    /**
     * 1단계: 출금 요청 (잔고 동결)
     * 이 시점에 available -> locked로 이동시킵니다.
     */
    @Transactional
    public Long requestWithdrawal(Long userId, String assetSymbol, BigDecimal amount, String address) {
        // 1. 계좌 조회 및 동결
        Account account = accountRepository.findByUserIdAndAssetSymbol(userId, assetSymbol)
                .orElseThrow(() -> new IllegalStateException("계좌가 존재하지 않습니다."));

        long amountAsLong = amount.movePointRight(8).longValue(); // 10^8 스케일 변환
        account.holdBalance(amountAsLong);
        accountRepository.save(account);

        // 2. 출금 레코드 생성
        Withdrawal withdrawal = Withdrawal.builder()
                .userId(userId)
                .assetSymbol(assetSymbol)
                .amount(amount)
                .destinationAddress(address)
                .status(WithdrawalStatus.REQUESTED)
                .build();
        withdrawalRepository.save(withdrawal);

        // 3. 원장 기록 (동결됨)
        saveLedger(account, LedgerEntryType.WITHDRAWAL_LOCK, -amountAsLong, withdrawal.getId());

        return withdrawal.getId();
    }

    /**
     * 2단계: 출금 완료 (최종 차감)
     * 외부(블록체인) 전송이 성공한 후 호출됩니다.
     */
    @Transactional
    public void completeWithdrawal(Long withdrawalId, String txHash) {
        Withdrawal withdrawal = withdrawalRepository.findById(withdrawalId)
                .orElseThrow(() -> new IllegalStateException("출금 요청을 찾을 수 없습니다."));

        Account account = accountRepository.findByUserIdAndAssetSymbol(withdrawal.getUserId(), withdrawal.getAssetSymbol())
                .orElseThrow(() -> new IllegalStateException("계좌가 존재하지 않습니다."));

        // 1. 상태 업데이트
        withdrawal.complete(txHash);
        withdrawalRepository.save(withdrawal);

        // 2. 최종 차감 (locked -> total 감소)
        long amountAsLong = withdrawal.getAmount().movePointRight(8).longValue();
        account.deductLockedBalance(amountAsLong);
        accountRepository.save(account);

        // 3. 원장 기록 (최종 출금)
        saveLedger(account, LedgerEntryType.WITHDRAWAL_FILL, -amountAsLong, withdrawal.getId());
    }

    private void saveLedger(Account account, LedgerEntryType type, long amount, Long referenceId) {
        AccountLedger ledger = AccountLedger.builder()
                .accountId(account.getId())
                .userId(account.getUserId())
                .entryType(type)
                .assetSymbol(account.getAssetSymbol())
                .amount(amount)
                .balanceBefore(account.getTotalBalance() - amount)
                .balanceAfter(account.getTotalBalance())
                .referenceId(referenceId)
                .build();
        ledgerRepository.save(ledger);
    }
}
