package com.project.snaptrade.wallet.service;

import com.project.snaptrade.account.domain.Account;
import com.project.snaptrade.account.domain.AccountLedger;
import com.project.snaptrade.account.domain.LedgerEntryType;
import com.project.snaptrade.account.repository.AccountLedgerRepository;
import com.project.snaptrade.account.repository.AccountRepository;
import com.project.snaptrade.wallet.domain.Deposit;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class BalanceAdjustmentService {

    private final AccountRepository accountRepository;
    private final AccountLedgerRepository ledgerRepository;

    /**
     * 입금 처리 로직
     * 1. 멱등성 검증 (중복 입금 차단)
     * 2. 계좌 잔고 증가
     * 3. 원장 기록
     */
    @Transactional
    public void processDeposit(Deposit deposit) {
        // 1. 멱등성 검증: 동일한 Deposit ID로 이미 처리된 원장 기록이 있는지 확인
        if (ledgerRepository.existsByReferenceIdAndEntryType(deposit.getId(), LedgerEntryType.DEPOSIT)) {
            return; // 이미 처리됨
        }

        // 2. 계좌 조회
        Account account = accountRepository.findByUserIdAndAssetSymbol(deposit.getUserId(), deposit.getAssetSymbol())
                .orElseThrow(() -> new IllegalStateException("계좌가 존재하지 않습니다."));

        // 3. 잔고 업데이트
        long amountAsLong = deposit.getAmount().longValue();
        long balanceBefore = account.getTotalBalance();

        account.addAvailableBalance(amountAsLong);
        accountRepository.save(account);

        // 4. 원장 기록
        AccountLedger ledger = AccountLedger.builder()
                .accountId(account.getId())
                .userId(account.getUserId())
                .entryType(LedgerEntryType.DEPOSIT)
                .assetSymbol(account.getAssetSymbol())
                .amount(amountAsLong)
                .balanceBefore(balanceBefore)
                .balanceAfter(balanceBefore + amountAsLong)
                .referenceId(deposit.getId())
                .build();

        ledgerRepository.save(ledger);
    }
}
