package com.project.snaptrade.wallet.service;

import com.project.snaptrade.account.domain.Account;
import com.project.snaptrade.account.domain.AccountLedger;
import com.project.snaptrade.account.domain.LedgerEntryType;
import com.project.snaptrade.account.repository.AccountLedgerRepository;
import com.project.snaptrade.account.repository.AccountRepository;
import com.project.snaptrade.common.event.NotificationType;
import com.project.snaptrade.common.event.PrivateNotificationEvent;
import com.project.snaptrade.wallet.domain.Deposit;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
public class BalanceAdjustmentService {

    private final AccountRepository accountRepository;
    private final AccountLedgerRepository ledgerRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Transactional
    public void processDeposit(Deposit deposit) {
        // 멱등성 검증
        if (ledgerRepository.existsByReferenceIdAndEntryType(deposit.getId(), LedgerEntryType.DEPOSIT)) {
            return; // 이미 처리됨
        }

        // 계좌 조회
        Account account = accountRepository.findByUserIdAndAssetSymbol(deposit.getUserId(), deposit.getAssetSymbol())
                .orElseThrow(() -> new IllegalStateException("계좌가 존재하지 않습니다."));

        // 잔고 업데이트
        long amountAsLong = deposit.getAmount().longValue();
        long balanceBefore = account.getTotalBalance();

        account.addAvailableBalance(amountAsLong);
        accountRepository.save(account);

        // 원장 기록
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

        eventPublisher.publishEvent(new PrivateNotificationEvent(
                deposit.getUserId(),
                NotificationType.DEPOSIT_COMPLETED,
                String.format("입금이 완료되었습니다. %s %s", deposit.getAmount().toPlainString(), deposit.getAssetSymbol()),
                Map.of(
                        "assetSymbol", deposit.getAssetSymbol(),
                        "amount", deposit.getAmount().toPlainString()
                )
        ));
    }
}
