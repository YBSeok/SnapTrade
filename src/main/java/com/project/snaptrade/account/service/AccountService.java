package com.project.snaptrade.account.service;

import com.project.snaptrade.account.domain.Account;
import com.project.snaptrade.account.domain.AccountLedger;
import com.project.snaptrade.account.domain.LedgerEntryType;
import com.project.snaptrade.account.repository.AccountLedgerRepository;
import com.project.snaptrade.account.repository.AccountRepository;
import com.project.snaptrade.engine.domain.Trade;
import com.project.snaptrade.engine.service.MarketMetadataCache;
import com.project.snaptrade.market.domain.MarketSpec;
import com.project.snaptrade.market.dto.TradeCompletedEvent;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final AccountLedgerRepository ledgerRepository;
    private final MarketMetadataCache marketCache;

    @Async("accountingTaskExecutor")
    @EventListener
    @Transactional
    public void onTradeCompleted(TradeCompletedEvent event) {
        Trade trade = event.trade();
        MarketSpec spec = marketCache.getSpec(trade.getMarketId());

        long buyerFee = determineFee(trade, trade.getBuyerId());
        long sellerFee = determineFee(trade, trade.getSellerId());

        processBuyer(trade, spec, buyerFee);
        processSeller(trade, spec, sellerFee);
    }

    private void processBuyer(Trade trade, MarketSpec spec, long fee) {
        Account quoteAcc = getAccount(trade.getBuyerId(), spec.quoteAsset());
        Account baseAcc = getAccount(trade.getBuyerId(), spec.baseAsset());

        long quoteBefore = quoteAcc.getTotalBalance();
        saveLedger(quoteAcc, LedgerEntryType.TRADE_FILL, -trade.getQuoteQuantity(), quoteBefore, trade.getId());
        quoteAcc.deductLockedBalance(trade.getQuoteQuantity());

        long baseBefore = baseAcc.getTotalBalance();
        saveLedger(baseAcc, LedgerEntryType.TRADE_FILL, trade.getQuantity(), baseBefore, trade.getId());
        baseAcc.addAvailableBalance(trade.getQuantity());

        if (fee > 0) {
            long feeBefore = baseAcc.getTotalBalance();
            saveLedger(baseAcc, LedgerEntryType.TRADE_FEE, -fee, feeBefore, trade.getId());
            baseAcc.deductAvailableBalance(fee);
        }
    }

    private void processSeller(Trade trade, MarketSpec spec, long fee) {
        Account baseAcc = getAccount(trade.getSellerId(), spec.baseAsset());
        Account quoteAcc = getAccount(trade.getSellerId(), spec.quoteAsset());

        long baseBefore = baseAcc.getTotalBalance();
        saveLedger(baseAcc, LedgerEntryType.TRADE_FILL, -trade.getQuantity(), baseBefore, trade.getId());
        baseAcc.deductLockedBalance(trade.getQuantity());

        long quoteBefore = quoteAcc.getTotalBalance();
        saveLedger(quoteAcc, LedgerEntryType.TRADE_FILL, trade.getQuoteQuantity(), quoteBefore, trade.getId());
        quoteAcc.addAvailableBalance(trade.getQuoteQuantity());

        if (fee > 0) {
            long feeBefore = quoteAcc.getTotalBalance();
            saveLedger(quoteAcc, LedgerEntryType.TRADE_FEE, -fee, feeBefore, trade.getId());
            quoteAcc.deductAvailableBalance(fee);
        }
    }

    private Account getAccount(Long userId, String assetSymbol) {
        return accountRepository.findByUserIdAndAssetSymbol(userId, assetSymbol)
                .orElseThrow(() -> new IllegalStateException("계좌를 찾을 수 없습니다. UserID: " + userId + ", Asset: " + assetSymbol));
    }

    private void saveLedger(Account account, LedgerEntryType type, long amount, long balanceBefore, Long tradeId) {
        AccountLedger ledger = AccountLedger.builder()
                .accountId(account.getId())
                .userId(account.getUserId())
                .entryType(type)
                .assetSymbol(account.getAssetSymbol())
                .amount(amount)
                .balanceBefore(balanceBefore)
                .balanceAfter(balanceBefore + amount)
                .referenceId(tradeId)
                .build();
        ledgerRepository.save(ledger);
    }

    private long determineFee(Trade trade, Long userId) {
        if (trade.getMakerUserId().equals(userId)) {
            return trade.getMakerFee();
        }
        else {
            return trade.getTakerFee();
        }
    }
}
