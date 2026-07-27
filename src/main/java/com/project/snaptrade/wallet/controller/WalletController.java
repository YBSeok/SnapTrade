package com.project.snaptrade.wallet.controller;

import com.project.snaptrade.wallet.domain.Deposit;
import com.project.snaptrade.wallet.domain.constant.DepositStatus;
import com.project.snaptrade.wallet.dto.DepositRequest;
import com.project.snaptrade.wallet.dto.WithdrawRequest;
import com.project.snaptrade.wallet.repository.DepositRepository;
import com.project.snaptrade.wallet.repository.WithdrawalRepository;
import com.project.snaptrade.wallet.service.BalanceAdjustmentService;
import com.project.snaptrade.wallet.service.WithdrawalService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/wallet")
@RequiredArgsConstructor
public class WalletController {

    private final WithdrawalService withdrawalService;
    private final BalanceAdjustmentService balanceAdjustmentService;
    private final DepositRepository depositRepository;
    private final WithdrawalRepository withdrawalRepository;

    @PostMapping("/withdraw")
    public ResponseEntity<Long> requestWithdrawal(@Valid @RequestBody WithdrawRequest request) {
        Long withdrawalId = withdrawalService.requestWithdrawal(
                request.userId(),
                request.assetSymbol(),
                request.amount(),
                request.destinationAddress()
        );
        return ResponseEntity.status(201).body(withdrawalId);
    }

    @GetMapping("/withdrawals")
    public ResponseEntity<?> getWithdrawalHistory(@RequestParam Long userId) {
        return ResponseEntity.ok(withdrawalRepository.findAllByUserId(userId));
    }

    @GetMapping("/deposits")
    public ResponseEntity<?> getDepositHistory(@RequestParam Long userId) {
        return ResponseEntity.ok(depositRepository.findAllByUserId(userId));
    }

    @PostMapping("/deposit")
    public ResponseEntity<Long> processManualDeposit(@Valid @RequestBody DepositRequest request) {
        Deposit deposit = Deposit.builder()
                .userId(request.userId())
                .assetSymbol(request.assetSymbol())
                .amount(request.amount())
                .fromAddress(request.fromAddress())
                .toAddress("SYSTEM_COLD_WALLET_ADDRESS")
                .status(DepositStatus.CONFIRMED)
                .build();
        depositRepository.save(deposit);
        balanceAdjustmentService.processDeposit(deposit);
        return ResponseEntity.status(201).body(deposit.getId());
    }
}
