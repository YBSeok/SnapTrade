package com.project.snaptrade.wallet.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record DepositRequest(
        @NotNull Long userId,
        @NotBlank String assetSymbol,
        @NotNull @Positive BigDecimal amount,
        @NotBlank String fromAddress
) {}
