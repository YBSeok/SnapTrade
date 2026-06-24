package com.project.snaptrade.wallet.domain;

import com.project.snaptrade.wallet.domain.constant.WithdrawalStatus;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "withdrawals")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Withdrawal {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "asset_symbol", nullable = false, length = 20)
    private String assetSymbol;

    @Column(nullable = false, precision = 36, scale = 18)
    private BigDecimal amount;

    @Column(name = "destination_address", nullable = false, length = 255)
    private String destinationAddress;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private WithdrawalStatus status;

    @Column(name = "tx_hash", length = 255)
    private String txHash;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    @Builder
    public Withdrawal(Long userId, String assetSymbol, BigDecimal amount, String destinationAddress, WithdrawalStatus status) {
        this.userId = userId;
        this.assetSymbol = assetSymbol;
        this.amount = amount;
        this.destinationAddress = destinationAddress;
        this.status = status;
    }

    public void complete(String txHash) {
        this.status = WithdrawalStatus.COMPLETED;
        this.txHash = txHash;
        this.processedAt = LocalDateTime.now();
    }
}
