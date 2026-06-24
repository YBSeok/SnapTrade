package com.project.snaptrade.wallet.domain;

import com.project.snaptrade.wallet.domain.constant.DepositStatus;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "deposits")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Deposit {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "asset_symbol", nullable = false, length = 20)
    private String assetSymbol;

    @Column(nullable = false, precision = 36, scale = 18)
    private BigDecimal amount;

    @Column(name = "from_address", length = 255)
    private String fromAddress;

    @Column(name = "to_address", length = 255)
    private String toAddress;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private DepositStatus status;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "confirmed_at")
    private LocalDateTime confirmedAt;

    @Builder
    public Deposit(Long userId, String assetSymbol, BigDecimal amount, String fromAddress, String toAddress, DepositStatus status) {
        this.userId = userId;
        this.assetSymbol = assetSymbol;
        this.amount = amount;
        this.fromAddress = fromAddress;
        this.toAddress = toAddress;
        this.status = status;
    }
}