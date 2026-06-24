package com.project.snaptrade.account.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(
        name = "accounts",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"user_id", "asset_symbol"})}
)
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "asset_symbol", nullable = false, length = 20)
    private String assetSymbol; // 예: KRW, BTC, USDT

    @Column(nullable = false)
    private long totalBalance;

    @Column(nullable = false)
    private long availableBalance;

    @Column(nullable = false)
    private long lockedBalance;

    @Version
    @Column(nullable = false)
    private Integer version;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    public void holdBalance(long amount) {
        if (this.availableBalance < amount) {
            throw new IllegalStateException("가용 잔고가 부족합니다.");
        }
        this.availableBalance -= amount;
        this.lockedBalance += amount;
    }

    public void releaseBalance(long amount) {
        this.availableBalance += amount;
        this.lockedBalance -= amount;
    }

    public void deductLockedBalance(long amount) {
        this.totalBalance -= amount;
        this.lockedBalance -= amount;
    }

    public void addAvailableBalance(long amount) {
        this.totalBalance += amount;
        this.availableBalance += amount;
    }

    public void deductAvailableBalance(long amount) {
        if (this.availableBalance < amount) {
            throw new IllegalStateException("가용 잔고가 부족합니다.");
        }
        this.totalBalance -= amount;
        this.availableBalance -= amount;
    }
}
