package com.project.snaptrade.account.repository;

import com.project.snaptrade.account.domain.Account;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AccountRepository extends JpaRepository<Account, Long> {
    Optional<Account> findByUserIdAndAssetSymbol(Long userId, String assetSymbol);
}
