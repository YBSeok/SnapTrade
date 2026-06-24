package com.project.snaptrade.wallet.repository;

import com.project.snaptrade.wallet.domain.Withdrawal;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WithdrawalRepository extends JpaRepository<Withdrawal, Long> {
    List<Withdrawal> findAllByUserId(Long userId);
}
