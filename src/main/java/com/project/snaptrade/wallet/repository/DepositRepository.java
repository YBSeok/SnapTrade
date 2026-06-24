package com.project.snaptrade.wallet.repository;

import com.project.snaptrade.wallet.domain.Deposit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DepositRepository extends JpaRepository<Deposit, Long> {
    List<Deposit> findAllByUserId(Long userId);
}
