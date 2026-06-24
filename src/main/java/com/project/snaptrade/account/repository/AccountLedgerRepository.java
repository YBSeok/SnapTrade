package com.project.snaptrade.account.repository;

import com.project.snaptrade.account.domain.AccountLedger;
import com.project.snaptrade.account.domain.LedgerEntryType;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AccountLedgerRepository extends JpaRepository<AccountLedger, Long> {
    boolean existsByReferenceIdAndEntryType(Long referenceId, LedgerEntryType entryType);
}
