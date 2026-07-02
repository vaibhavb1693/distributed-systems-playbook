package com.vaibhav.idempotent.loanprocessing.repository;

import com.vaibhav.idempotent.loanprocessing.domain.LoanApplication;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface LoanApplicationRepository extends JpaRepository<LoanApplication, UUID> {

    // Strategy 3: optimistic locking / version check. A direct compare-and-swap update
    // rather than JPA's standard load-then-save @Version flow, so a stale or duplicate
    // event can be detected (0 rows updated) without loading the entity first, and without
    // throwing an OptimisticLockException that the caller would have to catch.
    @Modifying
    @Query("""
            UPDATE LoanApplication l
            SET l.kycVerified = true, l.version = l.version + 1
            WHERE l.id = :loanId AND l.version = :expectedVersion
            """)
    int markKycVerifiedIfVersionMatches(@Param("loanId") UUID loanId, @Param("expectedVersion") long expectedVersion);

    @Modifying
    @Query("UPDATE LoanApplication l SET l.creditScore = :creditScore WHERE l.id = :loanId")
    void updateCreditScore(@Param("loanId") UUID loanId, @Param("creditScore") int creditScore);
}
