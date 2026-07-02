package com.vaibhav.idempotent.loanprocessing.repository;

import com.vaibhav.idempotent.loanprocessing.domain.LoanDecision;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.UUID;

@Repository
public interface LoanDecisionRepository extends JpaRepository<LoanDecision, UUID> {

    // Strategy 4: natural idempotency via upsert. Applying this any number of times with
    // the same loanId converges to identical state — no dedup bookkeeping needed.
    @Modifying
    @Query(value = """
            INSERT INTO loan_decisions (loan_id, decision, decided_at)
            VALUES (:loanId, :decision, :decidedAt)
            ON CONFLICT (loan_id) DO UPDATE
            SET decision = EXCLUDED.decision, decided_at = EXCLUDED.decided_at
            """, nativeQuery = true)
    void upsert(@Param("loanId") UUID loanId, @Param("decision") String decision, @Param("decidedAt") LocalDateTime decidedAt);
}
