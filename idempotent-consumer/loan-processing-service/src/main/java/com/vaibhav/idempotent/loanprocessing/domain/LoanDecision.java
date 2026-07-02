package com.vaibhav.idempotent.loanprocessing.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "loan_decisions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LoanDecision {

    @Id
    private UUID loanId;

    private String decision;

    private LocalDateTime decidedAt;
}
