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
@Table(name = "processed_credit_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedCreditEvent {

    @Id
    private UUID eventId;

    private UUID loanId;

    private LocalDateTime processedAt;
}
