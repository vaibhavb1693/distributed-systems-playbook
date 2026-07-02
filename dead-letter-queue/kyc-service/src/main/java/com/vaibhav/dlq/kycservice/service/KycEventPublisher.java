package com.vaibhav.dlq.kycservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.dlq.kycservice.dto.KycDocumentUploadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class KycEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public String publishDocumentUploaded(String userId, String documentType, String simulate) {
        String documentId = UUID.randomUUID().toString();
        KycDocumentUploadedEvent event = new KycDocumentUploadedEvent(
                documentId, userId, documentType, simulate, LocalDateTime.now().toString());
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("finflow.kyc.document.uploaded", documentId, json);
            log.info("Published document upload: documentId={} userId={} simulate={}", documentId, userId, simulate);
            return documentId;
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish KYC document upload event", e);
        }
    }
}
