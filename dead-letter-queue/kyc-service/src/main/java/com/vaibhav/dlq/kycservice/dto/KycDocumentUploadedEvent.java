package com.vaibhav.dlq.kycservice.dto;

public record KycDocumentUploadedEvent(
        String documentId,
        String userId,
        String documentType,
        String simulate,
        String uploadedAt
) {}
