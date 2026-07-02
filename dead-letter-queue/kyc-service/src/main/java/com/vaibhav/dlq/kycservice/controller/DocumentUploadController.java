package com.vaibhav.dlq.kycservice.controller;

import com.vaibhav.dlq.kycservice.dto.UploadDocumentRequest;
import com.vaibhav.dlq.kycservice.dto.UploadDocumentResponse;
import com.vaibhav.dlq.kycservice.service.KycEventPublisher;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kyc/documents")
@RequiredArgsConstructor
public class DocumentUploadController {

    private final KycEventPublisher kycEventPublisher;

    @PostMapping("/upload")
    public ResponseEntity<UploadDocumentResponse> upload(@Valid @RequestBody UploadDocumentRequest request) {
        String simulate = request.simulate() == null ? "NONE" : request.simulate();
        String documentId = kycEventPublisher.publishDocumentUploaded(request.userId(), request.documentType(), simulate);
        return ResponseEntity.accepted().body(new UploadDocumentResponse(documentId));
    }
}
