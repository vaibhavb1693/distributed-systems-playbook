package com.vaibhav.dlq.kycservice.exception;

import lombok.Getter;

/** Corrupted document, unsupported format, OCR failure, vendor fraud flag — retrying won't help. */
@Getter
public class PermanentFailureException extends RuntimeException {

    private final String reasonCode;

    public PermanentFailureException(String reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }
}
