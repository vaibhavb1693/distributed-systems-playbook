package com.vaibhav.dlq.kycservice.exception;

import lombok.Getter;

/** Vendor timeout, DB connection blip, downstream 503 — worth retrying. */
@Getter
public class TransientFailureException extends RuntimeException {

    private final String reasonCode;

    public TransientFailureException(String reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }
}
