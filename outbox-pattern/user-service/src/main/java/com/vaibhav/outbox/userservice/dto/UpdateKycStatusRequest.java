package com.vaibhav.outbox.userservice.dto;

import com.vaibhav.outbox.userservice.domain.KycStatus;
import jakarta.validation.constraints.NotNull;

public record UpdateKycStatusRequest(
        @NotNull KycStatus status
) {}
