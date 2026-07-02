package com.vaibhav.dlq.kycprocessor.dto;

import jakarta.validation.constraints.NotBlank;

public record ResolveRequest(@NotBlank String resolutionNote, @NotBlank String resolvedBy) {}
