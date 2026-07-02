package com.vaibhav.outbox.userservice.dto;

import com.vaibhav.outbox.userservice.domain.KycStatus;
import com.vaibhav.outbox.userservice.domain.User;

import java.time.LocalDateTime;
import java.util.UUID;

public record UserResponse(
        UUID id,
        String name,
        String email,
        String phone,
        KycStatus kycStatus,
        LocalDateTime createdAt
) {
    public static UserResponse from(User user) {
        return new UserResponse(
                user.getId(),
                user.getName(),
                user.getEmail(),
                user.getPhone(),
                user.getKycStatus(),
                user.getCreatedAt()
        );
    }
}
