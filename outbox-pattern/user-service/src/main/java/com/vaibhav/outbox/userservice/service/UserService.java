package com.vaibhav.outbox.userservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.outbox.userservice.domain.*;
import com.vaibhav.outbox.userservice.dto.*;
import com.vaibhav.outbox.userservice.repository.OutboxEventRepository;
import com.vaibhav.outbox.userservice.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {

    private final UserRepository userRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    // Tags every outbox event with how it was published (polling|cdc) so downstream
    // consumers can compare end-to-end latency between the two approaches.
    @Value("${outbox.mode:unknown}")
    private String outboxMode;

    // Both the user write and outbox event write commit or roll back together.
    // This is the core guarantee of the Outbox Pattern: no dual-write risk.
    @Transactional
    public UserResponse registerUser(RegisterUserRequest request) {
        if (userRepository.existsByEmail(request.email())) {
            throw new IllegalArgumentException("Email already registered: " + request.email());
        }

        User user = User.builder()
                .name(request.name())
                .email(request.email())
                .phone(request.phone())
                .kycStatus(KycStatus.PENDING)
                .build();
        User saved = userRepository.save(user);

        Map<String, Object> payload = Map.of(
                "userId", saved.getId().toString(),
                "name", saved.getName(),
                "email", saved.getEmail(),
                "phone", saved.getPhone() != null ? saved.getPhone() : "",
                "kycStatus", saved.getKycStatus().name(),
                "registeredAt", LocalDateTime.now().toString()
        );
        createOutboxEvent(saved.getId().toString(), EventType.USER_REGISTERED, payload);

        log.info("User registered: id={}, email={}", saved.getId(), saved.getEmail());
        return UserResponse.from(saved);
    }

    @Transactional
    public UserResponse updateProfile(UUID userId, UpdateProfileRequest request) {
        User user = findUserOrThrow(userId);
        String previousName = user.getName();

        user.setName(request.name());
        user.setPhone(request.phone());
        User saved = userRepository.save(user);

        Map<String, Object> payload = Map.of(
                "userId", saved.getId().toString(),
                "previousName", previousName,
                "name", saved.getName(),
                "phone", saved.getPhone() != null ? saved.getPhone() : "",
                "updatedAt", LocalDateTime.now().toString()
        );
        createOutboxEvent(saved.getId().toString(), EventType.PROFILE_UPDATED, payload);

        log.info("Profile updated: userId={}", userId);
        return UserResponse.from(saved);
    }

    @Transactional
    public UserResponse updateKycStatus(UUID userId, UpdateKycStatusRequest request) {
        User user = findUserOrThrow(userId);
        KycStatus previousStatus = user.getKycStatus();

        user.setKycStatus(request.status());
        User saved = userRepository.save(user);

        Map<String, Object> payload = Map.of(
                "userId", saved.getId().toString(),
                "previousStatus", previousStatus.name(),
                "newStatus", saved.getKycStatus().name(),
                "updatedAt", LocalDateTime.now().toString()
        );
        createOutboxEvent(saved.getId().toString(), EventType.KYC_STATUS_UPDATED, payload);

        log.info("KYC status updated: userId={}, {} -> {}", userId, previousStatus, request.status());
        return UserResponse.from(saved);
    }

    private void createOutboxEvent(String aggregateId, EventType eventType, Map<String, Object> payloadMap) {
        try {
            // eventCreatedAtMs + mode ride along inside the payload itself (not a side-channel)
            // because both the poller and Debezium's expand.json.payload publish this JSON
            // verbatim — so the same two fields let us measure polling-vs-CDC latency fairly
            // without touching the Debezium connector config.
            Map<String, Object> enrichedPayload = new HashMap<>(payloadMap);
            enrichedPayload.put("eventCreatedAtMs", String.valueOf(System.currentTimeMillis()));
            enrichedPayload.put("mode", outboxMode);

            String payloadJson = objectMapper.writeValueAsString(enrichedPayload);
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("USER")
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .topicName(eventType.getTopic())
                    .payload(payloadJson)
                    .status(OutboxStatus.PENDING)
                    .build();
            outboxEventRepository.save(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize outbox event payload", e);
        }
    }

    private User findUserOrThrow(UUID userId) {
        return userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("User not found: " + userId));
    }
}
