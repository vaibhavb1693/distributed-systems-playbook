package com.vaibhav.outbox.userservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaibhav.outbox.userservice.domain.OutboxEvent;
import com.vaibhav.outbox.userservice.domain.User;
import com.vaibhav.outbox.userservice.dto.RegisterUserRequest;
import com.vaibhav.outbox.userservice.repository.OutboxEventRepository;
import com.vaibhav.outbox.userservice.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserServiceTest {

    @Mock
    private UserRepository userRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private UserService userService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(userService, "outboxMode", "polling");
    }

    @Test
    void shouldTagOutboxPayloadWithModeAndCreationTimestamp() throws Exception {
        when(userRepository.existsByEmail(anyString())).thenReturn(false);
        when(userRepository.save(org.mockito.ArgumentMatchers.any(User.class)))
                .thenAnswer(invocation -> {
                    User user = invocation.getArgument(0);
                    user.setId(UUID.randomUUID());
                    return user;
                });

        long before = System.currentTimeMillis();
        userService.registerUser(new RegisterUserRequest("Vaibhav Bhatt", "vaibhav@finflow.com", "+91-9876543210"));
        long after = System.currentTimeMillis();

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        org.mockito.Mockito.verify(outboxEventRepository).save(captor.capture());

        String payload = captor.getValue().getPayload();
        assertThat(payload).contains("\"mode\":\"polling\"");

        long eventCreatedAtMs = Long.parseLong(
                objectMapper.readTree(payload).get("eventCreatedAtMs").asText());
        assertThat(eventCreatedAtMs).isBetween(before, after);
    }
}
