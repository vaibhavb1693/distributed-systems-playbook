package com.vaibhav.outbox.userservice.controller;

import com.vaibhav.outbox.userservice.dto.*;
import com.vaibhav.outbox.userservice.service.UserService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @PostMapping("/register")
    public ResponseEntity<UserResponse> register(@Valid @RequestBody RegisterUserRequest request) {
        UserResponse response = userService.registerUser(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PutMapping("/{userId}/profile")
    public ResponseEntity<UserResponse> updateProfile(
            @PathVariable UUID userId,
            @Valid @RequestBody UpdateProfileRequest request) {
        UserResponse response = userService.updateProfile(userId, request);
        return ResponseEntity.ok(response);
    }

    @PutMapping("/{userId}/kyc-status")
    public ResponseEntity<UserResponse> updateKycStatus(
            @PathVariable UUID userId,
            @Valid @RequestBody UpdateKycStatusRequest request) {
        UserResponse response = userService.updateKycStatus(userId, request);
        return ResponseEntity.ok(response);
    }
}
