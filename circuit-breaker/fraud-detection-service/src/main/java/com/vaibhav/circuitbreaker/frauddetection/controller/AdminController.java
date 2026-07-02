package com.vaibhav.circuitbreaker.frauddetection.controller;

import com.vaibhav.circuitbreaker.frauddetection.domain.DegradeMode;
import com.vaibhav.circuitbreaker.frauddetection.service.DegradeState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Failure-injection endpoint for demoing the circuit breaker. Excluded from a "prod"
 * profile — this class of endpoint (flip a live service into an error state on command)
 * should never exist in a real production deployment.
 */
@RestController
@RequestMapping("/admin")
@Profile("!prod")
@RequiredArgsConstructor
@Slf4j
public class AdminController {

    private final DegradeState degradeState;

    @PostMapping("/degrade")
    public ResponseEntity<String> degrade(
            @RequestParam String mode,
            @RequestParam(required = false, defaultValue = "5000") long latencyMs) {

        switch (mode.toLowerCase()) {
            case "latency" -> {
                degradeState.setMode(DegradeMode.LATENCY);
                degradeState.setLatencyMs(latencyMs);
            }
            case "error" -> degradeState.setMode(DegradeMode.ERROR);
            case "down" -> degradeState.setMode(DegradeMode.DOWN);
            case "reset" -> degradeState.setMode(DegradeMode.NONE);
            default -> {
                return ResponseEntity.badRequest().body("Unknown mode: " + mode + " (expected latency|error|down|reset)");
            }
        }

        log.warn("Fraud detection service degrade mode changed to: {}", degradeState.getMode());
        return ResponseEntity.ok("Degrade mode set to: " + degradeState.getMode());
    }
}
