package com.vaibhav.ratelimiter.openbanking.controller;

import com.vaibhav.ratelimiter.openbanking.dto.AccountsResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * One generic endpoint, not five near-identical ones — {algorithm} is only used by
 * RateLimitInterceptor (which runs before this method) to pick the rate limiting
 * strategy; the controller itself is algorithm-agnostic.
 */
@RestController
public class OpenBankingController {

    @GetMapping("/ob/{algorithm}/accounts")
    public ResponseEntity<AccountsResponse> getAccounts(@PathVariable String algorithm) {
        return ResponseEntity.ok(new AccountsResponse(List.of("acc-1001", "acc-1002", "acc-1003")));
    }
}
