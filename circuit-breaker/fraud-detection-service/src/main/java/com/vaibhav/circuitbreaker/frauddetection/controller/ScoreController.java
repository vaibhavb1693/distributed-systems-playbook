package com.vaibhav.circuitbreaker.frauddetection.controller;

import com.vaibhav.circuitbreaker.frauddetection.domain.DegradeMode;
import com.vaibhav.circuitbreaker.frauddetection.dto.ScoreRequest;
import com.vaibhav.circuitbreaker.frauddetection.dto.ScoreResponse;
import com.vaibhav.circuitbreaker.frauddetection.service.DegradeState;
import com.vaibhav.circuitbreaker.frauddetection.service.RiskScoringService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/fraud")
@RequiredArgsConstructor
@Slf4j
public class ScoreController {

    private final RiskScoringService riskScoringService;
    private final DegradeState degradeState;

    @PostMapping("/score")
    public ResponseEntity<ScoreResponse> score(@Valid @RequestBody ScoreRequest request) throws InterruptedException {
        DegradeMode mode = degradeState.getMode();

        if (mode == DegradeMode.DOWN) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
        if (mode == DegradeMode.ERROR) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
        if (mode == DegradeMode.LATENCY) {
            Thread.sleep(degradeState.getLatencyMs());
        }

        ScoreResponse response = new ScoreResponse(riskScoringService.score(request.amount()));
        log.info("Scored transaction payerId={} payeeId={} amount={} -> riskLevel={}",
                request.payerId(), request.payeeId(), request.amount(), response.riskLevel());
        return ResponseEntity.ok(response);
    }
}
