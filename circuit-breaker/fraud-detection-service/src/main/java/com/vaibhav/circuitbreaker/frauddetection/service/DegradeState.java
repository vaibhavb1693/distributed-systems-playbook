package com.vaibhav.circuitbreaker.frauddetection.service;

import com.vaibhav.circuitbreaker.frauddetection.domain.DegradeMode;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

/**
 * In-memory failure-injection switch, flipped via AdminController. Deliberately not
 * persisted anywhere — this is a single-instance demo service, and the whole point is
 * to be able to flip it back to NONE instantly for the next demo run.
 */
@Component
@Getter
@Setter
public class DegradeState {
    private volatile DegradeMode mode = DegradeMode.NONE;
    private volatile long latencyMs = 5000;
}
