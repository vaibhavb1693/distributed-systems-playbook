package com.vaibhav.ratelimiter.loadtest;

import io.gatling.javaapi.core.OpenInjectionStep;
import io.gatling.javaapi.core.PopulationBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * The whole point of this pattern: every algorithm enforces the SAME FREE-tier limit
 * (60/min, no burst allowance) for the same partner, so any difference in how much traffic
 * gets through during a burst is purely the algorithm's doing.
 *
 * All 5 algorithm scenarios run concurrently against the identical injection profile — 3
 * bursts of 80 near-simultaneous requests, 10s apart (well above the 1 req/sec steady rate
 * a 60/min limit implies). Expected shapes:
 *   - fixed:            first burst admits up to 60, hard-caps, next burst may catch a fresh window
 *   - sliding-log:       exact — should behave close to fixed but without the boundary-reset gift
 *   - sliding-counter:   smoother than fixed, no full double-burst at the boundary
 *   - token-bucket:      first burst fully absorbed up to capacity (60), then throttles hard
 *   - leaky-bucket:      admits ~1/sec regardless of burst — flattest, most rejects per burst
 */
public class RateLimiterComparisonSimulation extends Simulation {

    private static final String baseUrl = System.getProperty("baseUrl", "http://localhost:8111");
    private static final String partnerId = System.getProperty("partnerId", "partner-free-1");

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(baseUrl)
            .acceptHeader("application/json")
            .header("X-Partner-Id", partnerId);

    private OpenInjectionStep[] burstPattern() {
        return new OpenInjectionStep[]{
                atOnceUsers(80),
                nothingFor(Duration.ofSeconds(10)),
                atOnceUsers(80),
                nothingFor(Duration.ofSeconds(10)),
                atOnceUsers(80)
        };
    }

    private ScenarioBuilder scenarioFor(String algorithm) {
        return scenario(algorithm)
                .exec(
                        http("GET /ob/" + algorithm + "/accounts")
                                .get("/ob/" + algorithm + "/accounts")
                                .check(status().in(200, 429))
                );
    }

    {
        PopulationBuilder fixed = scenarioFor("fixed").injectOpen(burstPattern());
        PopulationBuilder slidingLog = scenarioFor("sliding-log").injectOpen(burstPattern());
        PopulationBuilder slidingCounter = scenarioFor("sliding-counter").injectOpen(burstPattern());
        PopulationBuilder tokenBucket = scenarioFor("token-bucket").injectOpen(burstPattern());
        PopulationBuilder leakyBucket = scenarioFor("leaky-bucket").injectOpen(burstPattern());

        setUp(fixed, slidingLog, slidingCounter, tokenBucket, leakyBucket)
                .protocols(httpProtocol);
        // No global success-rate assertion here on purpose — 429s are an EXPECTED, desired
        // outcome of this test, not a failure. The interesting signal is in Grafana.
    }
}
