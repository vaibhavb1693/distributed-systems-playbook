package com.vaibhav.dlq.loadtest;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Weighted mix of simulate= values, roughly modeling a real KYC failure distribution:
 * most documents succeed, a modest slice hits transient vendor/DB issues, a smaller slice
 * is genuinely bad (permanent).
 *
 * Deliberately modest load (2-3 req/s): kyc-service's transient-failure retry is
 * synchronous/blocking (see dead-letter-queue/README.md's documented trade-off) — with no
 * listener concurrency configured, each transient failure ties up the consumer thread for
 * up to 7s (1s+2s+4s backoff). At even this modest rate you should see kyc-service's consumer
 * lag behind during the run and catch back up after — that lag IS the demo, not a bug.
 */
public class KycUploadSimulation extends Simulation {

    private static final String baseUrl = System.getProperty("baseUrl", "http://localhost:8121");

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(baseUrl)
            .acceptHeader("application/json")
            .contentTypeHeader("application/json");

    private final ScenarioBuilder scn = scenario("Upload KYC Documents")
            .exec(
                    http("Upload Document")
                            .post("/api/kyc/documents/upload")
                            .body(StringBody(session -> """
                                    {"userId":"gatling-user-%s","documentType":"PASSPORT","simulate":"%s"}
                                    """.formatted(java.util.UUID.randomUUID(), pickSimulateValue())))
                            .check(status().is(202))
            );

    private static String pickSimulateValue() {
        int roll = ThreadLocalRandom.current().nextInt(100);
        if (roll < 70) return "NONE";                    // 70% happy path
        if (roll < 80) return "VENDOR_TIMEOUT";           // 10% transient
        if (roll < 85) return "DB_ERROR";                 // 5% transient
        if (roll < 90) return "CORRUPTED";                // 5% permanent
        if (roll < 95) return "UNSUPPORTED_FORMAT";       // 5% permanent
        if (roll < 98) return "FRAUD_FLAG";               // 3% permanent
        return "OCR_FAILURE";                             // 2% permanent
    }

    {
        setUp(
                scn.injectOpen(
                        rampUsersPerSec(1).to(3).during(Duration.ofSeconds(15)),
                        constantUsersPerSec(3).during(Duration.ofSeconds(30))
                )
        )
                .protocols(httpProtocol)
                .assertions(global().successfulRequests().percent().gt(95.0));
    }
}
