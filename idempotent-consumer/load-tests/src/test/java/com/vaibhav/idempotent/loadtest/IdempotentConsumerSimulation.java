package com.vaibhav.idempotent.loadtest;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Each virtual user generates one fresh loanId and drives it through all 4 lifecycle events,
 * each with times=3 — so every single "logical" event is redelivered 3x for real, at load,
 * not just in a one-off curl demo. The dashboard should show duplicate rate ~= 2x processed
 * rate for every strategy (3 deliveries - 1 real = 2 duplicates each).
 */
public class IdempotentConsumerSimulation extends Simulation {

    private static final String baseUrl = System.getProperty("baseUrl", "http://localhost:8101");
    private static final int redeliveries = Integer.getInteger("redeliveries", 3);

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(baseUrl)
            .acceptHeader("application/json")
            .contentTypeHeader("application/json");

    private final ScenarioBuilder scn = scenario("Loan Lifecycle With Redelivery")
            .exec(session -> session.set("loanId", UUID.randomUUID().toString()))
            .exec(
                    http("Submit Application")
                            .post("/api/loans/#{loanId}/submit-application?times=" + redeliveries)
                            .body(StringBody("""
                                    {"applicantName":"Gatling User","amount":500000}
                                    """))
                            .check(status().is(202))
            )
            .pause(Duration.ofMillis(200))
            .exec(
                    http("KYC Verified")
                            .post("/api/loans/#{loanId}/kyc-verified?expectedVersion=0&times=" + redeliveries)
                            .check(status().is(202))
            )
            .pause(Duration.ofMillis(200))
            .exec(
                    http("Credit Score Received")
                            .post("/api/loans/#{loanId}/credit-score?times=" + redeliveries)
                            .body(StringBody("""
                                    {"creditScore":720}
                                    """))
                            .check(status().is(202))
            )
            .pause(Duration.ofMillis(200))
            .exec(
                    http("Decision Made")
                            .post("/api/loans/#{loanId}/decision?times=" + redeliveries)
                            .body(StringBody("""
                                    {"decision":"APPROVED"}
                                    """))
                            .check(status().is(202))
            );

    {
        setUp(
                scn.injectOpen(
                        rampUsersPerSec(1).to(10).during(Duration.ofSeconds(20)),
                        constantUsersPerSec(10).during(Duration.ofSeconds(40))
                )
        )
                .protocols(httpProtocol)
                .assertions(global().successfulRequests().percent().gt(95.0));
    }
}
