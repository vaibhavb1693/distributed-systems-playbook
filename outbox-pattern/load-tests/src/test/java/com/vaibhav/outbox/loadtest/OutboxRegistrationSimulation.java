package com.vaibhav.outbox.loadtest;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Drives POST /api/users/register + PUT /api/users/{id}/kyc-status against whichever
 * user-service instance is running (polling or CDC — same host port, so run this once
 * against each profile and compare the Grafana dashboard between runs).
 *
 * Load profile intentionally exceeds the polling approach's steady-state publish ceiling
 * (batch-size=10 every interval-ms=500 ~= 20 events/sec), so the "Polling Backlog" panel
 * visibly grows then drains under --profile polling, while staying flat under --profile cdc.
 */
public class OutboxRegistrationSimulation extends Simulation {

    private static final String baseUrl = System.getProperty("baseUrl", "http://localhost:8081");

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(baseUrl)
            .acceptHeader("application/json")
            .contentTypeHeader("application/json");

    private final ScenarioBuilder scn = scenario("Register + Update KYC Status")
            .exec(
                    http("Register User")
                            .post("/api/users/register")
                            .body(StringBody(session -> """
                                    {"name":"Gatling User","email":"gatling-%s@finflow.com","phone":"+91-9000000000"}
                                    """.formatted(UUID.randomUUID())))
                            .check(status().is(201), jsonPath("$.id").saveAs("userId"))
            )
            .pause(Duration.ofMillis(100))
            .exec(
                    http("Update KYC Status")
                            .put("/api/users/#{userId}/kyc-status")
                            .body(StringBody("""
                                    {"status":"VERIFIED"}
                                    """))
                            .check(status().is(200))
            );

    {
        setUp(
                scn.injectOpen(
                        rampUsersPerSec(1).to(30).during(Duration.ofSeconds(30)),
                        constantUsersPerSec(30).during(Duration.ofSeconds(90))
                )
        )
                .protocols(httpProtocol)
                .assertions(global().successfulRequests().percent().gt(95.0));
    }
}
