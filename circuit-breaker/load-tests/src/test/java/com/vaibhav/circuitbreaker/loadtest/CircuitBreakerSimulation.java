package com.vaibhav.circuitbreaker.loadtest;

import io.gatling.javaapi.core.PopulationBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Two scenarios run concurrently in the same setUp():
 *  - transactionLoad: sustained traffic to transaction-service, ramping past the bulkhead's
 *    maxConcurrentCalls=10 so bulkhead pressure is visible even before any degrade.
 *  - degradeControl: a single virtual user that flips fraud-detection-service into ERROR
 *    mode partway through the run, then resets it — driving the circuit through
 *    CLOSED -> OPEN live, on camera, while transactionLoad keeps generating traffic.
 *
 * All /api/transactions calls return 200 regardless of circuit state (fail-open fallback),
 * so the interesting signal is in Grafana/actuator, not the Gatling pass/fail assertion.
 */
public class CircuitBreakerSimulation extends Simulation {

    private static final String baseUrl = System.getProperty("baseUrl", "http://localhost:8091");
    private static final String fraudAdminUrl = System.getProperty("fraudAdminUrl", "http://localhost:8092");

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(baseUrl)
            .acceptHeader("application/json")
            .contentTypeHeader("application/json");

    private final ScenarioBuilder transactionLoad = scenario("Process Transactions")
            .exec(
                    http("Process Transaction")
                            .post("/api/transactions")
                            .body(StringBody(session -> """
                                    {"payerId":"p1","payeeId":"p2","amount":%d}
                                    """.formatted(1_000 + ThreadLocalRandom.current().nextInt(150_000))))
                            .check(status().is(200))
            );

    private final ScenarioBuilder degradeControl = scenario("Degrade Control")
            .pause(Duration.ofSeconds(15))
            .exec(http("Degrade Fraud Service (ERROR)").post(fraudAdminUrl + "/admin/degrade?mode=error"))
            .pause(Duration.ofSeconds(30))
            .exec(http("Reset Fraud Service").post(fraudAdminUrl + "/admin/degrade?mode=reset"));

    {
        PopulationBuilder load = transactionLoad.injectOpen(
                rampUsersPerSec(1).to(20).during(Duration.ofSeconds(15)),
                constantUsersPerSec(20).during(Duration.ofSeconds(45))
        );
        PopulationBuilder control = degradeControl.injectOpen(atOnceUsers(1));

        setUp(load, control)
                .protocols(httpProtocol)
                .assertions(global().successfulRequests().percent().gt(95.0));
    }
}
