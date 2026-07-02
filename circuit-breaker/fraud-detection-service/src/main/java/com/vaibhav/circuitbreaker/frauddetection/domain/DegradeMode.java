package com.vaibhav.circuitbreaker.frauddetection.domain;

public enum DegradeMode {
    NONE,
    LATENCY,
    ERROR,
    DOWN
}
