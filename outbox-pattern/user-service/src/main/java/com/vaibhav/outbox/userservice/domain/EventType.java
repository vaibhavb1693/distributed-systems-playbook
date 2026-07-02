package com.vaibhav.outbox.userservice.domain;

public enum EventType {
    USER_REGISTERED("finflow.user.registered"),
    PROFILE_UPDATED("finflow.user.profile.updated"),
    KYC_STATUS_UPDATED("finflow.user.kyc.updated");

    private final String topic;

    EventType(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
