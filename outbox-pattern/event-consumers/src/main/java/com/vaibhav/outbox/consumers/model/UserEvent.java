package com.vaibhav.outbox.consumers.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

// Flexible model: captures all event fields without being coupled to a specific event type.
// Works for USER_REGISTERED, PROFILE_UPDATED, and KYC_STATUS_UPDATED payloads.
@Getter
@NoArgsConstructor
@ToString
public class UserEvent {

    private final Map<String, Object> fields = new HashMap<>();

    @JsonAnySetter
    public void setField(String key, Object value) {
        fields.put(key, value);
    }

    public String getUserId() {
        return (String) fields.get("userId");
    }

    public String get(String key) {
        return fields.getOrDefault(key, "").toString();
    }
}
