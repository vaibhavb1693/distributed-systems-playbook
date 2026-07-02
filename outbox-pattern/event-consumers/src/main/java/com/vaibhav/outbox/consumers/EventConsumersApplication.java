package com.vaibhav.outbox.consumers;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EventConsumersApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventConsumersApplication.class, args);
    }
}
