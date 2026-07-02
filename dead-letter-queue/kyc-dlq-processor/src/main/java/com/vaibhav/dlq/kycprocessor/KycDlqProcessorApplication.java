package com.vaibhav.dlq.kycprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KycDlqProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(KycDlqProcessorApplication.class, args);
    }
}
