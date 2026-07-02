package com.vaibhav.dlq.kycservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic kycDocumentUploadedTopic() {
        return TopicBuilder.name("finflow.kyc.document.uploaded")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic kycDocumentDlqTopic() {
        return TopicBuilder.name("finflow.kyc.document.dlq")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
