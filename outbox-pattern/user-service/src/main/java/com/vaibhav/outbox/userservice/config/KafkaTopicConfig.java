package com.vaibhav.outbox.userservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic userRegisteredTopic() {
        return TopicBuilder.name("finflow.user.registered")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic profileUpdatedTopic() {
        return TopicBuilder.name("finflow.user.profile.updated")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic kycUpdatedTopic() {
        return TopicBuilder.name("finflow.user.kyc.updated")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
