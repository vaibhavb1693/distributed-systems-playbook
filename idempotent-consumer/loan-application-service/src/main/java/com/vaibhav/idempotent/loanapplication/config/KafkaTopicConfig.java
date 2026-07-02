package com.vaibhav.idempotent.loanapplication.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic loanApplicationSubmittedTopic() {
        return TopicBuilder.name("finflow.loan.application.submitted")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic kycDocumentVerifiedTopic() {
        return TopicBuilder.name("finflow.kyc.document.verified")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic creditScoreReceivedTopic() {
        return TopicBuilder.name("finflow.credit.score.received")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic loanDecisionMadeTopic() {
        return TopicBuilder.name("finflow.loan.decision.made")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
