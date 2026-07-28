package com.artivisi.paymentgateway.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Explicit topic provisioning.
 *
 * Without these beans, every topic referenced by producers/consumers auto-creates with the
 * broker default of 1 partition regardless of {@code num.stream.threads} — see
 * benchmark-remediation-guideline.md F4/G3. Partition count is driven by
 * {@code app.kafka.partitions} so it stays in sync with
 * {@code spring.kafka.streams.properties.num.stream.threads} and
 * {@code spring.kafka.listener.concurrency}.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.partitions:6}")
    private int partitions;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    @Value("${app.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    @Value("${app.topics.reconciliation-events:reconciliation-events}")
    private String reconciliationEventsTopic;

    @Value("${app.topics.webhook-events:webhook-events}")
    private String webhookEventsTopic;

    @Bean
    public NewTopic chargeEventsTopic() {
        return TopicBuilder.name(chargeEventsTopic).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic vaEventsTopic() {
        return TopicBuilder.name(vaEventsTopic).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name(paymentEventsTopic).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic reconciliationEventsTopic() {
        return TopicBuilder.name(reconciliationEventsTopic).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic webhookEventsTopic() {
        return TopicBuilder.name(webhookEventsTopic).partitions(partitions).replicas(1).build();
    }
}
