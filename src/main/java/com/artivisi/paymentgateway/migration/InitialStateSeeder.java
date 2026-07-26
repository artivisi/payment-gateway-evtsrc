package com.artivisi.paymentgateway.migration;

import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Migration & Initial Deployment Seeder Service.
 * 
 * Seeding Mechanics for Pre-Existing VAs & Charges:
 * When migrating pre-existing charges and Virtual Accounts from legacy databases or CSV exports:
 * 1. Reads existing active charges and VAs.
 * 2. Emits synthetic ChargeCreatedEvent and SiblingVaRegisteredEvent into Kafka event topics.
 * 3. Kafka Streams automatically hydrates local off-heap RocksDB state stores (<1ms lookups).
 * 4. Kafka Broker creates changelog topics for container failover recovery.
 * 5. PostgreSQL projection sink asynchronously builds read models.
 */
@Service
public class InitialStateSeeder {

    private static final Logger log = LoggerFactory.getLogger(InitialStateSeeder.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    public InitialStateSeeder(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public record SeedVaItem(
            String bankCode,
            String vaNumber
    ) {}

    public record SeedChargeItem(
            String chargeId,
            String clientId,
            String chargeType,
            BigDecimal totalAmount,
            String description,
            List<SeedVaItem> siblingVas
    ) {}

    /**
     * Seeds pre-existing charges and Virtual Accounts into Kafka & RocksDB.
     */
    public int seedInitialState(List<SeedChargeItem> preExistingCharges) {
        int seededCount = 0;
        Instant now = Instant.now();

        for (SeedChargeItem item : preExistingCharges) {
            try {
                String chargeId = item.chargeId() != null && !item.chargeId().isBlank()
                        ? item.chargeId()
                        : UUID.randomUUID().toString();

                // 1. Publish ChargeCreatedEvent to charge-events topic
                ChargeCreatedEvent chargeEvent = new ChargeCreatedEvent(
                        UUID.randomUUID().toString(),
                        chargeId,
                        item.clientId(),
                        item.chargeType(),
                        item.totalAmount(),
                        item.description(),
                        now
                );
                kafkaTemplate.send(chargeEventsTopic, chargeId, objectMapper.writeValueAsString(chargeEvent)).get();

                // 2. Publish SiblingVaRegisteredEvent for each pre-existing bank VA to va-events topic
                if (item.siblingVas() != null) {
                    for (SeedVaItem va : item.siblingVas()) {
                        SiblingVaRegisteredEvent vaEvent = new SiblingVaRegisteredEvent(
                                UUID.randomUUID().toString(),
                                chargeId,
                                va.bankCode(),
                                va.vaNumber(),
                                now
                        );
                        kafkaTemplate.send(vaEventsTopic, chargeId, objectMapper.writeValueAsString(vaEvent)).get();
                    }
                }

                seededCount++;
                log.info("Seeded pre-existing chargeId {} with {} sibling VAs into Kafka & RocksDB", chargeId, item.siblingVas() != null ? item.siblingVas().size() : 0);

            } catch (Exception e) {
                log.error("Failed to seed pre-existing chargeId: {}", item.chargeId(), e);
            }
        }

        log.info("Initial state seeding completed successfully. Total charges seeded: {}", seededCount);
        return seededCount;
    }
}
