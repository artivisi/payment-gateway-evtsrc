package com.artivisi.paymentgateway.migration;

import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.SiblingVaProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.SiblingVaProjectionRepository;
import com.artivisi.paymentgateway.streams.StoreConstants;
import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Migration Seeder from PostgreSQL to Kafka & RocksDB.
 * 
 * Cold-Start Differentiation:
 * 1. Initial Migration Cold-Start (from PostgreSQL):
 *    - Triggered ONLY when app.migration.seed-from-postgres=true AND local RocksDB state store is empty (0 entries).
 *    - Reads PostgreSQL dump tables and emits initial synthetic ChargeCreatedEvent / SiblingVaRegisteredEvent records.
 * 
 * 2. Subsequent Operational Cold-Starts (from Kafka Changelogs):
 *    - When the container restarts after routine deployment or failure, RocksDB state stores are automatically
 *      re-hydrated by Kafka Streams directly from Kafka broker changelog topics (...-changelog).
 *    - PostgresInitialStateSeeder detects existing state in RocksDB / Kafka and SKIPS PostgreSQL seeding automatically
 *      to prevent duplicate event emission.
 */
@Service
public class PostgresInitialStateSeeder {

    private static final Logger log = LoggerFactory.getLogger(PostgresInitialStateSeeder.class);

    private final ChargeProjectionRepository chargeProjectionRepository;
    private final SiblingVaProjectionRepository siblingVaProjectionRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Value("${app.migration.seed-from-postgres:false}")
    private boolean seedFromPostgresEnabled;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    public PostgresInitialStateSeeder(ChargeProjectionRepository chargeProjectionRepository,
                                      SiblingVaProjectionRepository siblingVaProjectionRepository,
                                      KafkaTemplate<String, String> kafkaTemplate,
                                      ObjectMapper objectMapper) {
        this.chargeProjectionRepository = chargeProjectionRepository;
        this.siblingVaProjectionRepository = siblingVaProjectionRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!seedFromPostgresEnabled) {
            log.debug("app.migration.seed-from-postgres=false (default). Skipping PostgreSQL initial state seeding.");
            return;
        }

        // Automatic Safeguard: Check if RocksDB / Kafka already contains populated state
        if (isAlreadySeeded()) {
            log.info("Kafka Streams RocksDB state store already contains populated state. Skipping PostgreSQL seeding to prevent duplicate event emission.");
            return;
        }

        log.info("app.migration.seed-from-postgres is ENABLED and RocksDB state store is empty. Starting initial PostgreSQL -> Kafka/RocksDB state migration seeder...");
        seedFromPostgres();
    }

    public boolean isAlreadySeeded() {
        if (streamsBuilderFactoryBean != null) {
            try {
                KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
                if (kafkaStreams != null && kafkaStreams.state() == KafkaStreams.State.RUNNING) {
                    ReadOnlyKeyValueStore<String, String> chargeStore = kafkaStreams.store(
                            StoreQueryParameters.fromNameAndType(StoreConstants.CHARGE_STATE_STORE, QueryableStoreTypes.keyValueStore())
                    );
                    return chargeStore.approximateNumEntries() > 0;
                }
            } catch (Exception e) {
                log.debug("Unable to query RocksDB approximate entries during seeding check: {}", e.getMessage());
            }
        }
        return false;
    }

    public int seedFromPostgres() {
        int seededChargeCount = 0;
        Instant now = Instant.now();

        try {
            List<ChargeProjectionEntity> charges = chargeProjectionRepository.findAll();
            log.info("Found {} pre-existing charges in PostgreSQL database to migrate into Kafka & RocksDB", charges.size());

            for (ChargeProjectionEntity charge : charges) {
                // 1. Emit ChargeCreatedEvent to Kafka topic
                ChargeCreatedEvent chargeEvent = new ChargeCreatedEvent(
                        UUID.randomUUID().toString(),
                        charge.getId().toString(),
                        charge.getClientId(),
                        charge.getChargeType(),
                        charge.getRemainingAmount() != null ? charge.getRemainingAmount() : charge.getTotalAmount(),
                        charge.getDescription(),
                        charge.getCreatedAt() != null ? charge.getCreatedAt() : now
                );
                kafkaTemplate.send(chargeEventsTopic, charge.getId().toString(), objectMapper.writeValueAsString(chargeEvent)).get();

                // 2. Fetch and emit SiblingVaRegisteredEvents for this charge
                List<SiblingVaProjectionEntity> vas = siblingVaProjectionRepository.findByChargeId(charge.getId());
                for (SiblingVaProjectionEntity va : vas) {
                    SiblingVaRegisteredEvent vaEvent = new SiblingVaRegisteredEvent(
                            UUID.randomUUID().toString(),
                            charge.getId().toString(),
                            va.getBankCode(),
                            va.getVaNumber(),
                            va.getCreatedAt() != null ? va.getCreatedAt() : now
                    );
                    kafkaTemplate.send(vaEventsTopic, charge.getId().toString(), objectMapper.writeValueAsString(vaEvent)).get();
                }

                seededChargeCount++;
            }

            log.info("PostgreSQL -> Kafka/RocksDB state seeder completed successfully. Seeded {} charges and associated VAs.", seededChargeCount);

        } catch (Exception e) {
            log.error("Failed during PostgreSQL -> Kafka/RocksDB state migration seeding", e);
        }

        return seededChargeCount;
    }
}
