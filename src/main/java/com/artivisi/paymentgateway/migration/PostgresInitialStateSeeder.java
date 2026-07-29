package com.artivisi.paymentgateway.migration;

import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.SiblingVaProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.SiblingVaProjectionRepository;
import com.artivisi.paymentgateway.settlement.ChargeSettlementStore;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Migration Seeder from PostgreSQL to {@link ChargeSettlementStore} & Kafka.
 *
 * Runs once, on {@link ApplicationReadyEvent}, only when {@code app.migration.seed-from-postgres=true}
 * AND the settlement store is empty ({@link #isAlreadySeeded()}). Registers each pre-existing charge
 * and its sibling VAs directly into {@link ChargeSettlementStore} (synchronously, the same call path
 * {@link com.artivisi.paymentgateway.service.ChargeApplicationService#createCharge} uses), then emits
 * the equivalent {@code ChargeCreatedEvent}/{@code SiblingVaRegisteredEvent} records to Kafka purely
 * for {@code PostgresProjectionSink}'s read model.
 *
 * {@link ChargeSettlementStore} is a plain, directly-owned RocksDB directory (see its own Javadoc) --
 * there is no Kafka Streams changelog backing it, so nothing re-hydrates it from Kafka on restart. If
 * {@code app.settlement-store.dir} points at a persistent volume, the directory survives a container
 * restart and {@link #isAlreadySeeded()} correctly skips reseeding. If it does not (ephemeral storage),
 * the store comes back empty and this seeder runs again on every restart -- operators relying on this
 * path for a real migration must give the settlement store a persistent volume.
 */
@Service
public class PostgresInitialStateSeeder {

    private static final Logger log = LoggerFactory.getLogger(PostgresInitialStateSeeder.class);

    private final ChargeProjectionRepository chargeProjectionRepository;
    private final SiblingVaProjectionRepository siblingVaProjectionRepository;
    private final ChargeSettlementStore settlementStore;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.migration.seed-from-postgres:false}")
    private boolean seedFromPostgresEnabled;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    public PostgresInitialStateSeeder(ChargeProjectionRepository chargeProjectionRepository,
                                      SiblingVaProjectionRepository siblingVaProjectionRepository,
                                      ChargeSettlementStore settlementStore,
                                      KafkaTemplate<String, String> kafkaTemplate,
                                      ObjectMapper objectMapper) {
        this.chargeProjectionRepository = chargeProjectionRepository;
        this.siblingVaProjectionRepository = siblingVaProjectionRepository;
        this.settlementStore = settlementStore;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!seedFromPostgresEnabled) {
            log.debug("app.migration.seed-from-postgres=false (default). Skipping PostgreSQL initial state seeding.");
            return;
        }

        // Automatic Safeguard: Check if the settlement store already contains populated state
        if (isAlreadySeeded()) {
            log.info("ChargeSettlementStore already contains populated state. Skipping PostgreSQL seeding to prevent duplicate event emission.");
            return;
        }

        log.info("app.migration.seed-from-postgres is ENABLED and the settlement store is empty. Starting initial PostgreSQL -> Kafka/RocksDB state migration seeder...");
        seedFromPostgres();
    }

    public boolean isAlreadySeeded() {
        try {
            return settlementStore.approximateKeyCount() > 0;
        } catch (Exception e) {
            log.debug("Unable to query ChargeSettlementStore approximate key count during seeding check: {}", e.getMessage());
            return false;
        }
    }

    public int seedFromPostgres() {
        int seededChargeCount = 0;
        Instant now = Instant.now();

        try {
            List<ChargeProjectionEntity> charges = chargeProjectionRepository.findAll();
            log.info("Found {} pre-existing charges in PostgreSQL database to migrate into the settlement store & Kafka", charges.size());

            for (ChargeProjectionEntity charge : charges) {
                Instant chargeTimestamp = charge.getCreatedAt() != null ? charge.getCreatedAt() : now;
                var totalAmount = charge.getRemainingAmount() != null ? charge.getRemainingAmount() : charge.getTotalAmount();

                // 1. Register synchronously into the settlement store (source of truth).
                settlementStore.registerCharge(charge.getId().toString(), charge.getClientId(),
                        charge.getChargeType(), totalAmount, charge.getDescription(), chargeTimestamp);

                // 2. Emit ChargeCreatedEvent for the read model.
                ChargeCreatedEvent chargeEvent = new ChargeCreatedEvent(
                        UUID.randomUUID().toString(),
                        charge.getId().toString(),
                        charge.getClientId(),
                        charge.getChargeType(),
                        totalAmount,
                        charge.getDescription(),
                        chargeTimestamp
                );
                kafkaTemplate.send(chargeEventsTopic, charge.getId().toString(), objectMapper.writeValueAsString(chargeEvent)).get();

                // 3. Fetch, register, and emit SiblingVaRegisteredEvents for this charge
                List<SiblingVaProjectionEntity> vas = siblingVaProjectionRepository.findByChargeId(charge.getId());
                for (SiblingVaProjectionEntity va : vas) {
                    settlementStore.registerVa(va.getBankCode(), va.getVaNumber(), charge.getId().toString());

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

            log.info("PostgreSQL -> settlement store & Kafka seeder completed successfully. Seeded {} charges and associated VAs.", seededChargeCount);

        } catch (Exception e) {
            log.error("Failed during PostgreSQL -> settlement store & Kafka migration seeding", e);
        }

        return seededChargeCount;
    }
}
