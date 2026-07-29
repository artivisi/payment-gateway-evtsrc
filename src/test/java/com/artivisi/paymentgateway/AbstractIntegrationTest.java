package com.artivisi.paymentgateway;

import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Base class for all integration tests.
 *
 * Uses the Singleton Container pattern (recommended by Testcontainers for Spring Boot):
 * - PostgreSQL 18 container starts once and is shared across ALL test classes.
 * - Spring's test context cache reuses the same JDBC URL for all subclasses.
 * - No @Testcontainers/@Container annotations — container lifecycle is manual.
 * - Container is never stopped (JVM shutdown hook handles cleanup).
 *
 * Kafka is provided by Spring EmbeddedKafka (in-process, random port, fresh broker/topic
 * identity every JVM run) — used only to broadcast already-decided facts to
 * PostgresProjectionSink's read model; it is not where correctness decisions are made (see
 * ChargeSettlementStore). ChargeSettlementStore's own RocksDB directory is pointed at a fresh
 * temp directory per JVM run (below) rather than the production default (./target/settlement-store):
 * that path must not be shared across separate test runs' state.
 *
 * Charge/VA registration and payment application are synchronous against ChargeSettlementStore —
 * no hydration lag to poll for. Flyway is disabled (Hibernate ddl-auto=update manages schema for
 * tests).
 *
 * Requires Docker daemon accessible on the host.
 */
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {"payment-events", "charge-events", "va-events"}
)
public abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer<?> postgres;
    static final Path settlementStoreDir;

    static {
        postgres = new PostgreSQLContainer<>("postgres:18-alpine")
                .withDatabaseName("payment_gateway_test")
                .withUsername("testuser")
                .withPassword("testpassword");
        postgres.start();

        try {
            settlementStoreDir = Files.createTempDirectory("evtsrc-settlement-store-test-");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // PostgreSQL - Testcontainers dynamic port
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.driver-class-name", postgres::getDriverClassName);

        // JPA - auto-create schema for tests
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "update");

        // Flyway - disabled for tests (Hibernate manages schema)
        registry.add("spring.flyway.enabled", () -> "false");

        // ChargeSettlementStore's own RocksDB directory, fresh per JVM run.
        registry.add("app.settlement-store.dir", () -> settlementStoreDir.toString());

        // Match KafkaTopicConfig's NewTopic partition count to @EmbeddedKafka's pre-created
        // 1-partition topics.
        registry.add("app.kafka.partitions", () -> "1");
    }
}
