package com.artivisi.paymentgateway;

import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base class for all integration tests.
 *
 * Uses the Singleton Container pattern (recommended by Testcontainers for Spring Boot):
 * - PostgreSQL 18 container starts once and is shared across ALL test classes.
 * - Spring's test context cache reuses the same JDBC URL for all subclasses.
 * - No @Testcontainers/@Container annotations — container lifecycle is manual.
 * - Container is never stopped (JVM shutdown hook handles cleanup).
 *
 * Kafka is provided by Spring EmbeddedKafka (in-process, random port).
 * Kafka Streams auto-startup is disabled (not needed for REST endpoint tests).
 * Flyway is disabled (Hibernate ddl-auto=update manages schema for tests).
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

    static {
        postgres = new PostgreSQLContainer<>("postgres:18-alpine")
                .withDatabaseName("payment_gateway_test")
                .withUsername("testuser")
                .withPassword("testpassword");
        postgres.start();
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

        // Kafka Streams - disabled for REST endpoint tests
        registry.add("spring.kafka.streams.auto-startup", () -> "false");
    }
}
