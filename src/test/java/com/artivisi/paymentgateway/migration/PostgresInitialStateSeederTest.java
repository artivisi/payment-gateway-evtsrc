package com.artivisi.paymentgateway.migration;

import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.SiblingVaProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.SiblingVaProjectionRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@SpringBootTest(properties = {
    "spring.datasource.url=jdbc:h2:mem:testdb_pg_seeder;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.flyway.enabled=false",
    "spring.kafka.streams.auto-startup=false"
})
class PostgresInitialStateSeederTest {

    @Autowired
    private PostgresInitialStateSeeder postgresInitialStateSeeder;

    @Autowired
    private ChargeProjectionRepository chargeProjectionRepository;

    @Autowired
    private SiblingVaProjectionRepository siblingVaProjectionRepository;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("POSITIVE: Seeding from PostgreSQL database populates Kafka and RocksDB successfully")
    void testSeedFromPostgres_Positive() {
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        given(kafkaTemplate.send(anyString(), anyString(), anyString())).willReturn(future);

        Instant now = Instant.now();
        // Insert pre-existing PostgreSQL charge and sibling VA
        ChargeProjectionEntity charge = new ChargeProjectionEntity(
                "PG-MIGRATED-CHG-001",
                "CLIENT-TAZKIA",
                "CLOSED",
                new BigDecimal("1500000.00"),
                BigDecimal.ZERO,
                new BigDecimal("1500000.00"),
                "ACTIVE",
                "Migrated charge",
                now,
                now
        );
        chargeProjectionRepository.save(charge);

        SiblingVaProjectionEntity va = new SiblingVaProjectionEntity(
                "VA-ID-001",
                "PG-MIGRATED-CHG-001",
                "MAYBANK",
                "88011223344",
                "ACTIVE",
                now,
                now
        );
        siblingVaProjectionRepository.save(va);

        int count = postgresInitialStateSeeder.seedFromPostgres();
        assertThat(count).isEqualTo(1);
    }
}
