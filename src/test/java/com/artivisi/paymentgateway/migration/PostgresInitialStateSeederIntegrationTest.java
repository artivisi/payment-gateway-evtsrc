package com.artivisi.paymentgateway.migration;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.SiblingVaProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.SiblingVaProjectionRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresInitialStateSeederIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private PostgresInitialStateSeeder postgresInitialStateSeeder;

    @Autowired
    private ChargeProjectionRepository chargeProjectionRepository;

    @Autowired
    private SiblingVaProjectionRepository siblingVaProjectionRepository;

    @Test
    @DisplayName("REAL INTEGRATION: Seeding from real PostgreSQL database populates real repository successfully")
    void testSeedFromPostgres_Positive() {
        Instant now = Instant.now();
        UUID chargeId = UUID.randomUUID();
        UUID vaId = UUID.randomUUID();

        // Insert real entity with UUID into database table
        ChargeProjectionEntity charge = new ChargeProjectionEntity(
                chargeId,
                "CLIENT-REAL-TAZKIA",
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
                vaId,
                chargeId,
                "MAYBANK",
                "88011223344",
                "ACTIVE",
                now,
                now
        );
        siblingVaProjectionRepository.save(va);

        int count = postgresInitialStateSeeder.seedFromPostgres();
        assertThat(count).isGreaterThanOrEqualTo(1);
    }
}
