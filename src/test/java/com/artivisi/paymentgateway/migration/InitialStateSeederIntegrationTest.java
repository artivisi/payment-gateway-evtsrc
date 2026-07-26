package com.artivisi.paymentgateway.migration;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InitialStateSeederIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private InitialStateSeeder initialStateSeeder;

    @Test
    @DisplayName("REAL INTEGRATION: Seeding pre-existing VAs processes charges in real Spring Boot container")
    void testSeedInitialState_Positive() {
        List<InitialStateSeeder.SeedChargeItem> legacyCharges = List.of(
                new InitialStateSeeder.SeedChargeItem(
                        "LEGACY-CHG-REAL-001",
                        "CLIENT-REAL-TAZKIA",
                        "CLOSED",
                        new BigDecimal("2500000.00"),
                        "Pre-existing tuition fee",
                        List.of(
                                new InitialStateSeeder.SeedVaItem("MAYBANK", "88099887766"),
                                new InitialStateSeeder.SeedVaItem("BSI", "99099887766")
                        )
                ),
                new InitialStateSeeder.SeedChargeItem(
                        "LEGACY-CHG-REAL-002",
                        "CLIENT-REAL-UNIVERSITAS",
                        "OPEN",
                        new BigDecimal("500000.00"),
                        "Pre-existing open fee",
                        List.of(
                                new InitialStateSeeder.SeedVaItem("CIMB", "77099887766")
                        )
                )
        );

        int count = initialStateSeeder.seedInitialState(legacyCharges);
        assertThat(count).isEqualTo(2);
    }
}
