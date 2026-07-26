package com.artivisi.paymentgateway.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@SpringBootTest(properties = {
    "spring.datasource.url=jdbc:h2:mem:testdb_seeder;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.flyway.enabled=false",
    "spring.kafka.streams.auto-startup=false"
})
class InitialStateSeederTest {

    @Autowired
    private InitialStateSeeder initialStateSeeder;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("POSITIVE: Seeding pre-existing VAs and charges publishes events to Kafka successfully")
    void testSeedInitialState_Positive() {
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        given(kafkaTemplate.send(anyString(), anyString(), anyString())).willReturn(future);

        List<InitialStateSeeder.SeedChargeItem> legacyCharges = List.of(
                new InitialStateSeeder.SeedChargeItem(
                        "LEGACY-CHG-001",
                        "CLIENT-TAZKIA",
                        "CLOSED",
                        new BigDecimal("2500000.00"),
                        "Pre-existing tuition fee",
                        List.of(
                                new InitialStateSeeder.SeedVaItem("MAYBANK", "88099887766"),
                                new InitialStateSeeder.SeedVaItem("BSI", "99099887766")
                        )
                ),
                new InitialStateSeeder.SeedChargeItem(
                        "LEGACY-CHG-002",
                        "CLIENT-UNIVERSITAS",
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
