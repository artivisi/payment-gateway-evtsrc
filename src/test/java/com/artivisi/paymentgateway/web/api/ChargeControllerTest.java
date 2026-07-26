package com.artivisi.paymentgateway.web.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
    "spring.datasource.url=jdbc:h2:mem:testdb_charge;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.flyway.enabled=false",
    "spring.kafka.streams.auto-startup=false"
})
@AutoConfigureMockMvc
class ChargeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("POSITIVE: Successful charge creation on /api/charges returns HTTP 201 Created with chargeId")
    void testCreateCharge_Positive() throws Exception {
        CreateChargeRequest request = new CreateChargeRequest(
                "CLIENT-TAZKIA-001",
                "CLOSED",
                new BigDecimal("1500000.00"),
                "Tuition fee 2026",
                List.of(
                        new SiblingVaRequest("MAYBANK", "88019283019"),
                        new SiblingVaRequest("BSI", "99019283019")
                )
        );

        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        given(kafkaTemplate.send(anyString(), anyString(), anyString())).willReturn(future);

        mockMvc.perform(post("/api/charges")
                .header("X-Client-Id", "CLIENT-TAZKIA-001")
                .header("X-Client-Secret", "secret123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.chargeId").isNotEmpty())
                .andExpect(jsonPath("$.message").value("Charge and sibling VAs created"));
    }

    @Test
    @DisplayName("POSITIVE: Successful charge cancellation on POST /api/charges/{id}/cancel returns HTTP 200 OK")
    void testCancelCharge_Positive() throws Exception {
        String chargeId = UUID.randomUUID().toString();
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        given(kafkaTemplate.send(anyString(), anyString(), anyString())).willReturn(future);

        mockMvc.perform(post("/api/charges/{id}/cancel", chargeId)
                .header("X-Client-Id", "CLIENT-TAZKIA-001")
                .header("X-Client-Secret", "secret123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.chargeId").value(chargeId))
                .andExpect(jsonPath("$.message").value("Charge cancellation requested"));
    }

    @Test
    @DisplayName("NEGATIVE: Fetching non-existent charge on GET /api/charges/{id} returns HTTP 404 Not Found")
    void testGetChargeNotFound_Negative() throws Exception {
        String nonExistentId = UUID.randomUUID().toString();

        mockMvc.perform(get("/api/charges/{id}", nonExistentId)
                .header("X-Client-Id", "CLIENT-TAZKIA-001")
                .header("X-Client-Secret", "secret123"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("NEGATIVE: Charge creation with missing clientId returns HTTP 400 Bad Request")
    void testCreateChargeMissingClientId_Negative() throws Exception {
        CreateChargeRequest request = new CreateChargeRequest(
                "", // Missing clientId
                "CLOSED",
                new BigDecimal("1500000.00"),
                "Tuition fee 2026",
                List.of()
        );

        mockMvc.perform(post("/api/charges")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("NEGATIVE: Charge creation with non-positive total amount returns HTTP 400 Bad Request")
    void testCreateChargeInvalidTotalAmount_Negative() throws Exception {
        CreateChargeRequest request = new CreateChargeRequest(
                "CLIENT-TAZKIA-001",
                "CLOSED",
                new BigDecimal("0.00"), // Invalid zero amount
                "Invalid charge",
                List.of()
        );

        mockMvc.perform(post("/api/charges")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }
}
