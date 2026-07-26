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
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
    "spring.datasource.url=jdbc:h2:mem:testdb_callback;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.flyway.enabled=false",
    "spring.kafka.streams.auto-startup=false"
})
@AutoConfigureMockMvc
class BankCallbackControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("POSITIVE: Successful bank payment callback returns HTTP 200 OK with eventId")
    void testSuccessfulPaymentCallback_Positive() throws Exception {
        String chargeId = UUID.randomUUID().toString();
        PaymentCallbackRequest request = new PaymentCallbackRequest(
                chargeId,
                "MAYBANK",
                "88019283019",
                "REF-MAYBANK-001",
                new BigDecimal("500000.00"),
                Instant.now()
        );

        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        given(kafkaTemplate.send(anyString(), eq(chargeId), anyString())).willReturn(future);

        mockMvc.perform(post("/api/v1/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.message").value("Payment command accepted"))
                .andExpect(jsonPath("$.eventId").isNotEmpty());
    }

    @Test
    @DisplayName("NEGATIVE: Payment callback with missing chargeId returns HTTP 400 Bad Request")
    void testPaymentCallbackMissingChargeId_Negative() throws Exception {
        PaymentCallbackRequest request = new PaymentCallbackRequest(
                "", // Blank chargeId
                "MAYBANK",
                "88019283019",
                "REF-MAYBANK-002",
                new BigDecimal("500000.00"),
                Instant.now()
        );

        mockMvc.perform(post("/api/v1/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("NEGATIVE: Payment callback with invalid non-positive amount returns HTTP 400 Bad Request")
    void testPaymentCallbackInvalidAmount_Negative() throws Exception {
        PaymentCallbackRequest request = new PaymentCallbackRequest(
                UUID.randomUUID().toString(),
                "BSI",
                "99019283019",
                "REF-BSI-003",
                new BigDecimal("-10000.00"), // Invalid negative amount
                Instant.now()
        );

        mockMvc.perform(post("/api/v1/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("NEGATIVE: Payment callback with missing bankCode returns HTTP 400 Bad Request")
    void testPaymentCallbackMissingBankCode_Negative() throws Exception {
        PaymentCallbackRequest request = new PaymentCallbackRequest(
                UUID.randomUUID().toString(),
                null, // Missing bankCode
                "88019283019",
                "REF-CIMB-004",
                new BigDecimal("250000.00"),
                Instant.now()
        );

        mockMvc.perform(post("/api/v1/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }
}
