package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BankCallbackControllerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    @DisplayName("REAL INTEGRATION: Positive payment callback returns HTTP 200 via real Spring HTTP server")
    void testPaymentCallback_Positive() {
        String chargeId = UUID.randomUUID().toString();
        String bankReference = "REF-" + UUID.randomUUID();

        PaymentCallbackRequest request = new PaymentCallbackRequest(
                chargeId,
                "MAYBANK",
                "8801928371",
                bankReference,
                new BigDecimal("2500000.00"),
                Instant.now()
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<PaymentCallbackRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<PaymentCallbackResponse> response = restTemplate.postForEntity("/api/v1/payments", entity, PaymentCallbackResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("SUCCESS");
        assertThat(response.getBody().eventId()).isNotBlank();
    }

    @Test
    @DisplayName("REAL INTEGRATION: Bank callback with missing chargeId returns HTTP 400 Bad Request")
    void testPaymentCallbackMissingChargeId_Negative() {
        PaymentCallbackRequest request = new PaymentCallbackRequest(
                "",
                "BSI",
                "9901928371",
                "REF-INVALID-1",
                new BigDecimal("100000.00"),
                Instant.now()
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<PaymentCallbackRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/v1/payments", entity, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("REAL INTEGRATION: Bank callback with negative amount returns HTTP 400 Bad Request")
    void testPaymentCallbackNegativeAmount_Negative() {
        PaymentCallbackRequest request = new PaymentCallbackRequest(
                UUID.randomUUID().toString(),
                "CIMB",
                "7701928371",
                "REF-INVALID-2",
                new BigDecimal("-50000.00"),
                Instant.now()
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<PaymentCallbackRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/v1/payments", entity, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
