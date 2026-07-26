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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ChargeControllerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    @DisplayName("REAL INTEGRATION: Create charge via HTTP POST returns HTTP 201 Created and outputs chargeId")
    void testCreateCharge_Positive() {
        CreateChargeRequest request = new CreateChargeRequest(
                "CLIENT-REAL-001",
                "CLOSED",
                new BigDecimal("1500000.00"),
                "Tuition fee charge test",
                List.of(
                        new SiblingVaRequest("MAYBANK", "8801928371"),
                        new SiblingVaRequest("BSI", "9901928371")
                )
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<CreateChargeRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<CreateChargeResponse> response = restTemplate.postForEntity("/api/charges", entity, CreateChargeResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("SUCCESS");
        assertThat(response.getBody().chargeId()).isNotBlank();
    }

    @Test
    @DisplayName("REAL INTEGRATION: Create charge with missing chargeType returns HTTP 400 Bad Request")
    void testCreateChargeMissingType_Negative() {
        CreateChargeRequest request = new CreateChargeRequest(
                "CLIENT-REAL-002",
                "",
                new BigDecimal("500000.00"),
                "Missing charge type",
                null
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<CreateChargeRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/charges", entity, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("REAL INTEGRATION: Cancel charge via HTTP POST returns HTTP 200 OK")
    void testCancelCharge_Positive() {
        String chargeId = UUID.randomUUID().toString();
        ResponseEntity<CreateChargeResponse> response = restTemplate.postForEntity("/api/charges/" + chargeId + "/cancel", null, CreateChargeResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("SUCCESS");
    }
}
