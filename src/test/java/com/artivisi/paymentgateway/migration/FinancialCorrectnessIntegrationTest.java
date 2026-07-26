package com.artivisi.paymentgateway.migration;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.PaymentProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.PaymentProjectionRepository;
import com.artivisi.paymentgateway.web.api.PaymentCallbackRequest;
import com.artivisi.paymentgateway.web.api.PaymentCallbackResponse;
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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class FinancialCorrectnessIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ChargeProjectionRepository chargeProjectionRepository;

    @Autowired
    private PaymentProjectionRepository paymentProjectionRepository;

    @Test
    @DisplayName("REAL INTEGRATION HARNESS: Pre vs Post-Test Balance Correctness & Idempotency Audit")
    void testFinancialBalanceCorrectness_Positive() throws InterruptedException {
        UUID chargeId = UUID.randomUUID();
        Instant now = Instant.now();
        BigDecimal totalAmount = new BigDecimal("5000000.00");

        // 1. Initial State: Create Charge record in PostgreSQL
        ChargeProjectionEntity initialCharge = new ChargeProjectionEntity(
                chargeId,
                "CLIENT-TEST-AUDIT",
                "CLOSED",
                totalAmount,
                BigDecimal.ZERO,
                totalAmount,
                "ACTIVE",
                "Audit Harness Charge",
                now,
                now
        );
        chargeProjectionRepository.save(initialCharge);

        // Verify Initial Balance
        ChargeProjectionEntity preTestCharge = chargeProjectionRepository.findById(chargeId).orElseThrow();
        assertThat(preTestCharge.getPaidAmount()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(preTestCharge.getRemainingAmount()).isEqualByComparingTo(totalAmount);
        assertThat(preTestCharge.getStatus()).isEqualTo("ACTIVE");

        // 2. Submit Payments via REST Callback API
        BigDecimal payment1Amount = new BigDecimal("2000000.00");
        String bankRef1 = "REF-AUDIT-" + UUID.randomUUID();
        PaymentCallbackRequest request1 = new PaymentCallbackRequest(
                chargeId.toString(),
                "MAYBANK",
                "880998877",
                bankRef1,
                payment1Amount,
                now
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<PaymentCallbackResponse> resp1 = restTemplate.postForEntity(
                "/api/v1/payments",
                new HttpEntity<>(request1, headers),
                PaymentCallbackResponse.class
        );
        assertThat(resp1.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Submit duplicate payment (Idempotency Audit)
        ResponseEntity<PaymentCallbackResponse> resp1Duplicate = restTemplate.postForEntity(
                "/api/v1/payments",
                new HttpEntity<>(request1, headers),
                PaymentCallbackResponse.class
        );
        assertThat(resp1Duplicate.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Submit payment 2 (Remaining balance settlement)
        BigDecimal payment2Amount = new BigDecimal("3000000.00");
        String bankRef2 = "REF-AUDIT-" + UUID.randomUUID();
        PaymentCallbackRequest request2 = new PaymentCallbackRequest(
                chargeId.toString(),
                "BSI",
                "990998877",
                bankRef2,
                payment2Amount,
                now
        );
        ResponseEntity<PaymentCallbackResponse> resp2 = restTemplate.postForEntity(
                "/api/v1/payments",
                new HttpEntity<>(request2, headers),
                PaymentCallbackResponse.class
        );
        assertThat(resp2.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Allow async Kafka Stream & Projection Sink to catch up
        Thread.sleep(1500);

        // 3. Post-Test Balance Correctness Verification
        List<PaymentProjectionEntity> payments = paymentProjectionRepository.findByChargeId(chargeId);
        assertThat(payments).hasSizeGreaterThanOrEqualTo(2);

        ChargeProjectionEntity postTestCharge = chargeProjectionRepository.findById(chargeId).orElseThrow();
        
        // Assert Accounting Invariants
        BigDecimal expectedPaid = payment1Amount.add(payment2Amount);
        assertThat(postTestCharge.getPaidAmount()).isEqualByComparingTo(expectedPaid);
        assertThat(postTestCharge.getRemainingAmount()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(postTestCharge.getStatus()).isEqualTo("FULLY_PAID");
    }
}
