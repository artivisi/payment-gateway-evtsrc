package com.artivisi.paymentgateway.migration;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import com.artivisi.paymentgateway.TestSupport;
import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.PaymentProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.PaymentProjectionRepository;
import com.artivisi.paymentgateway.web.api.CreateChargeRequest;
import com.artivisi.paymentgateway.web.api.CreateChargeResponse;
import com.artivisi.paymentgateway.web.api.PaymentCallbackRequest;
import com.artivisi.paymentgateway.web.api.PaymentCallbackResponse;
import com.artivisi.paymentgateway.web.api.PaymentOutcome;
import com.artivisi.paymentgateway.web.api.SiblingVaRequest;
import org.awaitility.Awaitility;
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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the real event-sourced path end to end: create charge + sibling VAs via REST,
 * settle it via two pay-via-any-bank sibling VAs (including a replayed bankReference), and
 * assert both the RocksDB-derived state and the PostgreSQL projection agree on the outcome.
 */
class FinancialCorrectnessIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ChargeProjectionRepository chargeProjectionRepository;

    @Autowired
    private PaymentProjectionRepository paymentProjectionRepository;

    @Test
    @DisplayName("REAL INTEGRATION HARNESS: Pay-via-any-bank settlement across sibling VAs, with a replayed reference, is idempotent and balances correctly")
    void testFinancialBalanceCorrectness_Positive() {
        BigDecimal totalAmount = new BigDecimal("5000000.00");
        String vaMaybank = "MB-" + UUID.randomUUID().toString().substring(0, 8);
        String vaBsi = "BSI-" + UUID.randomUUID().toString().substring(0, 8);

        CreateChargeRequest chargeRequest = new CreateChargeRequest(
                "CLIENT-TEST-AUDIT",
                "CLOSED",
                totalAmount,
                "Audit Harness Charge",
                List.of(new SiblingVaRequest("MAYBANK", vaMaybank), new SiblingVaRequest("BSI", vaBsi))
        );
        ResponseEntity<CreateChargeResponse> chargeResponse = restTemplate.postForEntity(
                "/api/v1/charges", chargeRequest, CreateChargeResponse.class);
        assertThat(chargeResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String chargeId = chargeResponse.getBody().chargeId();

        // Wait for the charge and both sibling VAs to hydrate into RocksDB before paying.
        // charge-events and va-events are independently-lagging streams: the charge can report
        // ACTIVE before either sibling VA is resolvable in va-registry-store.
        TestSupport.awaitChargeStatus(restTemplate, chargeId, "ACTIVE");
        TestSupport.awaitVaResolvable(restTemplate, "MAYBANK", vaMaybank);
        TestSupport.awaitVaResolvable(restTemplate, "BSI", vaBsi);

        // 1. First payment via the Maybank sibling VA.
        BigDecimal payment1Amount = new BigDecimal("2000000.00");
        String bankRef1 = "REF-AUDIT-" + UUID.randomUUID();
        PaymentCallbackRequest request1 = new PaymentCallbackRequest(
                "MAYBANK", vaMaybank, bankRef1, payment1Amount, Instant.now());

        ResponseEntity<PaymentCallbackResponse> resp1 = postCallback(request1);
        assertThat(resp1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp1.getBody().status()).isEqualTo(PaymentOutcome.ACCEPTED.name());

        // Submit the exact same callback again (Idempotency Audit) — must not be double-counted.
        ResponseEntity<PaymentCallbackResponse> resp1Duplicate = postCallback(request1);
        assertThat(resp1Duplicate.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp1Duplicate.getBody().status()).isEqualTo(PaymentOutcome.DUPLICATE.name());

        // 2. Second payment via the BSI sibling VA settles the remaining balance.
        BigDecimal payment2Amount = new BigDecimal("3000000.00");
        String bankRef2 = "REF-AUDIT-" + UUID.randomUUID();
        PaymentCallbackRequest request2 = new PaymentCallbackRequest(
                "BSI", vaBsi, bankRef2, payment2Amount, Instant.now());
        ResponseEntity<PaymentCallbackResponse> resp2 = postCallback(request2);
        assertThat(resp2.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp2.getBody().status()).isEqualTo(PaymentOutcome.ACCEPTED.name());

        // RocksDB (authoritative): the charge reaches PAID once cumulativePaid >= totalAmount.
        TestSupport.awaitChargeStatus(restTemplate, chargeId, "PAID");

        // PostgreSQL projection (async): assert it converges to the same balance.
        UUID chargeUuid = UUID.fromString(chargeId);
        BigDecimal expectedPaid = payment1Amount.add(payment2Amount);
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    List<PaymentProjectionEntity> payments = paymentProjectionRepository.findByChargeId(chargeUuid);
                    assertThat(payments).hasSize(2);

                    ChargeProjectionEntity postTestCharge = chargeProjectionRepository.findById(chargeUuid).orElseThrow();
                    assertThat(postTestCharge.getPaidAmount()).isEqualByComparingTo(expectedPaid);
                    assertThat(postTestCharge.getRemainingAmount()).isEqualByComparingTo(BigDecimal.ZERO);
                    assertThat(postTestCharge.getStatus()).isEqualTo("FULLY_PAID");
                });
    }

    private ResponseEntity<PaymentCallbackResponse> postCallback(PaymentCallbackRequest request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return restTemplate.postForEntity(
                "/api/v1/payments", new HttpEntity<>(request, headers), PaymentCallbackResponse.class);
    }
}
