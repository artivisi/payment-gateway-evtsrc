package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import com.artivisi.paymentgateway.TestSupport;
import com.artivisi.paymentgateway.projection.entity.PaymentProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.PaymentProjectionRepository;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class BankCallbackControllerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PaymentProjectionRepository paymentProjectionRepository;

    private String createChargeAndAwaitHydration(String bankCode, String vaNumber, BigDecimal totalAmount) {
        CreateChargeRequest chargeRequest = new CreateChargeRequest(
                "CLIENT-CALLBACK-TEST",
                "CLOSED",
                totalAmount,
                "Callback integration test charge",
                List.of(new SiblingVaRequest(bankCode, vaNumber))
        );

        ResponseEntity<CreateChargeResponse> chargeResponse = restTemplate.postForEntity(
                "/api/v1/charges", chargeRequest, CreateChargeResponse.class);
        assertThat(chargeResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String chargeId = chargeResponse.getBody().chargeId();

        TestSupport.awaitChargeStatus(restTemplate, chargeId, "ACTIVE");
        TestSupport.awaitVaResolvable(restTemplate, bankCode, vaNumber);
        return chargeId;
    }

    @Test
    @DisplayName("REAL INTEGRATION: Payment callback for a registered VA returns ACCEPTED")
    void testPaymentCallback_Positive() {
        String vaNumber = "8801928371";
        createChargeAndAwaitHydration("MAYBANK", vaNumber, new BigDecimal("2500000.00"));

        PaymentCallbackRequest request = new PaymentCallbackRequest(
                "MAYBANK",
                vaNumber,
                "REF-" + UUID.randomUUID(),
                new BigDecimal("2500000.00"),
                Instant.now()
        );

        ResponseEntity<PaymentCallbackResponse> response = postCallback(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo(PaymentOutcome.ACCEPTED.name());
        assertThat(response.getBody().eventId()).isNotBlank();
    }

    @Test
    @DisplayName("REAL INTEGRATION: Replaying the same bankReference returns DUPLICATE, not a second event")
    void testPaymentCallback_Duplicate() {
        String vaNumber = "8801928372";
        String chargeId = createChargeAndAwaitHydration("MAYBANK", vaNumber, new BigDecimal("2500000.00"));

        PaymentCallbackRequest request = new PaymentCallbackRequest(
                "MAYBANK",
                vaNumber,
                "REF-" + UUID.randomUUID(),
                new BigDecimal("1000000.00"),
                Instant.now()
        );

        ResponseEntity<PaymentCallbackResponse> first = postCallback(request);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody().status()).isEqualTo(PaymentOutcome.ACCEPTED.name());
        String firstEventId = first.getBody().eventId();

        TestSupport.awaitCumulativePaidAtLeast(restTemplate, chargeId, request.amount());

        ResponseEntity<PaymentCallbackResponse> duplicate = postCallback(request);
        assertThat(duplicate.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(duplicate.getBody().status()).isEqualTo(PaymentOutcome.DUPLICATE.name());
        assertThat(duplicate.getBody().eventId()).isEqualTo(firstEventId);
    }

    @Test
    @DisplayName("REAL INTEGRATION: Payment callback against an unregistered VA returns HTTP 404 REJECTED_INVALID_VA")
    void testPaymentCallback_UnknownVa_Negative() {
        PaymentCallbackRequest request = new PaymentCallbackRequest(
                "MAYBANK",
                "NO-SUCH-VA-" + UUID.randomUUID(),
                "REF-" + UUID.randomUUID(),
                new BigDecimal("100000.00"),
                Instant.now()
        );

        ResponseEntity<PaymentCallbackResponse> response = postCallback(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody().status()).isEqualTo(PaymentOutcome.REJECTED_INVALID_VA.name());
    }

    @Test
    @DisplayName("REAL INTEGRATION: A payment against an already-PAID charge is rejected, not silently applied")
    void testPaymentCallback_ChargeAlreadyClosed_Negative() {
        String vaNumber = "8801928373";
        BigDecimal totalAmount = new BigDecimal("500000.00");
        String chargeId = createChargeAndAwaitHydration("BSI", vaNumber, totalAmount);

        PaymentCallbackRequest settlingPayment = new PaymentCallbackRequest(
                "BSI", vaNumber, "REF-" + UUID.randomUUID(), totalAmount, Instant.now());
        ResponseEntity<PaymentCallbackResponse> settling = postCallback(settlingPayment);
        assertThat(settling.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(settling.getBody().status()).isEqualTo(PaymentOutcome.ACCEPTED.name());

        TestSupport.awaitChargeStatus(restTemplate, chargeId, "PAID");

        String racedBankReference = "REF-" + UUID.randomUUID();
        PaymentCallbackRequest racedPayment = new PaymentCallbackRequest(
                "BSI", vaNumber, racedBankReference, totalAmount, Instant.now());
        ResponseEntity<PaymentCallbackResponse> raced = postCallback(racedPayment);

        assertThat(raced.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(raced.getBody().status()).isEqualTo(PaymentOutcome.REJECTED_CHARGE_CLOSED.name());

        // Confirm the rejection is not just an HTTP-layer opinion: a DoubleSettlementDetectedEvent
        // must actually reach the Postgres projection (is_double_settlement = true) so ops can see it.
        UUID chargeUuid = UUID.fromString(chargeId);
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    List<PaymentProjectionEntity> projected = paymentProjectionRepository.findByChargeId(chargeUuid);
                    assertThat(projected)
                            .anyMatch(entity -> entity.isDoubleSettlement()
                                    && entity.getBankReference().equals(racedBankReference));
                });
    }

    @Test
    @DisplayName("REAL INTEGRATION: Two concurrent full payments on one CLOSED charge apply exactly once, the other is flagged as a double settlement")
    void testPaymentCallback_ConcurrentFullSettlement_ExactlyOneApplied() throws Exception {
        String vaNumber = "8801928374";
        BigDecimal totalAmount = new BigDecimal("750000.00");
        String chargeId = createChargeAndAwaitHydration("CIMB", vaNumber, totalAmount);

        String bankReference1 = "REF-RACE-1-" + UUID.randomUUID();
        String bankReference2 = "REF-RACE-2-" + UUID.randomUUID();

        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Callable<ResponseEntity<PaymentCallbackResponse>> call1 = () -> {
                startLatch.await();
                return postCallback(new PaymentCallbackRequest("CIMB", vaNumber, bankReference1, totalAmount, Instant.now()));
            };
            Callable<ResponseEntity<PaymentCallbackResponse>> call2 = () -> {
                startLatch.await();
                return postCallback(new PaymentCallbackRequest("CIMB", vaNumber, bankReference2, totalAmount, Instant.now()));
            };

            Future<ResponseEntity<PaymentCallbackResponse>> future1 = executor.submit(call1);
            Future<ResponseEntity<PaymentCallbackResponse>> future2 = executor.submit(call2);
            startLatch.countDown();

            ResponseEntity<PaymentCallbackResponse> response1 = future1.get();
            ResponseEntity<PaymentCallbackResponse> response2 = future2.get();

            // Every response must be a well-formed outcome, never a silent double-apply.
            assertThat(List.of(response1.getStatusCode(), response2.getStatusCode()))
                    .allMatch(status -> status == HttpStatus.OK || status == HttpStatus.BAD_REQUEST);
        } finally {
            executor.shutdown();
        }

        // Source of truth: ChargeSettlementStore, via GET /api/v1/charges/{id}. Whichever of the
        // two concurrent requests won the getForUpdate lock on this chargeId, the charge must
        // settle exactly once -- never double-counted.
        TestSupport.awaitChargeStatus(restTemplate, chargeId, "PAID");
        ResponseEntity<String> finalCharge = restTemplate.getForEntity("/api/v1/charges/" + chargeId, String.class);
        assertThat(finalCharge.getStatusCode()).isEqualTo(HttpStatus.OK);
        BigDecimal cumulativePaid = extractCumulativePaid(finalCharge.getBody());
        assertThat(cumulativePaid).isEqualByComparingTo(totalAmount);

        // The loser of the race must be observable as a flagged double settlement in the
        // Postgres projection, whichever of the two concurrent requests lost the getForUpdate lock.
        UUID chargeUuid = UUID.fromString(chargeId);
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    List<PaymentProjectionEntity> projected = paymentProjectionRepository.findByChargeId(chargeUuid);
                    assertThat(projected).anyMatch(PaymentProjectionEntity::isDoubleSettlement);
                });
    }

    @Test
    @DisplayName("REAL INTEGRATION: Settling a CLOSED charge marks the paying VA PAID and its sibling CANCELLED -- both stop resolving on inquiry")
    void testPaymentCallback_ClosedChargeSettlement_PaidVaAndCancelledSiblingBothStopResolvingOnInquiry() {
        String payingVaNumber = "8801928375";
        String siblingVaNumber = "8801928376";
        BigDecimal totalAmount = new BigDecimal("650000.00");

        CreateChargeRequest chargeRequest = new CreateChargeRequest(
                "CLIENT-CALLBACK-TEST",
                "CLOSED",
                totalAmount,
                "Sibling VA settlement test charge",
                List.of(new SiblingVaRequest("BSI", payingVaNumber), new SiblingVaRequest("MAYBANK", siblingVaNumber))
        );
        ResponseEntity<CreateChargeResponse> chargeResponse = restTemplate.postForEntity(
                "/api/v1/charges", chargeRequest, CreateChargeResponse.class);
        assertThat(chargeResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String chargeId = chargeResponse.getBody().chargeId();

        TestSupport.awaitChargeStatus(restTemplate, chargeId, "ACTIVE");
        TestSupport.awaitVaResolvable(restTemplate, "BSI", payingVaNumber);
        TestSupport.awaitVaResolvable(restTemplate, "MAYBANK", siblingVaNumber);

        PaymentCallbackRequest settlingPayment = new PaymentCallbackRequest(
                "BSI", payingVaNumber, "REF-" + UUID.randomUUID(), totalAmount, Instant.now());
        ResponseEntity<PaymentCallbackResponse> settling = postCallback(settlingPayment);
        assertThat(settling.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(settling.getBody().status()).isEqualTo(PaymentOutcome.ACCEPTED.name());

        TestSupport.awaitChargeStatus(restTemplate, chargeId, "PAID");

        // The VA that actually received the payment: PAID, no longer resolvable.
        ResponseEntity<String> payingVaInquiry = inquire("BSI", payingVaNumber);
        assertThat(payingVaInquiry.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // Its sibling never received anything, but the charge it belongs to is now settled: CANCELLED,
        // also no longer resolvable -- the number is free for the next charge to reuse.
        ResponseEntity<String> siblingVaInquiry = inquire("MAYBANK", siblingVaNumber);
        assertThat(siblingVaInquiry.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private ResponseEntity<String> inquire(String bankCode, String vaNumber) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<InquiryController.AccountInquiryRequest> entity =
                new HttpEntity<>(new InquiryController.AccountInquiryRequest(bankCode, vaNumber), headers);
        return restTemplate.postForEntity("/api/v1/inquiry", entity, String.class);
    }

    private static BigDecimal extractCumulativePaid(String chargeJson) {
        Pattern pattern = Pattern.compile("\"cumulativePaid\"\\s*:\\s*\"?(-?[0-9.]+)\"?");
        Matcher matcher = pattern.matcher(chargeJson);
        assertThat(matcher.find()).as("cumulativePaid field present in charge JSON: %s", chargeJson).isTrue();
        return new BigDecimal(matcher.group(1));
    }

    @Test
    @DisplayName("REAL INTEGRATION: Bank callback with missing bankCode returns HTTP 400 Bad Request")
    void testPaymentCallbackMissingBankCode_Negative() {
        PaymentCallbackRequest request = new PaymentCallbackRequest(
                "",
                "9901928371",
                "REF-INVALID-1",
                new BigDecimal("100000.00"),
                Instant.now()
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<PaymentCallbackRequest> entity = new HttpEntity<>(request, headers);

        // Bean Validation rejects this before it ever reaches the controller method, so Spring's
        // default error body is returned (not our PaymentCallbackResponse shape) -- only the HTTP
        // status is a meaningful assertion here.
        ResponseEntity<String> response = restTemplate.postForEntity("/api/v1/payments", entity, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("REAL INTEGRATION: Bank callback with negative amount returns HTTP 400 Bad Request")
    void testPaymentCallbackNegativeAmount_Negative() {
        PaymentCallbackRequest request = new PaymentCallbackRequest(
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

    private ResponseEntity<PaymentCallbackResponse> postCallback(PaymentCallbackRequest request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<PaymentCallbackRequest> entity = new HttpEntity<>(request, headers);
        return restTemplate.postForEntity("/api/v1/payments", entity, PaymentCallbackResponse.class);
    }
}
