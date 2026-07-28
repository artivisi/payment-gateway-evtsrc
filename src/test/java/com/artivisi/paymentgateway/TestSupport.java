package com.artivisi.paymentgateway;

import org.awaitility.Awaitility;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared polling helpers for integration tests.
 *
 * Kafka Streams hydration (charge-events / va-events / payment-events -> RocksDB state stores) is
 * asynchronous relative to the REST call that published the event. Tests that create a charge/VA
 * via POST and then rely on RocksDB-derived state (payment callback pre-validation, inquiry, the
 * charge GET endpoint) must poll for the expected state instead of asserting immediately.
 *
 * Reuse {@link #awaitChargeStatus} and {@link #awaitChargeVisible} rather than adding ad-hoc
 * Thread.sleep()/retry loops in new tests.
 */
public final class TestSupport {

    private TestSupport() {}

    /**
     * Polls GET /api/v1/charges/{chargeId} until the charge-state-store JSON reports the given
     * "status" value (e.g. "ACTIVE", "PAID"), failing the test after the timeout elapses.
     */
    public static void awaitChargeStatus(TestRestTemplate restTemplate, String chargeId, String expectedStatus) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    ResponseEntity<String> response = restTemplate.getForEntity("/api/v1/charges/" + chargeId, String.class);
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getBody()).contains("\"status\":\"" + expectedStatus + "\"");
                });
    }

    /**
     * Polls GET /api/v1/charges/{chargeId} until the charge-state-store has hydrated at all
     * (any status), for tests that only need the VA registered before firing a payment callback.
     */
    public static void awaitChargeVisible(TestRestTemplate restTemplate, String chargeId) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    ResponseEntity<String> response = restTemplate.getForEntity("/api/v1/charges/" + chargeId, String.class);
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                });
    }

    /**
     * Polls POST /api/v1/inquiry until the given (bankCode, vaNumber) resolves successfully.
     *
     * charge-events and va-events are two independently-lagging Kafka Streams consumer pipelines:
     * the charge-state-store can report a charge "ACTIVE" (awaitChargeStatus) before va-registry-store
     * has processed that charge's SiblingVaRegisteredEvent, or vice versa. A test that creates a
     * charge+VA and immediately fires a payment callback must wait for BOTH stores, not just the
     * charge's own status, or it will intermittently observe REJECTED_INVALID_VA on a VA that is
     * genuinely registered moments later.
     */
    /**
     * Polls GET /api/v1/charges/{chargeId} until cumulativePaid reaches at least the given amount.
     *
     * PaymentApplicationService's request-thread idempotency check reads idempotency-store, which
     * is written asynchronously by the topology only after it consumes a PaymentReceivedEvent — not
     * by pre-validation itself (interactive queries are read-only). A retry fired immediately after
     * an ACCEPTED response can therefore race ahead of the topology and also come back ACCEPTED
     * instead of DUPLICATE, even though the topology's own idempotency re-check guarantees the
     * charge is never double-counted. Tests asserting DUPLICATE semantics must wait for the first
     * payment to actually land (this method) before firing the repeat, matching how a real bank's
     * retry-on-timeout behaves rather than a zero-delay repeat.
     */
    public static void awaitCumulativePaidAtLeast(TestRestTemplate restTemplate, String chargeId, BigDecimal minCumulativePaid) {
        Pattern pattern = Pattern.compile("\"cumulativePaid\"\\s*:\\s*\"?(-?[0-9.]+)\"?");
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    ResponseEntity<String> response = restTemplate.getForEntity("/api/v1/charges/" + chargeId, String.class);
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    Matcher matcher = pattern.matcher(response.getBody());
                    assertThat(matcher.find()).as("cumulativePaid field present in charge JSON: %s", response.getBody()).isTrue();
                    BigDecimal actual = new BigDecimal(matcher.group(1));
                    assertThat(actual).isGreaterThanOrEqualTo(minCumulativePaid);
                });
    }

    public static void awaitVaResolvable(TestRestTemplate restTemplate, String bankCode, String vaNumber) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    String body = "{\"bankCode\":\"" + bankCode + "\",\"vaNumber\":\"" + vaNumber + "\"}";
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    ResponseEntity<String> response = restTemplate.postForEntity(
                            "/api/v1/inquiry", new HttpEntity<>(body, headers), String.class);
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                });
    }
}
