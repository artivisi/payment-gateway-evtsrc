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
 * Charge/VA registration and payment settlement are all synchronous writes directly into
 * {@code ChargeSettlementStore} now (see its own Javadoc) -- GET /api/v1/charges/{id} and
 * POST /api/v1/inquiry read from that same store, so there is no hydration lag on this data path
 * any more (an older design had Kafka Streams hydrating separate RocksDB state stores
 * asynchronously; that's gone). These helpers are kept as defensive polling wrappers -- harmless
 * (they succeed on the first attempt today) and cheaper than touching every call site to remove
 * them -- rather than because anything here is still genuinely eventually-consistent.
 *
 * Reuse {@link #awaitChargeStatus} and {@link #awaitChargeVisible} rather than adding ad-hoc
 * Thread.sleep()/retry loops in new tests.
 */
public final class TestSupport {

    private TestSupport() {}

    /**
     * Polls GET /api/v1/charges/{chargeId} until the JSON reports the given "status" value (e.g.
     * "ACTIVE", "PAID"), failing the test after the timeout elapses.
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
     * Polls GET /api/v1/charges/{chargeId} until it responds at all (any status), for tests that
     * only need the VA registered before firing a payment callback.
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
     * Polls GET /api/v1/charges/{chargeId} until cumulativePaid reaches at least the given amount.
     *
     * Charge registration and payment settlement are both synchronous now (one atomic RocksDB
     * transaction per payment against {@code ChargeSettlementStore} -- see its own Javadoc), so a
     * retry fired immediately after an ACCEPTED response deterministically gets DUPLICATE; there is
     * no async re-check to race ahead of any more. Kept for tests that want to assert on the
     * settled amount specifically rather than just the immediate response.
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

    /** Polls POST /api/v1/inquiry until the given (bankCode, vaNumber) resolves successfully. */
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
