package com.artivisi.paymentgateway.settlement;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import com.artivisi.paymentgateway.service.ChargeApplicationService;
import com.artivisi.paymentgateway.service.PaymentApplicationService;
import com.artivisi.paymentgateway.web.api.CreateChargeRequest;
import com.artivisi.paymentgateway.web.api.PaymentOutcome;
import com.artivisi.paymentgateway.web.api.SiblingVaRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives real concurrent threads (not sequential event replay) at a single freshly-created CLOSED
 * charge, to prove {@link ChargeSettlementStore#applyPayment} genuinely serializes racing payment
 * attempts via RocksDB's pessimistic transaction locking -- the guarantee the whole rearchitecture
 * in benchmark-remediation-guideline.md's Sixth/Seventh gap exists to provide. This is the direct
 * successor to {@code PostgresProjectionSinkTest}'s sequential-event-replay proof: that test showed
 * the OLD split-decision bug and its projection-level patch; this test shows the race can no longer
 * occur in the first place, under real thread contention, at the source.
 */
class ConcurrentPaymentSettlementIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private ChargeApplicationService chargeApplicationService;

    @Autowired
    private PaymentApplicationService paymentApplicationService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("50 concurrent threads racing to pay the same freshly-created CLOSED charge: exactly one is accepted, the rest are flagged, cumulativePaid never double-counts")
    void concurrentPaymentsAgainstSameCharge_exactlyOneWins_noDoubleCounting() throws Exception {
        BigDecimal totalAmount = new BigDecimal("1000000.00");
        String vaNumber = "CONC-" + UUID.randomUUID().toString().substring(0, 8);

        CreateChargeRequest chargeRequest = new CreateChargeRequest(
                "CLIENT-CONCURRENCY-TEST",
                "CLOSED",
                totalAmount,
                "Concurrency stress test charge",
                List.of(new SiblingVaRequest("BSI", vaNumber))
        );
        String chargeId = chargeApplicationService.createCharge(chargeRequest, "CLIENT-CONCURRENCY-TEST");

        // Registration is synchronous now -- no polling/hydration wait needed before firing payments.
        int concurrentAttempts = 50;
        ExecutorService executor = Executors.newFixedThreadPool(concurrentAttempts);
        try {
            List<Callable<PaymentOutcome>> tasks = IntStream.range(0, concurrentAttempts)
                    .mapToObj(i -> (Callable<PaymentOutcome>) () -> {
                        String bankReference = "CONC-REF-" + i + "-" + UUID.randomUUID();
                        var result = paymentApplicationService.processPayment(
                                "BSI", vaNumber, bankReference, totalAmount, Instant.now());
                        return result.outcome();
                    })
                    .toList();

            List<Future<PaymentOutcome>> futures = executor.invokeAll(tasks);

            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger rejectedAsDoubleSettlement = new AtomicInteger();
            for (Future<PaymentOutcome> future : futures) {
                PaymentOutcome outcome = future.get();
                if (outcome == PaymentOutcome.ACCEPTED) {
                    accepted.incrementAndGet();
                } else if (outcome == PaymentOutcome.REJECTED_CHARGE_CLOSED) {
                    rejectedAsDoubleSettlement.incrementAndGet();
                } else {
                    throw new AssertionError("Unexpected outcome under concurrency: " + outcome);
                }
            }

            assertThat(accepted.get())
                    .as("exactly one of %d concurrent racing payments must win", concurrentAttempts)
                    .isEqualTo(1);
            assertThat(rejectedAsDoubleSettlement.get())
                    .as("all other concurrent attempts must be flagged as double settlement, never silently absorbed")
                    .isEqualTo(concurrentAttempts - 1);
        } finally {
            executor.shutdown();
        }

        // The charge's own cumulativePaid must reflect exactly ONE payment, never double-counted
        // despite 50 threads racing -- this is what RocksDB's getForUpdate row-lock on the charge
        // key is directly responsible for.
        String chargeJson = chargeApplicationService.getChargeJson(chargeId).orElseThrow();
        JsonNode node = objectMapper.readTree(chargeJson);
        BigDecimal cumulativePaid = new BigDecimal(node.get("cumulativePaid").asString());
        assertThat(cumulativePaid).isEqualByComparingTo(totalAmount);
        assertThat(node.get("status").asString()).isEqualTo("PAID");
    }
}
