package com.artivisi.paymentgateway.projection.sink;

import com.artivisi.paymentgateway.AbstractIntegrationTest;
import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.DoubleSettlementDetectedEvent;
import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.PaymentProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.PaymentProjectionRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link PostgresProjectionSink#consumeDomainEvents(List)} directly with a hand-crafted
 * event sequence, bypassing Kafka entirely, to deterministically reproduce the residual
 * pre-validation race documented on {@code PaymentApplicationService}: a request thread
 * optimistically accepts a payment (charge not yet PAID at its read), but the Kafka Streams
 * topology -- the actual single-writer authority -- later determines the charge was already
 * settled by a racing payment and emits a {@link DoubleSettlementDetectedEvent} for the SAME
 * bankReference.
 *
 * This does not depend on real concurrency, timing, or machine load: the two events are just fed
 * in the exact order the race produces them, so the outcome is 100% reproducible on any machine.
 */
class PostgresProjectionSinkTest extends AbstractIntegrationTest {

    @Autowired
    private PostgresProjectionSink sink;

    @Autowired
    private ChargeProjectionRepository chargeRepo;

    @Autowired
    private PaymentProjectionRepository paymentRepo;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("A bankReference optimistically accepted, then later overruled by the topology's own DoubleSettlementDetectedEvent for the same reference, must end up as exactly one flagged row -- never both an accepted and a flagged row")
    void lateDoubleSettlementForAnAlreadyAcceptedBankReference_correctsInPlace_doesNotDuplicate() {
        UUID chargeId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("3200000.00");
        String bankReference = "RACE-" + UUID.randomUUID();

        ChargeCreatedEvent chargeCreated = new ChargeCreatedEvent(
                UUID.randomUUID().toString(), chargeId.toString(), "CLIENT-TEST", "CLOSED",
                totalAmount, "Race test charge", Instant.now());
        sink.consumeDomainEvents(List.of(objectMapper.writeValueAsString(chargeCreated)));

        // Step 1: the request thread's pre-validation read the charge as not-yet-PAID and
        // optimistically accepted this payment -- exactly what PaymentApplicationService does
        // before the Kafka Streams topology gets a chance to re-check it.
        PaymentReceivedEvent accepted = new PaymentReceivedEvent(
                UUID.randomUUID().toString(), chargeId.toString(), "BSI", "99012026099",
                bankReference, bankReference, totalAmount, Instant.now(), Instant.now());
        sink.consumeDomainEvents(List.of(objectMapper.writeValueAsString(accepted)));

        ChargeProjectionEntity afterAccept = chargeRepo.findById(chargeId).orElseThrow();
        assertThat(afterAccept.getPaidAmount()).isEqualByComparingTo(totalAmount);
        assertThat(afterAccept.getStatus()).isEqualTo("FULLY_PAID");

        // Step 2: the Kafka Streams topology -- the real single-writer authority -- later
        // processes this SAME PaymentReceivedEvent and finds the charge was already settled by a
        // racing payment that got there first in partition order. It correctly emits a
        // DoubleSettlementDetectedEvent for THIS SAME bankReference, never silently absorbing it.
        DoubleSettlementDetectedEvent lateFlag = new DoubleSettlementDetectedEvent(
                UUID.randomUUID().toString(), chargeId.toString(), "BSI", "99012026099",
                bankReference, totalAmount, "BSI", Instant.now());
        sink.consumeDomainEvents(List.of(objectMapper.writeValueAsString(lateFlag)));

        List<PaymentProjectionEntity> payments = paymentRepo.findByChargeId(chargeId);
        assertThat(payments)
                .as("a single bankReference must never be recorded as both an accepted payment and a flagged double settlement")
                .hasSize(1);
        assertThat(payments.get(0).getBankReference()).isEqualTo(bankReference);
        assertThat(payments.get(0).isDoubleSettlement())
                .as("the topology's later, authoritative decision must supersede the request thread's optimistic accept")
                .isTrue();

        // The erroneous accept's contribution must be retracted, not left inflating the ledger.
        ChargeProjectionEntity afterCorrection = chargeRepo.findById(chargeId).orElseThrow();
        assertThat(afterCorrection.getPaidAmount()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(afterCorrection.getStatus()).isEqualTo("ACTIVE");
    }

    @Test
    @DisplayName("The normal double-settlement case (a bankReference that was always going to be rejected, never optimistically accepted) still gets exactly one flagged row")
    void doubleSettlementWithNoPriorAccept_stillInsertsOneRow() {
        UUID chargeId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("450000.00");
        String bankReference = "REJECTED-" + UUID.randomUUID();

        ChargeCreatedEvent chargeCreated = new ChargeCreatedEvent(
                UUID.randomUUID().toString(), chargeId.toString(), "CLIENT-TEST", "CLOSED",
                totalAmount, "Already-closed charge", Instant.now());
        sink.consumeDomainEvents(List.of(objectMapper.writeValueAsString(chargeCreated)));

        DoubleSettlementDetectedEvent flag = new DoubleSettlementDetectedEvent(
                UUID.randomUUID().toString(), chargeId.toString(), "BSI", "99012026098",
                bankReference, totalAmount, "BSI", Instant.now());
        sink.consumeDomainEvents(List.of(objectMapper.writeValueAsString(flag)));

        List<PaymentProjectionEntity> payments = paymentRepo.findByChargeId(chargeId);
        assertThat(payments).hasSize(1);
        assertThat(payments.get(0).isDoubleSettlement()).isTrue();

        ChargeProjectionEntity charge = chargeRepo.findById(chargeId).orElseThrow();
        assertThat(charge.getPaidAmount()).isEqualByComparingTo(BigDecimal.ZERO);
    }
}
