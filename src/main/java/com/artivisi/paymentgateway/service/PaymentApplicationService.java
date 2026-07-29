package com.artivisi.paymentgateway.service;

import com.artivisi.paymentgateway.domain.event.DoubleSettlementDetectedEvent;
import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.artivisi.paymentgateway.settlement.ChargeSettlementStore;
import com.artivisi.paymentgateway.web.api.PaymentOutcome;
import tools.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Application Service for payment callbacks.
 *
 * Delegates the entire lookup-validate-apply sequence to {@link ChargeSettlementStore} as one
 * atomic RocksDB transaction on this same request thread -- there is no separate, later
 * authoritative re-check, because there is no gap between "checked" and "applied" for another
 * request to land in. Kafka is only used afterward, to broadcast the already-final outcome to
 * downstream projections (see ChargeSettlementStore's class Javadoc for why the previous
 * request-thread-then-async-topology split was replaced).
 */
@Service
public class PaymentApplicationService {

    private final ChargeSettlementStore settlementStore;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public PaymentApplicationService(ChargeSettlementStore settlementStore,
                                      KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.settlementStore = settlementStore;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public record PaymentProcessingResult(
            String eventId,
            String chargeId,
            PaymentOutcome outcome,
            String message
    ) {}

    public PaymentProcessingResult processPayment(String bankCode, String vaNumber, String bankReference,
                                                   BigDecimal amount, Instant paymentTimestamp) throws Exception {
        if (isBlank(bankCode) || isBlank(vaNumber) || isBlank(bankReference)
                || amount == null || amount.signum() <= 0 || paymentTimestamp == null) {
            return new PaymentProcessingResult(null, null, PaymentOutcome.REJECTED_INVALID_REQUEST,
                    "bankCode, vaNumber, bankReference, a positive amount and paymentTimestamp are all required");
        }

        ChargeSettlementStore.PaymentApplicationResult result =
                settlementStore.applyPayment(bankCode, vaNumber, bankReference, amount, paymentTimestamp);

        publishFact(bankCode, vaNumber, bankReference, amount, paymentTimestamp, result);

        return new PaymentProcessingResult(result.eventId(), result.chargeId(), result.outcome(), result.message());
    }

    /**
     * Broadcasts the already-decided outcome downstream (PostgresProjectionSink's read model).
     * This never influences correctness -- the decision in ChargeSettlementStore.applyPayment is
     * already final by the time this runs.
     */
    private void publishFact(String bankCode, String vaNumber, String bankReference, BigDecimal amount,
                              Instant paymentTimestamp, ChargeSettlementStore.PaymentApplicationResult result) throws Exception {
        if (result.outcome() == PaymentOutcome.ACCEPTED) {
            PaymentReceivedEvent event = new PaymentReceivedEvent(
                    result.eventId(), result.chargeId(), bankCode, vaNumber, bankReference, bankReference,
                    amount, paymentTimestamp, Instant.now());
            kafkaTemplate.send(paymentEventsTopic, result.chargeId(), objectMapper.writeValueAsString(event)).get();
        } else if (result.outcome() == PaymentOutcome.REJECTED_CHARGE_CLOSED) {
            DoubleSettlementDetectedEvent event = new DoubleSettlementDetectedEvent(
                    result.eventId(), result.chargeId(), bankCode, vaNumber, bankReference, amount,
                    bankCode, Instant.now());
            kafkaTemplate.send(paymentEventsTopic, result.chargeId(), objectMapper.writeValueAsString(event)).get();
        }
        // DUPLICATE / REJECTED_INVALID_VA / REJECTED_INVALID_REQUEST: nothing new happened, nothing to broadcast.
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
