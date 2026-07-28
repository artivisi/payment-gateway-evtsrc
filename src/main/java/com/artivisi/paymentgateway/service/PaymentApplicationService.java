package com.artivisi.paymentgateway.service;

import com.artivisi.paymentgateway.domain.event.DoubleSettlementDetectedEvent;
import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.artivisi.paymentgateway.streams.StoreConstants;
import com.artivisi.paymentgateway.web.api.PaymentOutcome;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Application Service for payment callbacks.
 *
 * Pre-validates against the RocksDB state stores on the request thread (idempotency,
 * VA resolution, charge terminal-status check) before appending a PaymentReceivedEvent
 * to Kafka. This is NOT the authoritative serialization point — two concurrent callbacks
 * can both pass this pre-check before either event is applied by the streams topology.
 * That residual race is caught and flagged (never silently absorbed) by
 * PaymentGatewayStreamsTopology.PaymentEventProcessor, which is the single-writer
 * per partition key.
 */
@Service
public class PaymentApplicationService {

    private static final Logger log = LoggerFactory.getLogger(PaymentApplicationService.class);
    private static final String CHARGE_STATUS_PAID = "PAID";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Value("${app.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public PaymentApplicationService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
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

        KafkaStreams kafkaStreams = requireRunningStreams();

        ReadOnlyKeyValueStore<String, String> idempotencyStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(StoreConstants.IDEMPOTENCY_STORE, QueryableStoreTypes.keyValueStore()));
        ReadOnlyKeyValueStore<String, String> vaStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(StoreConstants.VA_REGISTRY_STORE, QueryableStoreTypes.keyValueStore()));
        ReadOnlyKeyValueStore<String, String> chargeStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(StoreConstants.CHARGE_STATE_STORE, QueryableStoreTypes.keyValueStore()));

        // (a) Idempotency
        String idempotencyKey = bankCode + "_" + bankReference;
        String existingRecord = idempotencyStore.get(idempotencyKey);
        if (existingRecord != null) {
            JsonNode recorded = objectMapper.readTree(existingRecord);
            String recordedEventId = recorded.hasNonNull("eventId") ? recorded.get("eventId").asString() : null;
            String recordedChargeId = recorded.hasNonNull("chargeId") ? recorded.get("chargeId").asString() : null;
            return new PaymentProcessingResult(recordedEventId, recordedChargeId, PaymentOutcome.DUPLICATE,
                    "bankReference already processed for bankCode " + bankCode);
        }

        // (b) VA resolution
        String vaKey = bankCode + "_" + vaNumber;
        String chargeId = vaStore.get(vaKey);
        if (chargeId == null) {
            return new PaymentProcessingResult(null, null, PaymentOutcome.REJECTED_INVALID_VA,
                    "No active virtual account found for bankCode " + bankCode + " and vaNumber " + vaNumber);
        }

        // (c) Charge terminal-status check
        String chargeJson = chargeStore.get(chargeId);
        if (chargeJson == null) {
            return new PaymentProcessingResult(null, chargeId, PaymentOutcome.REJECTED_INVALID_VA,
                    "Charge record not yet available for chargeId " + chargeId);
        }

        JsonNode chargeNode = objectMapper.readTree(chargeJson);
        String status = chargeNode.hasNonNull("status") ? chargeNode.get("status").asString() : null;
        if (CHARGE_STATUS_PAID.equals(status)) {
            String doubleSettlementEventId = UUID.randomUUID().toString();
            DoubleSettlementDetectedEvent doubleSettlementEvent = new DoubleSettlementDetectedEvent(
                    doubleSettlementEventId,
                    chargeId,
                    bankCode,
                    vaNumber,
                    bankReference,
                    amount,
                    bankCode,
                    Instant.now()
            );
            kafkaTemplate.send(paymentEventsTopic, chargeId, objectMapper.writeValueAsString(doubleSettlementEvent)).get();
            log.warn("Double settlement detected pre-validation: chargeId={}, bankCode={}, bankReference={}",
                    chargeId, bankCode, bankReference);
            return new PaymentProcessingResult(doubleSettlementEventId, chargeId, PaymentOutcome.REJECTED_CHARGE_CLOSED,
                    "Charge " + chargeId + " is already PAID; payment flagged as a possible double settlement");
        }

        // (d) Append PaymentReceivedEvent
        String eventId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        PaymentReceivedEvent event = new PaymentReceivedEvent(
                eventId, chargeId, bankCode, vaNumber, bankReference, bankReference, amount, paymentTimestamp, now);
        kafkaTemplate.send(paymentEventsTopic, chargeId, objectMapper.writeValueAsString(event)).get();

        return new PaymentProcessingResult(eventId, chargeId, PaymentOutcome.ACCEPTED, "Payment accepted");
    }

    private KafkaStreams requireRunningStreams() {
        if (streamsBuilderFactoryBean == null) {
            throw new IllegalStateException("Kafka Streams builder not initialized; cannot validate payment against state stores");
        }
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException("Kafka Streams is not RUNNING; state stores unavailable for payment validation");
        }
        return kafkaStreams;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
