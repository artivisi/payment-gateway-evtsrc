package com.artivisi.paymentgateway.service;

import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.artivisi.paymentgateway.web.api.PaymentCallbackRequest;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * Application Service for payment callbacks.
 * Encapsulates domain event creation and synchronous Kafka event log appending.
 */
@Service
public class PaymentApplicationService {

    private static final Logger log = LoggerFactory.getLogger(PaymentApplicationService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public PaymentApplicationService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public String processPaymentCallback(PaymentCallbackRequest request) throws Exception {
        String eventId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        String effectiveChargeId = request.chargeId() != null && !request.chargeId().isBlank()
                ? request.chargeId()
                : (request.vaNumber() != null ? request.vaNumber() : "UNKNOWN_CHARGE");
        String effectiveBankCode = request.bankCode() != null && !request.bankCode().isBlank()
                ? request.bankCode()
                : "GENERIC_BANK";
        String externalCorrId = request.bankReference() != null && !request.bankReference().isBlank()
                ? request.bankReference()
                : eventId;

        PaymentReceivedEvent event = new PaymentReceivedEvent(
                eventId,
                effectiveChargeId,
                effectiveBankCode,
                request.vaNumber(),
                externalCorrId,
                externalCorrId,
                request.amount(),
                request.paymentTimestamp() != null ? request.paymentTimestamp() : now,
                now
        );

        String jsonPayload = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(paymentEventsTopic, effectiveChargeId, jsonPayload).get();

        log.info("Payment event appended to Kafka topic {} for chargeId: {}, eventId: {}",
                paymentEventsTopic, effectiveChargeId, eventId);

        return eventId;
    }
}
