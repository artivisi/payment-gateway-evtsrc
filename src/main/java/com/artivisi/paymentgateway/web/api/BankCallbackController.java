package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping({"/api/payments", "/api/v1/payments"})
public class BankCallbackController {

    private static final Logger log = LoggerFactory.getLogger(BankCallbackController.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public BankCallbackController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<PaymentCallbackResponse> handleBankCallback(@Valid @RequestBody PaymentCallbackRequest request) {
        String eventId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        String externalCorrId = request.bankReference();
        PaymentReceivedEvent event = new PaymentReceivedEvent(
                eventId,
                request.chargeId(),
                request.bankCode(),
                request.vaNumber(),
                request.bankReference(),
                externalCorrId,
                request.amount(),
                request.paymentTimestamp() != null ? request.paymentTimestamp() : now,
                now
        );

        try {
            String jsonPayload = objectMapper.writeValueAsString(event);
            // Append event synchronously to Kafka with chargeId as partition key (<1ms hot-path append)
            kafkaTemplate.send(paymentEventsTopic, request.chargeId(), jsonPayload).get();

            log.info("Bank callback event appended to Kafka topic {} for chargeId: {}, eventId: {}",
                    paymentEventsTopic, request.chargeId(), eventId);

            return ResponseEntity.status(HttpStatus.OK)
                    .body(new PaymentCallbackResponse("SUCCESS", "Payment command accepted", eventId));

        } catch (Exception e) {
            log.error("Failed to append payment callback event to Kafka", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new PaymentCallbackResponse("ERROR", "Failed to process payment callback: " + e.getMessage(), null));
        }
    }
}
