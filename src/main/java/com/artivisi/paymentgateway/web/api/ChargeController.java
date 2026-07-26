package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/charges")
public class ChargeController {

    private static final Logger log = LoggerFactory.getLogger(ChargeController.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    public ChargeController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public record SiblingVaRequest(
            String bankCode,
            String vaNumber
    ) {}

    public record CreateChargeRequest(
            String clientId,
            String chargeType,
            BigDecimal totalAmount,
            String description,
            List<SiblingVaRequest> siblingVas
    ) {}

    public record CreateChargeResponse(
            String status,
            String chargeId,
            String message
    ) {}

    @PostMapping
    public ResponseEntity<CreateChargeResponse> createCharge(@RequestBody CreateChargeRequest request) {
        String chargeId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        try {
            // 1. Emit ChargeCreatedEvent
            ChargeCreatedEvent chargeEvent = new ChargeCreatedEvent(
                    UUID.randomUUID().toString(),
                    chargeId,
                    request.clientId(),
                    request.chargeType(),
                    request.totalAmount(),
                    request.description(),
                    now
            );
            kafkaTemplate.send(chargeEventsTopic, chargeId, objectMapper.writeValueAsString(chargeEvent)).get();

            // 2. Emit SiblingVaRegisteredEvent for each bank VA
            if (request.siblingVas() != null) {
                for (SiblingVaRequest vaReq : request.siblingVas()) {
                    SiblingVaRegisteredEvent vaEvent = new SiblingVaRegisteredEvent(
                            UUID.randomUUID().toString(),
                            chargeId,
                            vaReq.bankCode(),
                            vaReq.vaNumber(),
                            now
                    );
                    kafkaTemplate.send(vaEventsTopic, chargeId, objectMapper.writeValueAsString(vaEvent)).get();
                }
            }

            log.info("Charge created and domain events appended for chargeId: {}", chargeId);

            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(new CreateChargeResponse("SUCCESS", chargeId, "Charge and sibling VAs created"));

        } catch (Exception e) {
            log.error("Failed to append ChargeCreatedEvent to Kafka", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new CreateChargeResponse("ERROR", null, "Failed to create charge: " + e.getMessage()));
        }
    }
}
