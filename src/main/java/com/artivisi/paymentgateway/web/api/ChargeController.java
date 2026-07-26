package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.domain.event.ChargeCancelledEvent;
import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import com.artivisi.paymentgateway.streams.StoreConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

/**
 * Consumer-facing charge API matching sibling payment-gateway contracts.
 * Pure Kafka / RocksDB solution (<1ms off-heap lookup).
 * ZERO PostgreSQL dependency — PostgreSQL is reserved strictly for reporting sinks.
 */
@RestController
@RequestMapping({"/api/charges", "/api/v1/charges"})
public class ChargeController {

    private static final Logger log = LoggerFactory.getLogger(ChargeController.class);
    private static final String CLIENT_ID_HEADER = "X-Client-Id";
    private static final String CLIENT_SECRET_HEADER = "X-Client-Secret";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    public ChargeController(KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<CreateChargeResponse> createCharge(
            @RequestHeader(value = CLIENT_ID_HEADER, required = false) String headerClientId,
            @RequestHeader(value = CLIENT_SECRET_HEADER, required = false) String headerClientSecret,
            @Valid @RequestBody CreateChargeRequest request) {
        
        String chargeId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        String effectiveClientId = request.clientId() != null && !request.clientId().isBlank()
                ? request.clientId()
                : (headerClientId != null ? headerClientId : "DEFAULT_CLIENT");

        try {
            // 1. Emit ChargeCreatedEvent
            ChargeCreatedEvent chargeEvent = new ChargeCreatedEvent(
                    UUID.randomUUID().toString(),
                    chargeId,
                    effectiveClientId,
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

    @GetMapping("/{id}")
    public ResponseEntity<?> getCharge(
            @RequestHeader(value = CLIENT_ID_HEADER, required = false) String clientId,
            @RequestHeader(value = CLIENT_SECRET_HEADER, required = false) String clientSecret,
            @PathVariable String id) {
        
        if (streamsBuilderFactoryBean != null) {
            try {
                KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
                if (kafkaStreams != null && kafkaStreams.state() == KafkaStreams.State.RUNNING) {
                    ReadOnlyKeyValueStore<String, String> chargeStore = kafkaStreams.store(
                            StoreQueryParameters.fromNameAndType(StoreConstants.CHARGE_STATE_STORE, QueryableStoreTypes.keyValueStore())
                    );
                    String chargeJson = chargeStore.get(id);
                    if (chargeJson != null) {
                        return ResponseEntity.ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(chargeJson);
                    }
                }
            } catch (Exception e) {
                log.debug("RocksDB lookup for charge {} failed: {}", id, e.getMessage());
            }
        }

        return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<CreateChargeResponse> cancelCharge(
            @RequestHeader(value = CLIENT_ID_HEADER, required = false) String clientId,
            @RequestHeader(value = CLIENT_SECRET_HEADER, required = false) String clientSecret,
            @PathVariable String id) {
        
        Instant now = Instant.now();
        ChargeCancelledEvent cancelEvent = new ChargeCancelledEvent(
                UUID.randomUUID().toString(),
                id,
                "Cancelled via API",
                now
        );

        try {
            kafkaTemplate.send(chargeEventsTopic, id, objectMapper.writeValueAsString(cancelEvent)).get();
            log.info("Charge cancelled event appended to Kafka for chargeId: {}", id);
            return ResponseEntity.ok(new CreateChargeResponse("SUCCESS", id, "Charge cancellation requested"));
        } catch (Exception e) {
            log.error("Failed to append ChargeCancelledEvent to Kafka for chargeId: {}", id, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new CreateChargeResponse("ERROR", id, "Failed to cancel charge: " + e.getMessage()));
        }
    }
}
