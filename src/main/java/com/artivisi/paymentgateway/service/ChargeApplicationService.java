package com.artivisi.paymentgateway.service;

import com.artivisi.paymentgateway.domain.event.ChargeCancelledEvent;
import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import com.artivisi.paymentgateway.streams.StoreConstants;
import com.artivisi.paymentgateway.web.api.CreateChargeRequest;
import com.artivisi.paymentgateway.web.api.SiblingVaRequest;
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

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Application Service for Charge management.
 * Encapsulates domain event generation, RocksDB query logic, and cancellation logic.
 */
@Service
public class ChargeApplicationService {

    private static final Logger log = LoggerFactory.getLogger(ChargeApplicationService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    public ChargeApplicationService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public String createCharge(CreateChargeRequest request, String headerClientId) throws Exception {
        String chargeId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        String effectiveClientId = request.clientId() != null && !request.clientId().isBlank()
                ? request.clientId()
                : (headerClientId != null ? headerClientId : "DEFAULT_CLIENT");

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

        log.info("Charge created and events published for chargeId: {}", chargeId);
        return chargeId;
    }

    public Optional<String> getChargeJson(String id) {
        if (streamsBuilderFactoryBean != null) {
            try {
                KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
                if (kafkaStreams != null && kafkaStreams.state() == KafkaStreams.State.RUNNING) {
                    ReadOnlyKeyValueStore<String, String> chargeStore = kafkaStreams.store(
                            StoreQueryParameters.fromNameAndType(StoreConstants.CHARGE_STATE_STORE, QueryableStoreTypes.keyValueStore())
                    );
                    String chargeJson = chargeStore.get(id);
                    return Optional.ofNullable(chargeJson);
                }
            } catch (Exception e) {
                log.debug("RocksDB lookup for charge {} failed: {}", id, e.getMessage());
            }
        }
        return Optional.empty();
    }

    public void cancelCharge(String id) throws Exception {
        Instant now = Instant.now();
        ChargeCancelledEvent cancelEvent = new ChargeCancelledEvent(
                UUID.randomUUID().toString(),
                id,
                "Cancelled via API",
                now
        );
        kafkaTemplate.send(chargeEventsTopic, id, objectMapper.writeValueAsString(cancelEvent)).get();
        log.info("Charge cancelled event published for chargeId: {}", id);
    }
}
