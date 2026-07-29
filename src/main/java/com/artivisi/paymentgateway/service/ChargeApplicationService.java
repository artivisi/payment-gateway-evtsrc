package com.artivisi.paymentgateway.service;

import com.artivisi.paymentgateway.domain.event.ChargeCancelledEvent;
import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import com.artivisi.paymentgateway.settlement.ChargeSettlementStore;
import com.artivisi.paymentgateway.web.api.CreateChargeRequest;
import com.artivisi.paymentgateway.web.api.SiblingVaRequest;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Application Service for Charge management.
 *
 * Registers charges/VAs directly into {@link ChargeSettlementStore} on this same request thread --
 * synchronously, so a charge and its sibling VAs are resolvable the instant this call returns, no
 * hydration lag to poll for. Kafka events are still published afterward, purely for
 * PostgresProjectionSink's read model.
 */
@Service
public class ChargeApplicationService {

    private static final Logger log = LoggerFactory.getLogger(ChargeApplicationService.class);

    private final ChargeSettlementStore settlementStore;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    public ChargeApplicationService(ChargeSettlementStore settlementStore,
                                     KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.settlementStore = settlementStore;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public String createCharge(CreateChargeRequest request, String headerClientId) throws Exception {
        String chargeId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        String effectiveClientId = request.clientId() != null && !request.clientId().isBlank()
                ? request.clientId()
                : (headerClientId != null ? headerClientId : "DEFAULT_CLIENT");

        // 1. Register synchronously -- readable immediately, no async hydration wait.
        settlementStore.registerCharge(chargeId, effectiveClientId, request.chargeType(),
                request.totalAmount(), request.description(), now);
        if (request.siblingVas() != null) {
            for (SiblingVaRequest vaReq : request.siblingVas()) {
                settlementStore.registerVa(vaReq.bankCode(), vaReq.vaNumber(), chargeId);
            }
        }

        // 2. Publish facts for the read model.
        ChargeCreatedEvent chargeEvent = new ChargeCreatedEvent(
                UUID.randomUUID().toString(), chargeId, effectiveClientId, request.chargeType(),
                request.totalAmount(), request.description(), now);
        kafkaTemplate.send(chargeEventsTopic, chargeId, objectMapper.writeValueAsString(chargeEvent)).get();

        if (request.siblingVas() != null) {
            for (SiblingVaRequest vaReq : request.siblingVas()) {
                SiblingVaRegisteredEvent vaEvent = new SiblingVaRegisteredEvent(
                        UUID.randomUUID().toString(), chargeId, vaReq.bankCode(), vaReq.vaNumber(), now);
                kafkaTemplate.send(vaEventsTopic, chargeId, objectMapper.writeValueAsString(vaEvent)).get();
            }
        }

        log.info("Charge {} registered synchronously and events published for projection", chargeId);
        return chargeId;
    }

    public Optional<String> getChargeJson(String id) {
        try {
            return settlementStore.getChargeJson(id);
        } catch (Exception e) {
            log.debug("ChargeSettlementStore lookup for charge {} failed: {}", id, e.getMessage());
            return Optional.empty();
        }
    }

    public void cancelCharge(String id) throws Exception {
        Instant now = Instant.now();
        settlementStore.cancelCharge(id);

        ChargeCancelledEvent cancelEvent = new ChargeCancelledEvent(
                UUID.randomUUID().toString(), id, "Cancelled via API", now);
        kafkaTemplate.send(chargeEventsTopic, id, objectMapper.writeValueAsString(cancelEvent)).get();
        log.info("Charge {} cancelled synchronously and event published for projection", id);
    }
}
