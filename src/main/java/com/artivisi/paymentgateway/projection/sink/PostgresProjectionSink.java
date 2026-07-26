package com.artivisi.paymentgateway.projection.sink;

import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
import com.artivisi.paymentgateway.domain.event.DoubleSettlementDetectedEvent;
import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import com.artivisi.paymentgateway.projection.entity.ChargeProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.PaymentProjectionEntity;
import com.artivisi.paymentgateway.projection.entity.SiblingVaProjectionEntity;
import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.PaymentProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.SiblingVaProjectionRepository;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Component
public class PostgresProjectionSink {

    private static final Logger log = LoggerFactory.getLogger(PostgresProjectionSink.class);

    private final ChargeProjectionRepository chargeRepo;
    private final SiblingVaProjectionRepository siblingVaRepo;
    private final PaymentProjectionRepository paymentRepo;
    private final ObjectMapper objectMapper;

    public PostgresProjectionSink(ChargeProjectionRepository chargeRepo,
                                  SiblingVaProjectionRepository siblingVaRepo,
                                  PaymentProjectionRepository paymentRepo,
                                  ObjectMapper objectMapper) {
        this.chargeRepo = chargeRepo;
        this.siblingVaRepo = siblingVaRepo;
        this.paymentRepo = paymentRepo;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = {"${app.topics.charge-events:charge-events}", "${app.topics.va-events:va-events}", "${app.topics.payment-events:payment-events}"},
        groupId = "payment-gateway-projection-sink"
    )
    @Transactional
    public void consumeDomainEvent(String eventJson) {
        try {
            JsonNode root = objectMapper.readTree(eventJson);

            if (root.has("clientId") && root.has("totalAmount")) {
                // ChargeCreatedEvent
                ChargeCreatedEvent event = objectMapper.treeToValue(root, ChargeCreatedEvent.class);
                projectChargeCreated(event);
            } else if (root.has("bankCode") && root.has("vaNumber") && !root.has("bankReference")) {
                // SiblingVaRegisteredEvent
                SiblingVaRegisteredEvent event = objectMapper.treeToValue(root, SiblingVaRegisteredEvent.class);
                projectSiblingVaRegistered(event);
            } else if (root.has("bankReference") && !root.has("existingBankCode")) {
                // PaymentReceivedEvent
                PaymentReceivedEvent event = objectMapper.treeToValue(root, PaymentReceivedEvent.class);
                projectPaymentReceived(event);
            } else if (root.has("existingBankCode")) {
                // DoubleSettlementDetectedEvent
                DoubleSettlementDetectedEvent event = objectMapper.treeToValue(root, DoubleSettlementDetectedEvent.class);
                projectDoubleSettlement(event);
            }
        } catch (Exception e) {
            log.error("Failed to project domain event to PostgreSQL read models: {}", eventJson, e);
        }
    }

    private void projectChargeCreated(ChargeCreatedEvent event) {
        if (chargeRepo.existsById(event.chargeId())) {
            return; // Idempotent check
        }
        ChargeProjectionEntity entity = new ChargeProjectionEntity(
                event.chargeId(),
                event.clientId(),
                event.chargeType(),
                event.totalAmount(),
                BigDecimal.ZERO,
                event.totalAmount(),
                "ACTIVE",
                event.description(),
                event.timestamp(),
                event.timestamp()
        );

        chargeRepo.save(entity);
        log.info("Projected ChargeCreatedEvent to PostgreSQL for chargeId: {}", event.chargeId());
    }

    private void projectSiblingVaRegistered(SiblingVaRegisteredEvent event) {
        String entityId = UUID.randomUUID().toString();
        if (siblingVaRepo.findByBankCodeAndVaNumber(event.bankCode(), event.vaNumber()).isPresent()) {
            return; // Idempotent check
        }
        SiblingVaProjectionEntity entity = new SiblingVaProjectionEntity(
                entityId,
                event.chargeId(),
                event.bankCode(),
                event.vaNumber(),
                "ACTIVE",
                event.timestamp(),
                event.timestamp()
        );

        siblingVaRepo.save(entity);
        log.info("Projected SiblingVaRegisteredEvent to PostgreSQL for bank: {}, va: {}", event.bankCode(), event.vaNumber());
    }

    private void projectPaymentReceived(PaymentReceivedEvent event) {
        if (paymentRepo.findByBankCodeAndBankReference(event.bankCode(), event.bankReference()).isPresent()) {
            return; // Idempotent check
        }

        PaymentProjectionEntity paymentEntity = new PaymentProjectionEntity(
                event.eventId(),
                event.chargeId(),
                event.bankCode(),
                event.vaNumber(),
                event.bankReference(),
                event.amount(),
                event.paymentTimestamp(),
                false,
                event.timestamp()
        );

        paymentRepo.save(paymentEntity);

        // Update charge balance in PostgreSQL read model
        chargeRepo.findById(event.chargeId()).ifPresent(charge -> {
            BigDecimal newPaid = charge.getPaidAmount().add(event.amount());
            BigDecimal newRemaining = charge.getTotalAmount().subtract(newPaid);
            if (newRemaining.compareTo(BigDecimal.ZERO) < 0) {
                newRemaining = BigDecimal.ZERO;
            }

            charge.setPaidAmount(newPaid);
            charge.setRemainingAmount(newRemaining);
            if (newRemaining.compareTo(BigDecimal.ZERO) == 0) {
                charge.setStatus("FULLY_PAID");
            } else {
                charge.setStatus("PARTIALLY_PAID");
            }
            charge.setUpdatedAt(event.timestamp());
            chargeRepo.save(charge);
        });

        log.info("Projected PaymentReceivedEvent to PostgreSQL for chargeId: {}, amount: {}", event.chargeId(), event.amount());
    }

    private void projectDoubleSettlement(DoubleSettlementDetectedEvent event) {
        PaymentProjectionEntity paymentEntity = new PaymentProjectionEntity(
                event.eventId(),
                event.chargeId(),
                event.bankCode(),
                event.vaNumber(),
                event.bankReference(),
                event.amount(),
                event.timestamp(),
                true,
                event.timestamp()
        );

        paymentRepo.save(paymentEntity);
        log.warn("Projected DoubleSettlementDetectedEvent to PostgreSQL for chargeId: {}, bank: {}", event.chargeId(), event.bankCode());
    }
}
