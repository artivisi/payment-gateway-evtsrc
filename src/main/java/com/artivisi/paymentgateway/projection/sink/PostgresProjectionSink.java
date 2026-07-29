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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Batches domain events per poll instead of doing one existence-check + insert + charge-read +
 * charge-update JPA round-trip per record. See benchmark-remediation-guideline.md F3/G3: the
 * single-record listener could not keep up with acknowledged callback volume, so the read model
 * lagged the write path by hours.
 */
@Component
public class PostgresProjectionSink {

    private static final Logger log = LoggerFactory.getLogger(PostgresProjectionSink.class);

    private final ChargeProjectionRepository chargeRepo;
    private final SiblingVaProjectionRepository siblingVaRepo;
    private final PaymentProjectionRepository paymentRepo;
    private final ObjectMapper objectMapper;

    /** Latest observed (event.timestamp() -> projected) lag, in milliseconds. Null until the first payment is projected. */
    private final AtomicReference<Long> lastProjectionLagMillis = new AtomicReference<>();

    public PostgresProjectionSink(ChargeProjectionRepository chargeRepo,
                                  SiblingVaProjectionRepository siblingVaRepo,
                                  PaymentProjectionRepository paymentRepo,
                                  ObjectMapper objectMapper) {
        this.chargeRepo = chargeRepo;
        this.siblingVaRepo = siblingVaRepo;
        this.paymentRepo = paymentRepo;
        this.objectMapper = objectMapper;
    }

    public Long getLastProjectionLagMillis() {
        return lastProjectionLagMillis.get();
    }

    @KafkaListener(
        topics = {"${app.topics.charge-events:charge-events}", "${app.topics.va-events:va-events}", "${app.topics.payment-events:payment-events}"},
        groupId = "payment-gateway-projection-sink"
    )
    @Transactional
    public void consumeDomainEvents(List<String> eventJsons) {
        List<ChargeCreatedEvent> chargeCreatedEvents = new ArrayList<>();
        List<SiblingVaRegisteredEvent> siblingVaEvents = new ArrayList<>();
        List<PaymentReceivedEvent> paymentEvents = new ArrayList<>();
        List<DoubleSettlementDetectedEvent> doubleSettlementEvents = new ArrayList<>();

        for (String eventJson : eventJsons) {
            try {
                JsonNode root = objectMapper.readTree(eventJson);

                if (root.has("clientId") && root.has("totalAmount")) {
                    chargeCreatedEvents.add(objectMapper.treeToValue(root, ChargeCreatedEvent.class));
                } else if (root.has("bankCode") && root.has("vaNumber") && !root.has("bankReference")) {
                    siblingVaEvents.add(objectMapper.treeToValue(root, SiblingVaRegisteredEvent.class));
                } else if (root.has("bankReference") && !root.has("existingBankCode")) {
                    paymentEvents.add(objectMapper.treeToValue(root, PaymentReceivedEvent.class));
                } else if (root.has("existingBankCode")) {
                    doubleSettlementEvents.add(objectMapper.treeToValue(root, DoubleSettlementDetectedEvent.class));
                }
            } catch (Exception e) {
                log.error("Failed to parse domain event for projection: {}", eventJson, e);
            }
        }

        projectChargeCreatedBatch(chargeCreatedEvents);
        projectSiblingVaRegisteredBatch(siblingVaEvents);
        projectPaymentReceivedBatch(paymentEvents);
        projectDoubleSettlementBatch(doubleSettlementEvents);
    }

    private void projectChargeCreatedBatch(List<ChargeCreatedEvent> events) {
        if (events.isEmpty()) return;

        Map<UUID, ChargeCreatedEvent> byChargeId = new LinkedHashMap<>();
        for (ChargeCreatedEvent event : events) {
            UUID chargeId = parseUUID(event.chargeId());
            if (chargeId == null) continue;
            byChargeId.putIfAbsent(chargeId, event); // de-dupe within this batch, first occurrence wins
        }
        if (byChargeId.isEmpty()) return;

        Set<UUID> existingIds = new HashSet<>();
        chargeRepo.findAllById(byChargeId.keySet()).forEach(existing -> existingIds.add(existing.getId()));

        List<ChargeProjectionEntity> toSave = new ArrayList<>();
        byChargeId.forEach((chargeId, event) -> {
            if (existingIds.contains(chargeId)) return; // already projected
            toSave.add(new ChargeProjectionEntity(
                    chargeId,
                    event.clientId(),
                    event.chargeType(),
                    event.totalAmount(),
                    BigDecimal.ZERO,
                    event.totalAmount(),
                    "ACTIVE",
                    event.description(),
                    event.timestamp(),
                    event.timestamp()
            ));
        });

        if (!toSave.isEmpty()) {
            chargeRepo.saveAll(toSave);
        }
    }

    private void projectSiblingVaRegisteredBatch(List<SiblingVaRegisteredEvent> events) {
        if (events.isEmpty()) return;

        Set<String> seenInBatch = new HashSet<>();
        List<SiblingVaProjectionEntity> toSave = new ArrayList<>();
        for (SiblingVaRegisteredEvent event : events) {
            UUID chargeId = parseUUID(event.chargeId());
            if (chargeId == null) continue;

            String key = event.bankCode() + "_" + event.vaNumber();
            if (!seenInBatch.add(key)) continue; // duplicate within this batch
            if (siblingVaRepo.findByBankCodeAndVaNumber(event.bankCode(), event.vaNumber()).isPresent()) continue; // already projected

            toSave.add(new SiblingVaProjectionEntity(
                    UUID.randomUUID(),
                    chargeId,
                    event.bankCode(),
                    event.vaNumber(),
                    "ACTIVE",
                    event.timestamp(),
                    event.timestamp()
            ));
        }

        if (!toSave.isEmpty()) {
            siblingVaRepo.saveAll(toSave);
        }
    }

    private void projectPaymentReceivedBatch(List<PaymentReceivedEvent> events) {
        if (events.isEmpty()) return;

        Set<String> bankReferences = new HashSet<>();
        for (PaymentReceivedEvent event : events) {
            bankReferences.add(event.bankReference());
        }

        // Seeded with already-projected (bankCode_bankReference) keys, then reused below to also
        // catch duplicate references arriving within this same batch.
        Set<String> seenKeys = new HashSet<>();
        paymentRepo.findByBankReferenceIn(bankReferences)
                .forEach(existing -> seenKeys.add(existing.getBankCode() + "_" + existing.getBankReference()));

        List<PaymentProjectionEntity> paymentsToSave = new ArrayList<>();
        List<PaymentReceivedEvent> accepted = new ArrayList<>();
        Set<UUID> chargeIds = new LinkedHashSet<>();

        for (PaymentReceivedEvent event : events) {
            UUID eventId = parseUUID(event.eventId());
            UUID chargeId = parseUUID(event.chargeId());
            if (eventId == null || chargeId == null) continue;

            String key = event.bankCode() + "_" + event.bankReference();
            if (!seenKeys.add(key)) continue; // already projected, or duplicate within this batch

            paymentsToSave.add(new PaymentProjectionEntity(
                    eventId,
                    chargeId,
                    event.bankCode(),
                    event.vaNumber(),
                    event.bankReference(),
                    event.amount(),
                    event.paymentTimestamp(),
                    false,
                    event.timestamp()
            ));
            accepted.add(event);
            chargeIds.add(chargeId);

            lastProjectionLagMillis.set(Duration.between(event.timestamp(), Instant.now()).toMillis());
        }

        if (paymentsToSave.isEmpty()) return;
        paymentRepo.saveAll(paymentsToSave);

        Map<UUID, ChargeProjectionEntity> charges = new HashMap<>();
        chargeRepo.findAllById(chargeIds).forEach(charge -> charges.put(charge.getId(), charge));

        for (PaymentReceivedEvent event : accepted) {
            UUID chargeId = parseUUID(event.chargeId());
            ChargeProjectionEntity charge = charges.get(chargeId);
            if (charge == null) continue; // charge projection not hydrated yet -- best-effort, matches prior single-record behavior

            BigDecimal newPaid = charge.getPaidAmount().add(event.amount());
            BigDecimal newRemaining = charge.getTotalAmount().subtract(newPaid);

            charge.setPaidAmount(newPaid);
            // OPEN is a standing/always-active account (e.g. a donation VA): it never reaches
            // FULLY_PAID regardless of how far cumulativePaid exceeds the nominal amount, matching
            // ChargeSettlementStore's own truth and payment-gateway's own applyOpen()/CLAUDE.md
            // ("never auto-complete"). remainingAmount is left unclamped (can go negative) for
            // OPEN, since it's informational only, not an enforced cap.
            if ("OPEN".equals(charge.getChargeType())) {
                charge.setRemainingAmount(newRemaining);
                charge.setStatus("PARTIALLY_PAID");
            } else {
                if (newRemaining.compareTo(BigDecimal.ZERO) < 0) {
                    newRemaining = BigDecimal.ZERO;
                }
                charge.setRemainingAmount(newRemaining);
                charge.setStatus(newRemaining.compareTo(BigDecimal.ZERO) == 0 ? "FULLY_PAID" : "PARTIALLY_PAID");
            }
            charge.setUpdatedAt(event.timestamp());
        }

        if (!charges.isEmpty()) {
            chargeRepo.saveAll(charges.values());
        }
    }

    private void projectDoubleSettlementBatch(List<DoubleSettlementDetectedEvent> events) {
        if (events.isEmpty()) return;

        Set<String> bankReferences = new HashSet<>();
        for (DoubleSettlementDetectedEvent event : events) {
            bankReferences.add(event.bankReference());
        }

        // A bankReference can already have an "accepted" row from projectPaymentReceivedBatch,
        // projected optimistically by the request thread's pre-validation before the Kafka Streams
        // topology -- the actual single-writer authority -- determined this payment should have
        // been rejected as a double settlement instead (the residual pre-validation race
        // PaymentApplicationService's own javadoc documents: "two concurrent callbacks can both
        // pass this pre-check before either event is applied"). Once that authoritative decision
        // arrives here, it must correct the existing row in place, not add a second, contradicting
        // one for the same bankReference -- a single client-issued reference must never end up
        // recorded as both an accepted payment and a flagged double settlement.
        Map<String, PaymentProjectionEntity> existingByKey = new HashMap<>();
        paymentRepo.findByBankReferenceIn(bankReferences)
                .forEach(existing -> existingByKey.put(existing.getBankCode() + "_" + existing.getBankReference(), existing));

        List<PaymentProjectionEntity> toSave = new ArrayList<>();
        Set<String> seenInBatch = new HashSet<>();
        Set<UUID> chargeIdsToCorrect = new LinkedHashSet<>();
        Map<UUID, BigDecimal> retractedAmountByCharge = new HashMap<>();

        for (DoubleSettlementDetectedEvent event : events) {
            UUID eventId = parseUUID(event.eventId());
            UUID chargeId = parseUUID(event.chargeId());
            if (eventId == null || chargeId == null) continue;

            String key = event.bankCode() + "_" + event.bankReference();
            if (!seenInBatch.add(key)) continue; // duplicate within this batch

            PaymentProjectionEntity existing = existingByKey.get(key);
            if (existing != null) {
                if (!existing.isDoubleSettlement()) {
                    // Was wrongly counted as an accepted payment; retract its contribution from
                    // the charge below instead of leaving the ledger permanently inflated.
                    retractedAmountByCharge.merge(existing.getChargeId(), existing.getAmount(), BigDecimal::add);
                    chargeIdsToCorrect.add(existing.getChargeId());
                }
                existing.setDoubleSettlement(true);
                toSave.add(existing);
                continue;
            }

            toSave.add(new PaymentProjectionEntity(
                    eventId,
                    chargeId,
                    event.bankCode(),
                    event.vaNumber(),
                    event.bankReference(),
                    event.amount(),
                    event.timestamp(),
                    true,
                    event.timestamp()
            ));
        }

        if (!toSave.isEmpty()) {
            paymentRepo.saveAll(toSave);
            log.warn("Projected {} DoubleSettlementDetectedEvent(s) to PostgreSQL", toSave.size());
        }

        if (!chargeIdsToCorrect.isEmpty()) {
            correctChargesForRetractedPayments(chargeIdsToCorrect, retractedAmountByCharge);
        }
    }

    /**
     * Undoes the paidAmount/remainingAmount/status effect that {@link #projectPaymentReceivedBatch}
     * applied for a payment later found (via a same- or later-batch {@code DoubleSettlementDetectedEvent})
     * to have been an erroneously-accepted race loser, not a real settlement.
     */
    private void correctChargesForRetractedPayments(Set<UUID> chargeIds, Map<UUID, BigDecimal> retractedAmountByCharge) {
        Map<UUID, ChargeProjectionEntity> charges = new HashMap<>();
        chargeRepo.findAllById(chargeIds).forEach(charge -> charges.put(charge.getId(), charge));

        for (UUID chargeId : chargeIds) {
            ChargeProjectionEntity charge = charges.get(chargeId);
            if (charge == null) continue; // charge projection not hydrated yet -- best-effort, matches projectPaymentReceivedBatch

            BigDecimal retracted = retractedAmountByCharge.get(chargeId);
            BigDecimal correctedPaid = charge.getPaidAmount().subtract(retracted);
            charge.setPaidAmount(correctedPaid);

            if ("OPEN".equals(charge.getChargeType())) {
                charge.setRemainingAmount(charge.getTotalAmount().subtract(correctedPaid));
                charge.setStatus(correctedPaid.signum() > 0 ? "PARTIALLY_PAID" : "ACTIVE");
            } else {
                BigDecimal remaining = charge.getTotalAmount().subtract(correctedPaid);
                if (remaining.compareTo(BigDecimal.ZERO) < 0) {
                    remaining = BigDecimal.ZERO;
                }
                charge.setRemainingAmount(remaining);
                if (correctedPaid.signum() <= 0) {
                    charge.setStatus("ACTIVE");
                } else if (remaining.compareTo(BigDecimal.ZERO) == 0) {
                    charge.setStatus("FULLY_PAID");
                } else {
                    charge.setStatus("PARTIALLY_PAID");
                }
            }
            log.warn("Retracted erroneously-accepted payment amount {} from charge {} after a late double-settlement correction",
                    retracted, chargeId);
        }

        if (!charges.isEmpty()) {
            chargeRepo.saveAll(charges.values());
        }
    }

    private UUID parseUUID(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid UUID format encountered in domain event: {}", value);
            return null;
        }
    }
}
