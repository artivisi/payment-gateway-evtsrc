package com.artivisi.paymentgateway.domain.event;

import java.io.Serializable;
import java.time.Instant;

public sealed interface DomainEvent extends Serializable
        permits ChargeCreatedEvent, SiblingVaRegisteredEvent, PaymentReceivedEvent, DoubleSettlementDetectedEvent {

    String eventId();
    String chargeId();
    Instant timestamp();

    /** Internal uniform correlation ID (UUID format) */
    default String correlationId() {
        return eventId();
    }

    /** Optional external correlation ID / reference provided by incoming protocol/bank */
    default String externalCorrelationId() {
        return null;
    }
}
