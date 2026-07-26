package com.artivisi.paymentgateway.domain.event;

import java.time.Instant;

public record SiblingVaRegisteredEvent(
        String eventId,
        String chargeId,
        String bankCode,
        String vaNumber,
        Instant timestamp
) implements DomainEvent {
}
