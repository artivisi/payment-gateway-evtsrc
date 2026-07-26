package com.artivisi.paymentgateway.domain.event;

import java.time.Instant;

public record ChargeCancelledEvent(
        String eventId,
        String chargeId,
        String reason,
        Instant timestamp
) implements DomainEvent {
}
