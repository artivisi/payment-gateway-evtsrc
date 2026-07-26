package com.artivisi.paymentgateway.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

public record ChargeCreatedEvent(
        String eventId,
        String chargeId,
        String clientId,
        String chargeType,
        BigDecimal totalAmount,
        String description,
        Instant timestamp
) implements DomainEvent {
}
