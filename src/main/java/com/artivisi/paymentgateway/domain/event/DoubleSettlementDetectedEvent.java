package com.artivisi.paymentgateway.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

public record DoubleSettlementDetectedEvent(
        String eventId,
        String chargeId,
        String bankCode,
        String vaNumber,
        String bankReference,
        BigDecimal amount,
        String existingBankCode,
        Instant timestamp
) implements DomainEvent {
}
