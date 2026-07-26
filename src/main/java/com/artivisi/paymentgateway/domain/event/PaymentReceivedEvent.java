package com.artivisi.paymentgateway.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentReceivedEvent(
        String eventId,
        String chargeId,
        String bankCode,
        String vaNumber,
        String bankReference,
        String externalCorrelationId,
        BigDecimal amount,
        Instant paymentTimestamp,
        Instant timestamp
) implements DomainEvent {
    @Override
    public String externalCorrelationId() {
        return externalCorrelationId != null ? externalCorrelationId : bankReference;
    }
}
