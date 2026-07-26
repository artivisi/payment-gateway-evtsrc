package com.artivisi.paymentgateway.web.api;

public record PaymentCallbackResponse(
        String status,
        String message,
        String eventId
) {}
