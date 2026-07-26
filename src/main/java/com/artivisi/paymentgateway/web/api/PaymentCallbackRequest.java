package com.artivisi.paymentgateway.web.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCallbackRequest(
        @NotBlank(message = "chargeId is required")
        String chargeId,

        @NotBlank(message = "bankCode is required")
        String bankCode,

        @NotBlank(message = "vaNumber is required")
        String vaNumber,

        @NotBlank(message = "bankReference is required")
        String bankReference,

        @NotNull(message = "amount is required")
        @Positive(message = "amount must be greater than zero")
        BigDecimal amount,

        Instant paymentTimestamp
) {}
