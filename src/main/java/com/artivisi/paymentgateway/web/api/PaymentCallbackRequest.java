package com.artivisi.paymentgateway.web.api;

import com.fasterxml.jackson.annotation.JsonAlias;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Generic / Maybank-shaped payment callback request.
 * The caller must NOT supply the charge id — the gateway resolves it from the
 * (bankCode, vaNumber) pair via {@code ChargeSettlementStore}. A dedicated BSI controller
 * with its own DTO owns the BSI proprietary payload shape.
 */
public record PaymentCallbackRequest(
        @NotBlank(message = "bankCode is required")
        @JsonAlias({"bank_code", "kodeBank"})
        String bankCode,

        @NotBlank(message = "vaNumber is required")
        @JsonAlias({"va_number", "virtualAccountNo"})
        String vaNumber,

        @NotBlank(message = "bankReference is required")
        @JsonAlias({"bank_reference", "referenceNo"})
        String bankReference,

        @NotNull(message = "amount is required")
        @Positive(message = "amount must be greater than zero")
        @JsonAlias({"paid_amount", "paidAmount"})
        BigDecimal amount,

        @NotNull(message = "paymentTimestamp is required")
        @JsonAlias({"payment_timestamp"})
        Instant paymentTimestamp
) {}
