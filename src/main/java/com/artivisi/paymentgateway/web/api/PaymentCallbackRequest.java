package com.artivisi.paymentgateway.web.api;

import com.fasterxml.jackson.annotation.JsonAlias;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Universal Payment Callback Request supporting generic JSON payloads,
 * Maybank SNAP 1.0.2 payloads, and BSI REST payloads.
 */
public record PaymentCallbackRequest(
        @NotBlank(message = "chargeId is required")
        @JsonAlias({"charge_id", "idTransaksi", "virtualAccountNo"})
        String chargeId,

        @NotBlank(message = "bankCode is required")
        @JsonAlias({"bank_code", "kodeBank"})
        String bankCode,

        @NotBlank(message = "vaNumber is required")
        @JsonAlias({"va_number", "nomorPembayaran", "virtualAccountNo"})
        String vaNumber,

        @NotBlank(message = "bankReference is required")
        @JsonAlias({"bank_reference", "idTransaksi", "referenceNo", "checksum"})
        String bankReference,

        @NotNull(message = "amount is required")
        @Positive(message = "amount must be greater than zero")
        @JsonAlias({"paid_amount", "nilai", "paidAmount"})
        BigDecimal amount,

        @JsonAlias({"payment_timestamp", "tanggalTransaksi"})
        Instant paymentTimestamp
) {}
