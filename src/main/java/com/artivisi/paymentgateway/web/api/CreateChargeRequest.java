package com.artivisi.paymentgateway.web.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.List;

public record CreateChargeRequest(
        @NotBlank(message = "clientId is required")
        String clientId,

        @NotBlank(message = "chargeType is required")
        String chargeType,

        @NotNull(message = "totalAmount is required")
        @Positive(message = "totalAmount must be greater than zero")
        BigDecimal totalAmount,

        String description,
        List<SiblingVaRequest> siblingVas
) {}
