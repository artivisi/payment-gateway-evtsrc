package com.artivisi.paymentgateway.web.api;

public record CreateChargeResponse(
        String status,
        String chargeId,
        String message
) {}
