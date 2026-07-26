package com.artivisi.paymentgateway.web.api;

public record SiblingVaRequest(
        String bankCode,
        String vaNumber
) {}
