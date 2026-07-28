package com.artivisi.paymentgateway.web.api;

/** lagMillis is null until the projection sink has projected at least one payment event. */
public record ProjectionLagResponse(Long lagMillis) {}
