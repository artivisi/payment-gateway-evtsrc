package com.artivisi.paymentgateway.web.api;

/**
 * Single source of truth for the bank-callback outcome vocabulary.
 * Referenced by name (not ordinal) by adapters, load-test scripts, and audit tooling —
 * do not rename or remove values.
 */
public enum PaymentOutcome {
    ACCEPTED,
    DUPLICATE,
    REJECTED_INVALID_VA,
    REJECTED_CHARGE_CLOSED,
    REJECTED_INVALID_AMOUNT,
    REJECTED_INVALID_REQUEST
}
