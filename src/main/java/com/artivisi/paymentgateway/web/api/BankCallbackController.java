package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.service.PaymentApplicationService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Generic / Maybank-shaped Bank Callback HTTP Controller.
 * Thin HTTP adapter: delegates all business logic and event log emission to PaymentApplicationService.
 * BSI's proprietary payload is served by a dedicated controller, not this one.
 */
@RestController
@RequestMapping({
    "/api/payments",
    "/api/v1/payments",
    "/api/bank/maybank/v1.0/transfer-va/payment"
})
public class BankCallbackController {

    private static final Logger log = LoggerFactory.getLogger(BankCallbackController.class);

    private final PaymentApplicationService paymentApplicationService;

    public BankCallbackController(PaymentApplicationService paymentApplicationService) {
        this.paymentApplicationService = paymentApplicationService;
    }

    @PostMapping
    public ResponseEntity<PaymentCallbackResponse> handleBankCallback(@Valid @RequestBody PaymentCallbackRequest request) {
        try {
            PaymentApplicationService.PaymentProcessingResult result = paymentApplicationService.processPayment(
                    request.bankCode(),
                    request.vaNumber(),
                    request.bankReference(),
                    request.amount(),
                    request.paymentTimestamp()
            );

            HttpStatus httpStatus = switch (result.outcome()) {
                case ACCEPTED, DUPLICATE -> HttpStatus.OK;
                case REJECTED_INVALID_VA -> HttpStatus.NOT_FOUND;
                case REJECTED_CHARGE_CLOSED, REJECTED_INVALID_AMOUNT, REJECTED_INVALID_REQUEST -> HttpStatus.BAD_REQUEST;
            };

            return ResponseEntity.status(httpStatus)
                    .body(new PaymentCallbackResponse(result.outcome().name(), result.message(), result.eventId()));
        } catch (Exception e) {
            log.error("Failed to process payment callback", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new PaymentCallbackResponse("ERROR", "Failed to process payment callback: " + e.getMessage(), null));
        }
    }
}
