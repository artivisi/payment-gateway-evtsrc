package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.service.ChargeApplicationService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

/**
 * Consumer-facing Charge REST Controller.
 * Thin HTTP adapter: delegates all business logic, RocksDB queries, and event creation to ChargeApplicationService.
 */
@RestController
@RequestMapping({"/api/charges", "/api/v1/charges"})
public class ChargeController {

    private static final Logger log = LoggerFactory.getLogger(ChargeController.class);
    private static final String CLIENT_ID_HEADER = "X-Client-Id";
    private static final String CLIENT_SECRET_HEADER = "X-Client-Secret";

    private final ChargeApplicationService chargeApplicationService;

    public ChargeController(ChargeApplicationService chargeApplicationService) {
        this.chargeApplicationService = chargeApplicationService;
    }

    @PostMapping
    public ResponseEntity<CreateChargeResponse> createCharge(
            @RequestHeader(value = CLIENT_ID_HEADER, required = false) String headerClientId,
            @RequestHeader(value = CLIENT_SECRET_HEADER, required = false) String headerClientSecret,
            @Valid @RequestBody CreateChargeRequest request) {
        try {
            String chargeId = chargeApplicationService.createCharge(request, headerClientId);
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(new CreateChargeResponse("SUCCESS", chargeId, "Charge and sibling VAs created"));
        } catch (Exception e) {
            log.error("Failed to create charge", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new CreateChargeResponse("ERROR", null, "Failed to create charge: " + e.getMessage()));
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getCharge(
            @RequestHeader(value = CLIENT_ID_HEADER, required = false) String clientId,
            @RequestHeader(value = CLIENT_SECRET_HEADER, required = false) String clientSecret,
            @PathVariable String id) {
        
        Optional<String> chargeJson = chargeApplicationService.getChargeJson(id);
        if (chargeJson.isPresent()) {
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(chargeJson.get());
        }

        return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<CreateChargeResponse> cancelCharge(
            @RequestHeader(value = CLIENT_ID_HEADER, required = false) String clientId,
            @RequestHeader(value = CLIENT_SECRET_HEADER, required = false) String clientSecret,
            @PathVariable String id) {
        try {
            chargeApplicationService.cancelCharge(id);
            return ResponseEntity.ok(new CreateChargeResponse("SUCCESS", id, "Charge cancellation requested"));
        } catch (Exception e) {
            log.error("Failed to cancel charge for chargeId: {}", id, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new CreateChargeResponse("ERROR", id, "Failed to cancel charge: " + e.getMessage()));
        }
    }
}
