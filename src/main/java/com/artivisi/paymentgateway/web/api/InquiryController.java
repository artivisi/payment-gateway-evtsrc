package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.service.InquiryApplicationService;
import com.fasterxml.jackson.annotation.JsonAlias;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

/**
 * Bank Account Inquiry HTTP Controller.
 * Thin HTTP adapter: delegates all RocksDB resolution logic to InquiryApplicationService.
 */
@RestController
@RequestMapping({
    "/api/inquiry",
    "/api/v1/inquiry",
    "/api/bank/maybank/v1.0/transfer-va/inquiry"
})
public class InquiryController {

    private final InquiryApplicationService inquiryApplicationService;

    public InquiryController(InquiryApplicationService inquiryApplicationService) {
        this.inquiryApplicationService = inquiryApplicationService;
    }

    public record AccountInquiryRequest(
            @NotBlank(message = "bankCode is required")
            @JsonAlias({"bank_code", "kodeBank", "partnerServiceId"})
            String bankCode,

            @NotBlank(message = "vaNumber is required")
            @JsonAlias({"va_number", "nomorPembayaran", "virtualAccountNo"})
            String vaNumber
    ) {}

    public record AccountInquiryResponse(
            String status,
            String bankCode,
            String vaNumber,
            String customerName,
            BigDecimal amount,
            String message
    ) {}

    @PostMapping
    public ResponseEntity<AccountInquiryResponse> inquireAccount(@Valid @RequestBody AccountInquiryRequest request) {
        InquiryApplicationService.InquiryResult result = inquiryApplicationService.inquireAccount(request);

        HttpStatus status = switch (result.status()) {
            case "SUCCESS" -> HttpStatus.OK;
            case "INVALID_VA", "INVALID_CHARGE" -> HttpStatus.NOT_FOUND;
            case "ERROR" -> result.message() != null && result.message().contains("unavailable") 
                    ? HttpStatus.SERVICE_UNAVAILABLE 
                    : HttpStatus.INTERNAL_SERVER_ERROR;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };

        return ResponseEntity.status(status).body(new AccountInquiryResponse(
                result.status(),
                result.bankCode(),
                result.vaNumber(),
                result.customerName(),
                result.amount(),
                result.message()
        ));
    }
}
