package com.artivisi.paymentgateway.service;

import com.artivisi.paymentgateway.settlement.ChargeSettlementStore;
import com.artivisi.paymentgateway.web.api.InquiryController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * Application Service for Bank Account Inquiry.
 * Reads directly from {@link ChargeSettlementStore} -- the same local RocksDB transaction store
 * the write path uses, so an inquiry can never observe state that disagrees with what a payment
 * against the same VA would see.
 */
@Service
public class InquiryApplicationService {

    private static final Logger log = LoggerFactory.getLogger(InquiryApplicationService.class);

    private final ChargeSettlementStore settlementStore;
    private final ObjectMapper objectMapper;

    public InquiryApplicationService(ChargeSettlementStore settlementStore, ObjectMapper objectMapper) {
        this.settlementStore = settlementStore;
        this.objectMapper = objectMapper;
    }

    public record InquiryResult(
            String status,
            String bankCode,
            String vaNumber,
            String customerName,
            BigDecimal amount,
            String message
    ) {}

    /**
     * Controller-facing overload. Delegates to the primitive-argument method so future
     * non-generic adapters (e.g. a dedicated BSI controller) can call this service without
     * depending on InquiryController's inner request type.
     */
    public InquiryResult inquireAccount(InquiryController.AccountInquiryRequest request) {
        return inquireAccount(request.bankCode(), request.vaNumber());
    }

    public InquiryResult inquireAccount(String bankCode, String vaNumber) {
        String effectiveBankCode = bankCode != null ? bankCode : "GENERIC_BANK";

        try {
            Optional<String> chargeId = settlementStore.resolveChargeId(effectiveBankCode, vaNumber);
            if (chargeId.isEmpty()) {
                return new InquiryResult("INVALID_VA", effectiveBankCode, vaNumber, null, null, "Virtual account not found");
            }

            Optional<String> chargeJson = settlementStore.getChargeJson(chargeId.get());
            if (chargeJson.isEmpty()) {
                return new InquiryResult("INVALID_CHARGE", effectiveBankCode, vaNumber, null, null, "Charge record not found for VA");
            }

            JsonNode node = objectMapper.readTree(chargeJson.get());
            String customerName = node.hasNonNull("description") ? node.get("description").asString() : "Customer";
            BigDecimal totalAmount = node.hasNonNull("totalAmount") ? new BigDecimal(node.get("totalAmount").asString()) : BigDecimal.ZERO;

            log.info("Account inquiry resolved for bankCode: {}, vaNumber: {}, chargeId: {}",
                    effectiveBankCode, vaNumber, chargeId.get());

            return new InquiryResult("SUCCESS", effectiveBankCode, vaNumber, customerName, totalAmount, "Account inquiry successful");

        } catch (Exception e) {
            log.error("Account inquiry failed for vaNumber: {}", vaNumber, e);
            return new InquiryResult("ERROR", effectiveBankCode, vaNumber, null, null, "Inquiry processing error: " + e.getMessage());
        }
    }
}
