package com.artivisi.paymentgateway.web.api;

import com.artivisi.paymentgateway.streams.StoreConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

/**
 * Bank Account Inquiry Controller.
 * Pure Kafka / RocksDB solution (<1ms off-heap lookup).
 * ZERO PostgreSQL dependency — PostgreSQL is used strictly for reporting sinks.
 */
@RestController
@RequestMapping({"/api/inquiry", "/api/v1/inquiry"})
public class InquiryController {

    private static final Logger log = LoggerFactory.getLogger(InquiryController.class);

    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public InquiryController(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public record AccountInquiryRequest(
            @NotBlank(message = "bankCode is required")
            String bankCode,
            @NotBlank(message = "vaNumber is required")
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
        if (streamsBuilderFactoryBean == null) {
            log.warn("Kafka Streams builder not initialized; inquiry returning 503");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new AccountInquiryResponse("ERROR", request.bankCode(), request.vaNumber(), null, null, "Streaming engine unavailable"));
        }

        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(new AccountInquiryResponse("ERROR", request.bankCode(), request.vaNumber(), null, null, "Streams state store re-balancing"));
            }

            // 1. Query va-registry-store in local off-heap RocksDB (<1ms)
            ReadOnlyKeyValueStore<String, String> vaStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(StoreConstants.VA_REGISTRY_STORE, QueryableStoreTypes.keyValueStore())
            );
            String lookupKey = request.bankCode() + "_" + request.vaNumber();
            String chargeId = vaStore.get(lookupKey);
            if (chargeId == null) {
                // Fallback to vaNumber alone
                chargeId = vaStore.get(request.vaNumber());
            }

            if (chargeId == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(new AccountInquiryResponse("INVALID_VA", request.bankCode(), request.vaNumber(), null, null, "Virtual account not found"));
            }

            // 2. Query charge-state-store in local off-heap RocksDB (<1ms)
            ReadOnlyKeyValueStore<String, String> chargeStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(StoreConstants.CHARGE_STATE_STORE, QueryableStoreTypes.keyValueStore())
            );
            String chargeJson = chargeStore.get(chargeId);
            if (chargeJson == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(new AccountInquiryResponse("INVALID_CHARGE", request.bankCode(), request.vaNumber(), null, null, "Charge record not found for VA"));
            }

            JsonNode node = objectMapper.readTree(chargeJson);
            String customerName = node.has("description") ? node.get("description").asText("Customer") : "Customer";
            BigDecimal totalAmount = node.has("totalAmount") ? new BigDecimal(node.get("totalAmount").asText("0")) : BigDecimal.ZERO;

            log.info("Account inquiry resolved in RocksDB for bankCode: {}, vaNumber: {}, chargeId: {}",
                    request.bankCode(), request.vaNumber(), chargeId);

            return ResponseEntity.ok(new AccountInquiryResponse("SUCCESS", request.bankCode(), request.vaNumber(), customerName, totalAmount, "Account inquiry successful"));

        } catch (Exception e) {
            log.error("Account inquiry failed in RocksDB for vaNumber: {}", request.vaNumber(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new AccountInquiryResponse("ERROR", request.bankCode(), request.vaNumber(), null, null, "Inquiry processing error: " + e.getMessage()));
        }
    }
}
