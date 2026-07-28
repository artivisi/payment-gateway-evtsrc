package com.artivisi.paymentgateway.service;

import com.artivisi.paymentgateway.streams.StoreConstants;
import com.artivisi.paymentgateway.web.api.InquiryController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * Application Service for Bank Account Inquiry.
 * Encapsulates off-heap RocksDB state store queries (<1ms) and response resolution.
 */
@Service
public class InquiryApplicationService {

    private static final Logger log = LoggerFactory.getLogger(InquiryApplicationService.class);

    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public InquiryApplicationService(ObjectMapper objectMapper) {
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

        if (streamsBuilderFactoryBean == null) {
            log.warn("Kafka Streams builder not initialized; inquiry returning unavailable");
            return new InquiryResult("ERROR", effectiveBankCode, vaNumber, null, null, "Streaming engine unavailable");
        }

        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                return new InquiryResult("ERROR", effectiveBankCode, vaNumber, null, null, "Streams state store re-balancing");
            }

            // 1. Query va-registry-store in local off-heap RocksDB (<1ms)
            ReadOnlyKeyValueStore<String, String> vaStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(StoreConstants.VA_REGISTRY_STORE, QueryableStoreTypes.keyValueStore())
            );
            String lookupKey = effectiveBankCode + "_" + vaNumber;
            String chargeId = vaStore.get(lookupKey);
            if (chargeId == null && vaNumber != null) {
                chargeId = vaStore.get(vaNumber);
            }

            if (chargeId == null) {
                return new InquiryResult("INVALID_VA", effectiveBankCode, vaNumber, null, null, "Virtual account not found");
            }

            // 2. Query charge-state-store in local off-heap RocksDB (<1ms)
            ReadOnlyKeyValueStore<String, String> chargeStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(StoreConstants.CHARGE_STATE_STORE, QueryableStoreTypes.keyValueStore())
            );
            String chargeJson = chargeStore.get(chargeId);
            if (chargeJson == null) {
                return new InquiryResult("INVALID_CHARGE", effectiveBankCode, vaNumber, null, null, "Charge record not found for VA");
            }

            JsonNode node = objectMapper.readTree(chargeJson);
            String customerName = node.hasNonNull("description") ? node.get("description").asString() : "Customer";
            BigDecimal totalAmount = node.hasNonNull("totalAmount") ? new BigDecimal(node.get("totalAmount").asString()) : BigDecimal.ZERO;

            log.info("Account inquiry resolved in RocksDB for bankCode: {}, vaNumber: {}, chargeId: {}",
                    effectiveBankCode, vaNumber, chargeId);

            return new InquiryResult("SUCCESS", effectiveBankCode, vaNumber, customerName, totalAmount, "Account inquiry successful");

        } catch (Exception e) {
            log.error("Account inquiry failed in RocksDB for vaNumber: {}", vaNumber, e);
            return new InquiryResult("ERROR", effectiveBankCode, vaNumber, null, null, "Inquiry processing error: " + e.getMessage());
        }
    }
}
