package com.artivisi.paymentgateway.streams;

import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Kafka Streams Topology maintaining embedded off-heap RocksDB state stores.
 * 
 * Hydration & Mutation Flows:
 * 1. charge-events -> charge-state-store: Hydrates charge ID, clientId, totalAmount, and remaining balance.
 * 2. va-events     -> va-registry-store: Indexes (bankCode_vaNumber -> chargeId) & (vaNumber -> chargeId).
 * 3. payment-events -> idempotency-store & charge-state-store: Records reference idempotency and deducts debt balance.
 * 
 * Clean Architecture Topology Design:
 * Decomposed into modular processor implementations for store registration, charge hydration,
 * VA indexing, and payment processing.
 */
@Component
public class PaymentGatewayStreamsTopology {

    private static final Logger log = LoggerFactory.getLogger(PaymentGatewayStreamsTopology.class);

    private final ObjectMapper objectMapper;

    @Value("${app.topics.charge-events:charge-events}")
    private String chargeEventsTopic;

    @Value("${app.topics.va-events:va-events}")
    private String vaEventsTopic;

    @Value("${app.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public PaymentGatewayStreamsTopology(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        registerStateStores(builder);
        buildChargeHydrationStream(builder);
        buildVaRegistryStream(builder);
        buildPaymentEventStream(builder);
    }

    private void registerStateStores(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, String>> chargeStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(StoreConstants.CHARGE_STATE_STORE),
                Serdes.String(),
                Serdes.String()
        );
        StoreBuilder<KeyValueStore<String, String>> vaRegistryStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(StoreConstants.VA_REGISTRY_STORE),
                Serdes.String(),
                Serdes.String()
        );
        StoreBuilder<KeyValueStore<String, String>> idempotencyStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(StoreConstants.IDEMPOTENCY_STORE),
                Serdes.String(),
                Serdes.String()
        );

        builder.addStateStore(chargeStoreBuilder);
        builder.addStateStore(vaRegistryStoreBuilder);
        builder.addStateStore(idempotencyStoreBuilder);
    }

    private void buildChargeHydrationStream(StreamsBuilder builder) {
        KStream<String, String> chargeStream = builder.stream(
                chargeEventsTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );
        chargeStream.process(ChargeHydrationProcessor::new, StoreConstants.CHARGE_STATE_STORE);
    }

    private void buildVaRegistryStream(StreamsBuilder builder) {
        KStream<String, String> vaStream = builder.stream(
                vaEventsTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );
        vaStream.process(() -> new VaRegistryProcessor(objectMapper), StoreConstants.VA_REGISTRY_STORE);
    }

    private void buildPaymentEventStream(StreamsBuilder builder) {
        KStream<String, String> paymentStream = builder.stream(
                paymentEventsTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );
        paymentStream.process(
                () -> new PaymentEventProcessor(objectMapper),
                StoreConstants.IDEMPOTENCY_STORE, StoreConstants.CHARGE_STATE_STORE
        );
    }

    // ------------------------------------------------------------------------
    // Processors
    // ------------------------------------------------------------------------

    private static class ChargeHydrationProcessor implements Processor<String, String, Void, Void> {
        private KeyValueStore<String, String> chargeStore;

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.chargeStore = context.getStateStore(StoreConstants.CHARGE_STATE_STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            try {
                String chargeId = record.key();
                String jsonPayload = record.value();
                if (chargeId != null && jsonPayload != null) {
                    chargeStore.put(chargeId, jsonPayload);
                    log.info("RocksDB charge-state-store hydrated for chargeId: {}", chargeId);
                }
            } catch (Exception e) {
                log.error("Failed to hydrate charge-state-store in RocksDB", e);
            }
        }
    }

    private static class VaRegistryProcessor implements Processor<String, String, Void, Void> {
        private final ObjectMapper objectMapper;
        private KeyValueStore<String, String> vaStore;

        public VaRegistryProcessor(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.vaStore = context.getStateStore(StoreConstants.VA_REGISTRY_STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            try {
                String chargeId = record.key();
                String eventJson = record.value();
                if (eventJson != null) {
                    SiblingVaRegisteredEvent vaEvent = objectMapper.readValue(eventJson, SiblingVaRegisteredEvent.class);
                    String compoundKey = vaEvent.bankCode() + "_" + vaEvent.vaNumber();
                    vaStore.put(compoundKey, chargeId);
                    vaStore.put(vaEvent.vaNumber(), chargeId);
                    log.info("RocksDB va-registry-store indexed key {} -> chargeId {}", compoundKey, chargeId);
                }
            } catch (Exception e) {
                log.error("Failed to hydrate va-registry-store in RocksDB", e);
            }
        }
    }

    private static class PaymentEventProcessor implements Processor<String, String, Void, Void> {
        private final ObjectMapper objectMapper;
        private KeyValueStore<String, String> idempotencyStore;
        private KeyValueStore<String, String> chargeStore;

        public PaymentEventProcessor(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.idempotencyStore = context.getStateStore(StoreConstants.IDEMPOTENCY_STORE);
            this.chargeStore = context.getStateStore(StoreConstants.CHARGE_STATE_STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            try {
                String eventJson = record.value();
                if (eventJson == null) {
                    return;
                }

                JsonNode root = objectMapper.readTree(eventJson);
                if (!root.has("bankReference")) {
                    return;
                }

                PaymentReceivedEvent payment = objectMapper.treeToValue(root, PaymentReceivedEvent.class);
                recordIdempotency(payment);
                updateChargeBalance(payment);
            } catch (Exception e) {
                log.error("Failed to process payment event in RocksDB topology", e);
            }
        }

        private void recordIdempotency(PaymentReceivedEvent payment) {
            idempotencyStore.put(payment.bankReference(), "PROCESSED");
            if (payment.externalCorrelationId() != null) {
                idempotencyStore.put(payment.externalCorrelationId(), "PROCESSED");
            }
        }

        private void updateChargeBalance(PaymentReceivedEvent payment) throws Exception {
            String existingChargeJson = chargeStore.get(payment.chargeId());
            if (existingChargeJson == null) {
                return;
            }

            ObjectNode chargeNode = (ObjectNode) objectMapper.readTree(existingChargeJson);
            BigDecimal currentAmount = chargeNode.hasNonNull("totalAmount")
                    ? new BigDecimal(chargeNode.get("totalAmount").asString())
                    : BigDecimal.ZERO;
            BigDecimal newAmount = currentAmount.subtract(payment.amount()).max(BigDecimal.ZERO);
            chargeNode.put("totalAmount", newAmount.toPlainString());
            if (newAmount.compareTo(BigDecimal.ZERO) == 0) {
                chargeNode.put("status", "CLOSED");
            }
            chargeStore.put(payment.chargeId(), objectMapper.writeValueAsString(chargeNode));
            log.info("RocksDB charge-state-store balance updated for chargeId: {}, newBalance: {}", payment.chargeId(), newAmount);
        }
    }
}
