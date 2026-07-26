package com.artivisi.paymentgateway.streams;

import com.artivisi.paymentgateway.domain.event.ChargeCreatedEvent;
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
 * Backup & Recovery:
 * RocksDB mutations automatically stream to Kafka Changelog Topics on the broker.
 * On cold-start or container crash, Kafka Streams re-hydrates RocksDB from changelog topics automatically.
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
        // Register RocksDB KeyValueStateStores (backed by Kafka Changelog topics on broker)
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

        // 1. Process charge-events -> Hydrate charge-state-store in RocksDB
        KStream<String, String> chargeStream = builder.stream(
                chargeEventsTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        chargeStream.process(() -> new Processor<String, String, Void, Void>() {
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
        }, StoreConstants.CHARGE_STATE_STORE);

        // 2. Process va-events -> Hydrate va-registry-store in RocksDB
        KStream<String, String> vaStream = builder.stream(
                vaEventsTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        vaStream.process(() -> new Processor<String, String, Void, Void>() {
            private KeyValueStore<String, String> vaStore;

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
        }, StoreConstants.VA_REGISTRY_STORE);

        // 3. Process payment-events -> Update idempotency-store & deduct balance in charge-state-store
        KStream<String, String> paymentStream = builder.stream(
                paymentEventsTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        paymentStream.process(() -> new Processor<String, String, Void, Void>() {
            private KeyValueStore<String, String> idempotencyStore;
            private KeyValueStore<String, String> chargeStore;

            @Override
            public void init(ProcessorContext<Void, Void> context) {
                this.idempotencyStore = context.getStateStore(StoreConstants.IDEMPOTENCY_STORE);
                this.chargeStore = context.getStateStore(StoreConstants.CHARGE_STATE_STORE);
            }

            @Override
            public void process(Record<String, String> record) {
                try {
                    String eventJson = record.value();
                    if (eventJson != null) {
                        JsonNode root = objectMapper.readTree(eventJson);
                        if (root.has("bankReference")) {
                            PaymentReceivedEvent payment = objectMapper.treeToValue(root, PaymentReceivedEvent.class);
                            
                            // Mark reference in idempotency store
                            idempotencyStore.put(payment.bankReference(), "PROCESSED");
                            if (payment.externalCorrelationId() != null) {
                                idempotencyStore.put(payment.externalCorrelationId(), "PROCESSED");
                            }

                            // Update charge balance in charge-state-store
                            String existingChargeJson = chargeStore.get(payment.chargeId());
                            if (existingChargeJson != null) {
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
                } catch (Exception e) {
                    log.error("Failed to process payment event in RocksDB topology", e);
                }
            }
        }, StoreConstants.IDEMPOTENCY_STORE, StoreConstants.CHARGE_STATE_STORE);
    }
}
