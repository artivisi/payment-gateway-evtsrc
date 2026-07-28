package com.artivisi.paymentgateway.streams;

import com.artivisi.paymentgateway.domain.event.DoubleSettlementDetectedEvent;
import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.artivisi.paymentgateway.domain.event.SiblingVaRegisteredEvent;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
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
import java.time.Instant;
import java.util.UUID;

/**
 * Kafka Streams Topology maintaining embedded off-heap RocksDB state stores.
 *
 * Hydration & Mutation Flows:
 * 1. charge-events -> charge-state-store: Hydrates chargeId, clientId, chargeType, totalAmount
 *    (the original bill amount, never mutated again), cumulativePaid (starts at 0), status
 *    (ACTIVE until terminal), description, timestamp.
 * 2. va-events     -> va-registry-store: Indexes (bankCode_vaNumber -> chargeId) & (vaNumber -> chargeId).
 * 3. payment-events -> idempotency-store & charge-state-store: PaymentEventProcessor is the single
 *    authoritative serialization point per partition key (chargeId). It re-checks idempotency,
 *    re-checks the charge's terminal status, and either applies the payment (cumulativePaid +=
 *    amount, terminal at cumulativePaid >= totalAmount) or emits a DoubleSettlementDetectedEvent
 *    back onto payment-events instead of silently absorbing an overpayment.
 *
 * Every sibling VA of a charge resolves via va-registry-store to the SAME chargeId, so marking
 * the charge PAID here is the sibling-retirement mechanism: a later inquiry or payment attempt
 * against ANY sibling VA observes status PAID. No separate per-VA cancellation bookkeeping exists.
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
        chargeStream.process(() -> new ChargeHydrationProcessor(objectMapper), StoreConstants.CHARGE_STATE_STORE);
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
        // PaymentEventProcessor forwards ONLY when it detects a double settlement; that forwarded
        // record is re-published to payment-events (feeding the projection sink / webhook
        // dispatcher). The processor itself ignores any record already carrying "existingBankCode"
        // so this republish never causes it to reprocess its own output.
        KStream<String, String> doubleSettlementStream = paymentStream.process(
                () -> new PaymentEventProcessor(objectMapper),
                StoreConstants.IDEMPOTENCY_STORE, StoreConstants.CHARGE_STATE_STORE
        );
        doubleSettlementStream.to(paymentEventsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    // ------------------------------------------------------------------------
    // Processors
    // ------------------------------------------------------------------------

    private static class ChargeHydrationProcessor implements Processor<String, String, Void, Void> {
        private final ObjectMapper objectMapper;
        private KeyValueStore<String, String> chargeStore;

        ChargeHydrationProcessor(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.chargeStore = context.getStateStore(StoreConstants.CHARGE_STATE_STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            try {
                String chargeId = record.key();
                String jsonPayload = record.value();
                if (chargeId == null || jsonPayload == null) {
                    return;
                }

                JsonNode root = objectMapper.readTree(jsonPayload);
                if (root.hasNonNull("totalAmount")) {
                    // ChargeCreatedEvent: initial hydration. Seed the accounting fields the
                    // payment processor depends on. totalAmount is the original bill amount and
                    // must never be overwritten again.
                    ObjectNode chargeNode = (ObjectNode) root;
                    chargeNode.put("cumulativePaid", BigDecimal.ZERO.toPlainString());
                    chargeNode.put("status", "ACTIVE");
                    chargeStore.put(chargeId, objectMapper.writeValueAsString(chargeNode));
                } else {
                    // Other charge-events (e.g. ChargeCancelledEvent) carry no accounting fields;
                    // out of scope for this hydration pass.
                    chargeStore.put(chargeId, jsonPayload);
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
                }
            } catch (Exception e) {
                log.error("Failed to hydrate va-registry-store in RocksDB", e);
            }
        }
    }

    /**
     * Single-writer per partition key (chargeId). This is the authoritative serialization point:
     * it re-checks idempotency and the charge's terminal status before applying a payment, closing
     * the race window left open by PaymentApplicationService's request-thread pre-validation.
     */
    private static class PaymentEventProcessor implements Processor<String, String, String, String> {
        private static final String CHARGE_STATUS_PAID = "PAID";

        private final ObjectMapper objectMapper;
        private KeyValueStore<String, String> idempotencyStore;
        private KeyValueStore<String, String> chargeStore;
        private ProcessorContext<String, String> context;

        public PaymentEventProcessor(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
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
                if (root.has("existingBankCode")) {
                    // This is our own DoubleSettlementDetectedEvent output, re-consumed because it
                    // was republished onto payment-events. Nothing to apply.
                    return;
                }

                PaymentReceivedEvent payment = objectMapper.treeToValue(root, PaymentReceivedEvent.class);
                applyPayment(payment);
            } catch (Exception e) {
                log.error("Failed to process payment event in RocksDB topology", e);
            }
        }

        private void applyPayment(PaymentReceivedEvent payment) throws Exception {
            String idempotencyKey = payment.bankCode() + "_" + payment.bankReference();
            if (idempotencyStore.get(idempotencyKey) != null) {
                log.warn("Duplicate bankReference re-observed in topology, skipping apply: {}", idempotencyKey);
                return;
            }

            String chargeJson = chargeStore.get(payment.chargeId());
            if (chargeJson == null) {
                log.error("Payment references unknown chargeId in charge-state-store: {}", payment.chargeId());
                return;
            }

            ObjectNode chargeNode = (ObjectNode) objectMapper.readTree(chargeJson);
            String status = chargeNode.hasNonNull("status") ? chargeNode.get("status").asString() : null;

            if (CHARGE_STATUS_PAID.equals(status)) {
                // Pre-validation race: two callbacks both passed the request-thread check before
                // either event landed here. Never silently absorb the overpayment.
                emitDoubleSettlement(payment);
                recordIdempotency(idempotencyKey, payment.eventId(), payment.chargeId(), payment.amount());
                return;
            }

            BigDecimal totalAmount = chargeNode.hasNonNull("totalAmount")
                    ? new BigDecimal(chargeNode.get("totalAmount").asString())
                    : BigDecimal.ZERO;
            BigDecimal cumulativePaid = chargeNode.hasNonNull("cumulativePaid")
                    ? new BigDecimal(chargeNode.get("cumulativePaid").asString())
                    : BigDecimal.ZERO;

            BigDecimal newCumulativePaid = cumulativePaid.add(payment.amount());
            chargeNode.put("cumulativePaid", newCumulativePaid.toPlainString());
            if (newCumulativePaid.compareTo(totalAmount) >= 0) {
                chargeNode.put("status", CHARGE_STATUS_PAID);
            }

            recordIdempotency(idempotencyKey, payment.eventId(), payment.chargeId(), payment.amount());
            chargeStore.put(payment.chargeId(), objectMapper.writeValueAsString(chargeNode));
        }

        private void emitDoubleSettlement(PaymentReceivedEvent payment) {
            DoubleSettlementDetectedEvent event = new DoubleSettlementDetectedEvent(
                    UUID.randomUUID().toString(),
                    payment.chargeId(),
                    payment.bankCode(),
                    payment.vaNumber(),
                    payment.bankReference(),
                    payment.amount(),
                    payment.bankCode(),
                    Instant.now()
            );
            String eventJson = objectMapper.writeValueAsString(event);
            context.forward(new Record<>(payment.chargeId(), eventJson, System.currentTimeMillis()));
        }

        private void recordIdempotency(String idempotencyKey, String eventId, String chargeId, BigDecimal amount) {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("eventId", eventId);
            node.put("chargeId", chargeId);
            node.put("amount", amount.toPlainString());
            idempotencyStore.put(idempotencyKey, objectMapper.writeValueAsString(node));
        }
    }
}
