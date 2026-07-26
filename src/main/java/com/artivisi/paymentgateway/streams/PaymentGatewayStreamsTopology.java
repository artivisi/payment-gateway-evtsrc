package com.artivisi.paymentgateway.streams;

import com.artivisi.paymentgateway.domain.event.DoubleSettlementDetectedEvent;
import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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

@Component
public class PaymentGatewayStreamsTopology {

    private static final Logger log = LoggerFactory.getLogger(PaymentGatewayStreamsTopology.class);

    private final ObjectMapper objectMapper;

    @Value("${app.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public PaymentGatewayStreamsTopology(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        // Register RocksDB KeyValueStateStores
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

        KStream<String, String> paymentStream = builder.stream(
                paymentEventsTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Process payment events stream
        paymentStream.foreach((chargeId, eventJson) -> {
            try {
                JsonNode root = objectMapper.readTree(eventJson);

                // Verify if it is a PaymentReceivedEvent
                if (root.has("bankReference") && !root.has("existingBankCode")) {
                    PaymentReceivedEvent payment = objectMapper.treeToValue(root, PaymentReceivedEvent.class);
                    log.info("Kafka Streams processing PaymentReceivedEvent: chargeId={}, bank={}, ref={}, amount={}",
                            payment.chargeId(), payment.bankCode(), payment.bankReference(), payment.amount());
                }
            } catch (Exception e) {
                log.error("Kafka Streams error parsing payment event: {}", eventJson, e);
            }
        });
    }
}
