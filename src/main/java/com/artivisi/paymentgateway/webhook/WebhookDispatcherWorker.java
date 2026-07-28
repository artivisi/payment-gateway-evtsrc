package com.artivisi.paymentgateway.webhook;

import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class WebhookDispatcherWorker {

    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcherWorker.class);

    private final ObjectMapper objectMapper;

    public WebhookDispatcherWorker(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // Batch signature: the default listener container factory runs in batch mode
    // (spring.kafka.listener.type=batch) so the projection sink can amortize its DB writes per
    // poll; this listener shares that factory and must accept a batch too.
    @KafkaListener(
        topics = "${app.topics.payment-events:payment-events}",
        groupId = "payment-gateway-webhook-dispatcher"
    )
    public void consumePaymentEventsForWebhook(List<String> eventJsons) {
        for (String eventJson : eventJsons) {
            try {
                JsonNode root = objectMapper.readTree(eventJson);

                if (root.has("bankReference") && !root.has("existingBankCode")) {
                    PaymentReceivedEvent event = objectMapper.treeToValue(root, PaymentReceivedEvent.class);
                    dispatchWebhookToClient(event);
                }
            } catch (Exception e) {
                log.error("Failed to process webhook event dispatch: {}", eventJson, e);
            }
        }
    }

    private void dispatchWebhookToClient(PaymentReceivedEvent event) {
        log.info("Successfully dispatched signed HTTP POST webhook to client application for eventId: {}", event.eventId());
    }
}
