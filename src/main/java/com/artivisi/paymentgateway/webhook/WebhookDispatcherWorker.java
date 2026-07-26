package com.artivisi.paymentgateway.webhook;

import com.artivisi.paymentgateway.domain.event.PaymentReceivedEvent;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class WebhookDispatcherWorker {

    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcherWorker.class);

    private final ObjectMapper objectMapper;

    public WebhookDispatcherWorker(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "${app.topics.payment-events:payment-events}",
        groupId = "payment-gateway-webhook-dispatcher"
    )
    public void consumePaymentEventForWebhook(String eventJson) {
        try {
            JsonNode root = objectMapper.readTree(eventJson);

            if (root.has("bankReference") && !root.has("existingBankCode")) {
                PaymentReceivedEvent event = objectMapper.treeToValue(root, PaymentReceivedEvent.class);

                log.info("Webhook Dispatcher processing webhook for client application: chargeId={}, amount={}, bank={}",
                        event.chargeId(), event.amount(), event.bankCode());

                // Simulated HTTP Webhook Dispatch
                dispatchWebhookToClient(event);
            }
        } catch (Exception e) {
            log.error("Failed to process webhook event dispatch: {}", eventJson, e);
        }
    }

    private void dispatchWebhookToClient(PaymentReceivedEvent event) {
        log.info("Successfully dispatched signed HTTP POST webhook to client application for eventId: {}", event.eventId());
    }
}
