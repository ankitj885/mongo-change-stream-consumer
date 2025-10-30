package com.demo.mongo_change_stream_consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class DebeziumEventProcessor {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumEventProcessor.class);

    /**
     * This Transformer listens to 'debeziumChannel' and outputs to 'routingChannel'.
     * It processes the raw Debezium event, extracts the 'after' (or 'before'
     * for deletes) block, and adds a header for routing.
     */
    @Transformer(inputChannel = "debeziumChannel", outputChannel = "routingChannel")
    public Message<String> processDebeziumEvent(Message<ChangeEvent<String, String>> message) {

        ChangeEvent<String, String> event = message.getPayload();
        String jsonValue = event.value();

        try {
            // 1. PARSE THE DEBEZIUM JSON ENVELOPE
            JsonNode payload = objectMapper.readTree(jsonValue);
            String op = payload.path("op").asText(); // "c", "u", "d"

            String mainData;

            // 2. SEPARATE THE "MAIN DATA"
            if ("d".equals(op)) {
                // For deletes, send the "before" data
                mainData = payload.path("before").toString();
            } else {
                // For create ("c") or update ("u"), send the "after" data
                mainData = payload.path("after").toString();
            }

            // 3. GET ROUTING INFORMATION
            // Destination format: "server-name.database-name.collection-name"
            String destination = event.destination();
            String collectionName = destination.substring(destination.lastIndexOf('.') + 1);

            LOGGER.info("Processing event for collection: [{}], Op: [{}]", collectionName, op);

            // 4. BUILD THE NEW, SIMPLIFIED MESSAGE
            return MessageBuilder.withPayload(mainData)
                    .setHeader("kafka_topic_key", collectionName) // Header for routing
                    .setHeader("debezium_op", op) // Extra metadata
                    .build();

        } catch (IOException e) {
            LOGGER.error("Failed to parse Debezium event: {}", jsonValue, e);
            return null; // Or send to an error channel
        }
    }
}