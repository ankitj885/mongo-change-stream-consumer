package com.demo.mongo_change_stream_consumer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.debezium.support.DebeziumHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class ChangeEventHandler {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @ServiceActivator(inputChannel = "debeziumInputChannel")
    public void handleChangeEvent(Message<?> message) {
        String payload = new String((byte[]) message.getPayload());
        String destination = (String) message.getHeaders().get(DebeziumHeaders.DESTINATION);

        // Example routing logic
        if (destination.contains("orders")) {
            kafkaTemplate.send("orders-topic", payload);
        } else if (destination.contains("customers")) {
            kafkaTemplate.send("customers-topic", payload);
        } else {
            kafkaTemplate.send("default-topic", payload);
        }

        System.out.println("Sent to Kafka: " + payload);
    }
}
