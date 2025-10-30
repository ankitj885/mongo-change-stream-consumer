package com.demo.mongo_change_stream_consumer.config;

import io.debezium.engine.ChangeEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.debezium.inbound.DebeziumMessageProducer;
import org.springframework.messaging.MessageChannel;

@Configuration
@EnableIntegration // Enables Spring Integration
public class DebeziumIntegrationConfig {

    // The channel where raw Debezium events will be sent
    @Bean
    public MessageChannel debeziumChannel() {
        return new DirectChannel();
    }

    // The channel where our *processed* data will be sent for routing
    @Bean
    public MessageChannel routingChannel() {
        return new DirectChannel();
    }

    /**
     * The Debezium Inbound Channel Adapter.
     * This bean connects to the Debezium engine (using the Configuration bean
     * from DebeziumConfig.java) and starts listening.
     * It sends all raw ChangeEvents to the "debeziumChannel".
     */
    @Bean
    public DebeziumMessageProducer debeziumMessageProducer(
            io.debezium.config.Configuration debeziumMongoConfiguration) {

        DebeziumMessageProducer producer = new DebeziumMessageProducer(debeziumMongoConfiguration);
        producer.setOutputChannel(debeziumChannel());
        producer.setPayloadType(ChangeEvent.class);
        return producer;
    }
}