package com.demo.mongo_change_stream_consumer.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    /**
     * Configures the Kafka Producer Factory using properties from application.properties.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        // You can override or add more properties here
        // props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }

    // --- Kafka Outbound Adapters ---

    /**
     * This flow listens on "usersTopicChannel" and publishes
     * messages to the "mongo.events.users" Kafka topic.
     */
    @Bean
    public IntegrationFlow usersKafkaFlow(KafkaTemplate<String, String> kafkaTemplate) {
        return IntegrationFlow.from("usersTopicChannel")
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic("mongo.events.users"))
                .get();
    }

    /**
     * This flow listens on "ordersTopicChannel" and publishes
     * messages to the "mongo.events.orders" Kafka topic.
     */
    @Bean
    public IntegrationFlow ordersKafkaFlow(KafkaTemplate<String, String> kafkaTemplate) {
        return IntegrationFlow.from("ordersTopicChannel")
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic("mongo.events.orders"))
                .get();
    }

    // Add more @Bean IntegrationFlow methods here for each topic/channel
}