package com.demo.mongo_change_stream_consumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Router;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.router.HeaderValueRouter;
import org.springframework.messaging.MessageChannel;

@Configuration
public class RoutingConfig {

    // --- Define the channels that lead to Kafka ---

    @Bean
    public MessageChannel usersTopicChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel ordersTopicChannel() {
        return new DirectChannel();
    }

    // Add more channels here...
    // @Bean
    // public MessageChannel productsTopicChannel() {
    //    return new DirectChannel();
    // }

    /**
     * This Router listens to 'routingChannel'.
     * It reads the "kafka_topic_key" header from the message and
     * sends the message to the channel specified in the mapping.
     */
    @Bean
    @Router(inputChannel = "routingChannel")
    public HeaderValueRouter topicRouter() {
        HeaderValueRouter router = new HeaderValueRouter("kafka_topic_key");

        // Map header value "users" to "usersTopicChannel"
        router.setChannelMapping("users", "usersTopicChannel");

        // Map header value "orders" to "ordersTopicChannel"
        router.setChannelMapping("orders", "ordersTopicChannel");

        // Add more mappings here for other collections
        // router.setChannelMapping("products", "productsTopicChannel");

        return router;
    }
}