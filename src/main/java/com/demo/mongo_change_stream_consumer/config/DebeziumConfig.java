package com.demo.mongo_change_stream_consumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;

@Configuration
public class DebeziumConfig {

    /**
     * Creates the Debezium configuration bean from application properties.
     * This bean is automatically picked up by the DebeziumMessageProducer.
     */
    @Bean
    public io.debezium.config.Configuration debeziumMongoConfiguration(Environment env) throws IOException {
        return io.debezium.config.Configuration.create()
                .with("name", env.getProperty("debezium.name"))
                .with("connector.class", env.getProperty("debezium.connector.class"))
                .with("offset.storage", env.getProperty("debezium.offset.storage"))
                .with("offset.storage.file.filename", env.getProperty("debezium.offset.storage.file.filename"))
                .with("offset.flush.interval.ms", env.getProperty("debezium.offset.flush.interval.ms"))

                .with("topic.prefix", env.getProperty("debezium.topic.prefix"))
                .with("mongodb.connection.string", env.getProperty("debezium.mongodb.connection.string"))
                .with("database.include.list", env.getProperty("debezium.database.include.list"))
                .with("collection.include.list", env.getProperty("debezium.collection.include.list"))
                .with("capture.mode", env.getProperty("debezium.capture.mode"))
                .with("tasks.max", env.getProperty("debezium.tasks.max"))
                .build();
    }
}