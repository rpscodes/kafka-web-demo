package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class KafkaProducerService implements AutoCloseable {
    private final AppConfig config;
    private final KafkaProducer<String, String> producer;

    public KafkaProducerService(AppConfig config) {
        this.config = config;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "web-producer-" + UUID.randomUUID());
        this.producer = new KafkaProducer<>(props);
    }

    public Future<RecordMetadata> send(String key, String value) {
        ProducerRecord<String, String> rec = (key == null || key.isEmpty())
            ? new ProducerRecord<>(config.topic, value)
            : new ProducerRecord<>(config.topic, key, value);
        return producer.send(rec);
    }

    @Override
    public void close() {
        try { producer.close(); } catch (Exception ignored) {}
    }
}


