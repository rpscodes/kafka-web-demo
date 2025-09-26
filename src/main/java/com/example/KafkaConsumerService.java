package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Properties;

public class KafkaConsumerService implements AutoCloseable {
    private final AppConfig config;
    private final Deque<MessageRecord> recent;
    private volatile boolean running = true;
    private Thread thread;

    public KafkaConsumerService(AppConfig config, Deque<MessageRecord> sink) {
        this.config = config;
        this.recent = sink;
    }

    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        thread = new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(config.topic));
                while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : records) {
                        MessageRecord rec = new MessageRecord(r.timestamp(), r.partition(), r.offset(), r.key(), r.value());
                        recent.addFirst(rec);
                        while (recent.size() > config.maxMessages) {
                            recent.removeLast();
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Consumer error: " + e);
            }
        }, "consumer-thread");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void close() {
        running = false;
        try {
            if (thread != null) thread.join(1000);
        } catch (InterruptedException ignored) {}
    }
}


