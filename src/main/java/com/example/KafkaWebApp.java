package com.example;

import java.util.ArrayDeque;
import java.util.Deque;

public class KafkaWebApp {

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnv();
        System.out.println("[INFO] Starting Kafka Web Demo on port " + config.port);

        Deque<MessageRecord> recent = new ArrayDeque<>();

        try (KafkaProducerService producer = new KafkaProducerService(config);
             KafkaConsumerService consumer = new KafkaConsumerService(config, recent);
             WebServer server = new WebServer(config, producer, recent)) {

            consumer.start();
            server.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    server.close();
                    consumer.close();
                    producer.close();
                } catch (Exception ignored) {}
                System.out.println("[INFO] Shutting down.");
            }));

            // Keep main alive
            Thread.currentThread().join();
        }
    }
}


