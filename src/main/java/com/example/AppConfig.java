package com.example;

public class AppConfig {

    public final String bootstrapServers;
    public final String topic;
    public final String groupId;
    public final int port;
    public final int maxMessages;

    private AppConfig(String bootstrapServers,
                      String topic,
                      String groupId,
                      int port,
                      int maxMessages) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.port = port;
        this.maxMessages = maxMessages;
    }

    public static AppConfig fromEnv() {
        String bootstrap = env("KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap:9092");
        String topic = env("KAFKA_TOPIC", "demo");
        String group = env("KAFKA_GROUP", "web-demo");
        int port = Integer.parseInt(env("PORT", "8080"));
        int max = Integer.parseInt(env("MAX_MESSAGES", "200"));
        return new AppConfig(bootstrap, topic, group, port, max);
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}


