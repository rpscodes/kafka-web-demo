package com.example;

import java.util.Locale;

public class MessageRecord {
    public final long timestamp;
    public final int partition;
    public final long offset;
    public final String key;
    public final String value;

    public MessageRecord(long timestamp, int partition, long offset, String key, String value) {
        this.timestamp = timestamp;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    private String escape(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '"': sb.append("\\\""); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 32) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        sb.append("\"");
        return sb.toString();
    }

    public String toJson() {
        return String.format(Locale.ROOT,
            "{\"ts\":%d,\"partition\":%d,\"offset\":%d,\"key\":%s,\"value\":%s}",
            timestamp, partition, offset,
            key == null ? "null" : escape(key),
            value == null ? "null" : escape(value));
    }
}


