package com.example;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class WebServer implements AutoCloseable {
    private final AppConfig config;
    private final KafkaProducerService producerService;
    private final Deque<MessageRecord> recent;
    private HttpServer server;

    // Template loading helper
    private String loadTemplate(String templateName) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("templates/" + templateName)) {
            if (is == null) {
                throw new IOException("Template not found: " + templateName);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
    
    // Static resource serving helper
    private void serveStaticResource(HttpExchange exchange, String resourcePath, String contentType) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("static/" + resourcePath)) {
            if (is == null) {
                HttpUtil.writeString(exchange, 404, "Resource not found");
                return;
            }
            byte[] content = is.readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.getResponseHeaders().set("Cache-Control", "public, max-age=3600");
            exchange.sendResponseHeaders(200, content.length);
            exchange.getResponseBody().write(content);
        }
    }

    public WebServer(AppConfig config, KafkaProducerService producerService, Deque<MessageRecord> recent) {
        this.config = config;
        this.producerService = producerService;
        this.recent = recent;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(config.port), 0);
        server.createContext("/", this::handleIndex);
        server.createContext("/static/", this::handleStatic);
        server.createContext("/produce", this::handleProduce);
        server.createContext("/messages", this::handleMessages);
        server.createContext("/reset", this::handleReset);
        server.createContext("/healthz", e -> {
            try { HttpUtil.writeString(e, 200, "ok"); } catch (IOException ignored) {}
        });
        server.setExecutor(Executors.newFixedThreadPool(4, new ThreadFactory() {
            private int i = 0;
            @Override public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "http-" + (i++));
                t.setDaemon(true);
                return t;
            }
        }));
        server.start();
        System.out.println("[INFO] HTTP listening on :" + config.port);
    }

    private void handleStatic(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            HttpUtil.writeString(ex, 405, "Method Not Allowed");
            return;
        }
        
        String path = ex.getRequestURI().getPath();
        if (path.startsWith("/static/")) {
            path = path.substring(8); // Remove "/static/" prefix
        }
        
        String contentType = "text/plain";
        if (path.endsWith(".css")) {
            contentType = "text/css";
        } else if (path.endsWith(".js")) {
            contentType = "application/javascript";
        } else if (path.endsWith(".html")) {
            contentType = "text/html";
        }
        
        serveStaticResource(ex, path, contentType);
    }

    private void handleIndex(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            HttpUtil.writeString(ex, 405, "Method Not Allowed");
            return;
        }
        
        String html = loadTemplate("index.html")
            .replace("${BOOTSTRAP}", config.bootstrapServers)
            .replace("${TOPIC}", config.topic);
        HttpUtil.writeHtml(ex, 200, html);
    }

    private void handleProduce(HttpExchange ex) throws IOException {
        if (!("POST".equalsIgnoreCase(ex.getRequestMethod()) || "GET".equalsIgnoreCase(ex.getRequestMethod()))) {
            HttpUtil.writeString(ex, 405, "Method Not Allowed");
            return;
        }
        Map<String, String> form = HttpUtil.parseQuery(HttpUtil.bodyString(ex));
        String key = form.getOrDefault("key", "");
        String value = form.get("value");
        if (value == null || value.isEmpty()) {
            HttpUtil.writeString(ex, 400, "Missing 'value'");
            return;
        }
        try {
            var md = producerService.send(key, value).get();
            HttpUtil.writeString(ex, 200, "Produced to partition " + md.partition() + " offset " + md.offset());
        } catch (Exception e) {
            HttpUtil.writeString(ex, 500, "Error: " + e.getMessage());
        }
    }

    private void handleMessages(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            HttpUtil.writeString(ex, 405, "Method Not Allowed");
            return;
        }
        List<MessageRecord> list = new ArrayList<>(recent);
        List<String> out = new ArrayList<>(list.size());
        for (MessageRecord r : list) {
            out.add(r.toJson());
        }
        String json = "[" + String.join(",", out) + "]";
        HttpUtil.writeJson(ex, 200, json);
    }

    private void handleReset(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            HttpUtil.writeString(ex, 405, "Method Not Allowed");
            return;
        }
        // Clear all recent messages
        recent.clear();
        HttpUtil.writeString(ex, 200, "Reset successful");
    }

    @Override
    public void close() {
        try { if (server != null) server.stop(0); } catch (Exception ignored) {}
    }
}


