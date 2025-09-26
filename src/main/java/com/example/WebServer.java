package com.example;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
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

    // Java 11-safe HTML
    private static final String INDEX_HTML =
        "<!doctype html>\n" +
        "<html><head><meta charset=\"utf-8\"/>" +
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>" +
        "<title>Kafka Web Demo</title>" +
        "<style>" +
        ":root{--bg:#0f172a;--card:#ffffff;--muted:#64748b;--primary:#2563eb;--primary-ink:#ffffff;--border:#e5e7eb;--tableStripe:#f8fafc}" +
        "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;background:var(--bg);}" +
        ".container{max-width:960px;margin:0 auto;padding:24px;}" +
        ".heading{color:#e2e8f0;margin:0 0 16px 0;font-weight:600;}" +
        ".sub{color:#94a3b8;margin:0 0 24px 0;}" +
        ".grid{display:grid;grid-template-columns:1fr;gap:16px;}" +
        "@media(min-width:960px){.grid{grid-template-columns:1fr 1fr;}}" +
        ".card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.08);}" +
        ".card h2{margin:0 0 12px 0;font-size:18px;}" +
        ".badge{display:inline-block;font-size:12px;border-radius:999px;padding:4px 10px;margin-left:6px;}" +
        ".badge.producer{background:rgba(37,99,235,.12);color:#1e40af;border:1px solid rgba(37,99,235,.3);}" +
        ".badge.consumer{background:rgba(16,185,129,.12);color:#065f46;border:1px solid rgba(16,185,129,.3);}" +
        ".muted{color:var(--muted)}" +
        "code{background:#f6f8fa;padding:2px 6px;border-radius:6px}" +
        ".row{display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:center}" +
        "input{font-size:16px;padding:10px 12px;border-radius:10px;border:1px solid var(--border);width:100%}" +
        "button{font-size:15px;padding:10px 14px;border-radius:10px;border:1px solid var(--border);cursor:pointer;background:#f8fafc}" +
        ".primary{background:var(--primary);color:var(--primary-ink);border-color:var(--primary)}" +
        ".toolbar{display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap}" +
        "table{border-collapse:collapse;width:100%;background:white;border:1px solid var(--border);border-radius:12px;overflow:hidden}" +
        "thead th{position:sticky;top:0;background:#f1f5f9;font-weight:600;font-size:14px;border-bottom:1px solid var(--border);padding:10px;text-align:left}" +
        "tbody td{padding:10px;border-bottom:1px solid var(--border);font-size:14px}" +
        "tbody tr:nth-child(even){background:var(--tableStripe)}" +
        "footer{color:#94a3b8;text-align:center;margin-top:10px;font-size:12px}" +
        "</style></head><body>" +
        "<div class=\"container\">" +
        "  <h1 class=\"heading\">Kafka Web Demo</h1>" +
        "  <p class=\"sub\">Bootstrap: <code>${BOOTSTRAP}</code> · Topic: <code>${TOPIC}</code></p>" +
        "  <div class=\"grid\">" +
        "    <div class=\"card\">" +
        "      <h2>Producer <span class=\"badge producer\">send messages</span></h2>" +
        "      <div class=\"row\">" +
        "        <input id=\"key\" placeholder=\"key (optional)\"/>" +
        "        <input id=\"value\" placeholder=\"value\"/>" +
        "        <button id=\"sendBtn\" class=\"primary\" onclick=\"send()\">Send</button>" +
        "      </div>" +
        "      <p id=\"status\" class=\"muted\" style=\"margin:8px 0 0 0\"></p>" +
        "    </div>" +
        "    <div class=\"card\">" +
        "      <h2>Consumer <span class=\"badge consumer\">receive messages</span></h2>" +
        "      <table id=\"tbl\"><thead>" +
        "        <tr><th>Time</th><th>Partition</th><th>Offset</th><th>Key</th><th>Value</th></tr>" +
        "      </thead><tbody></tbody></table>" +
        "      <footer>Latest first. Auto-refresh every 1.5s.</footer>" +
        "    </div>" +
        "  </div>" +
        "</div>" +
        "<script>" +
        "let timer=null;" +
        "async function send(){const key=document.getElementById('key').value;const value=document.getElementById('value').value;" +
        "if(!value){alert('Please enter a value');return;}const btn=document.getElementById('sendBtn');btn.disabled=true;btn.textContent='Sending...';" +
        "try{const params=new URLSearchParams({key,value});const res=await fetch('/produce',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:params});const txt=await res.text();document.getElementById('status').textContent=txt;document.getElementById('value').value='';refresh();}catch(e){document.getElementById('status').textContent='Error: '+e;}finally{btn.disabled=false;btn.textContent='Send';}}" +
        "async function refresh(){const res=await fetch('/messages');const data=await res.json();const tbody=document.querySelector('#tbl tbody');tbody.innerHTML='';for(const m of data){const tr=document.createElement('tr');const dt=new Date(m.ts).toLocaleString();tr.innerHTML='<td>'+dt+'</td><td>'+m.partition+'</td><td>'+m.offset+'</td><td>'+(m.key||'')+'</td><td>'+(m.value||'')+'</td>';tbody.appendChild(tr);}}" +
        "document.getElementById('value').addEventListener('keydown',e=>{if(e.key==='Enter'){send();}});" +
        "document.getElementById('key').addEventListener('keydown',e=>{if(e.key==='Enter'){send();}});" +
        "timer=setInterval(refresh,1500);refresh();" +
        "</script>" +
        "</body></html>";

    public WebServer(AppConfig config, KafkaProducerService producerService, Deque<MessageRecord> recent) {
        this.config = config;
        this.producerService = producerService;
        this.recent = recent;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(config.port), 0);
        server.createContext("/", this::handleIndex);
        server.createContext("/produce", this::handleProduce);
        server.createContext("/messages", this::handleMessages);
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

    private void handleIndex(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            HttpUtil.writeString(ex, 405, "Method Not Allowed");
            return;
        }
        String html = INDEX_HTML
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

    @Override
    public void close() {
        try { if (server != null) server.stop(0); } catch (Exception ignored) {}
    }
}


