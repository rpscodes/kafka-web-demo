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
        ".card.flow{grid-column:1 / -1}" +
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
        ".tableWrap{max-height:320px;overflow:auto;border:1px solid var(--border);border-radius:12px}" +
        ".pager{display:flex;gap:8px;align-items:center;justify-content:flex-end;margin-top:8px}" +
        "#flow{width:100%;height:clamp(380px,56vh,640px);border:1px solid var(--border);border-radius:12px;background:white}" +
        ".label{font-size:16px;fill:#334155}" +
        ".part{fill:#f8fafc;stroke:#cbd5e1;stroke-width:1.5}" +
        ".plabel{font-size:16px;fill:#334155;font-weight:700}" +
        ".pcount{font-size:15px;fill:#475569}" +
        ".keys{font-size:14px;fill:#64748b}" +
        ".legend{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin:8px 0}" +
        ".legend .item{display:flex;gap:6px;align-items:center;font-size:12px;color:#475569}" +
        ".legend .swatch{width:10px;height:10px;border-radius:50%}" +
        ".flowControls{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:8px 0}" +
        ".flowControls input{width:auto}" +
        "</style></head><body>" +
        "<div class=\"container\">" +
        "  <h1 class=\"heading\">Kafka Web Demo</h1>" +
        "  <p class=\"sub\">Bootstrap: <code>${BOOTSTRAP}</code> · Topic: <code>${TOPIC}</code></p>" +
        "  <div class=\"grid\">" +
        "    <div class=\"card flow\">" +
        "      <h2>Animated flow <span class=\"badge producer\">producer</span> → <span class=\"badge consumer\">consumer</span></h2>" +
        "      <div class=\"flowControls\">" +
        "        <input id=\"key\" placeholder=\"key (optional)\"/>" +
        "        <input id=\"value\" placeholder=\"value\"/>" +
        "        <button id=\"sendBtn\" class=\"primary\" onclick=\"send()\">Send</button>" +
        "        <span class=\"muted\">Quick Send:</span>" +
        "        <button onclick=\"bulk(5)\">5</button>" +
        "        <button onclick=\"bulk(10)\">10</button>" +
        "        <button onclick=\"bulk(30)\">30</button>" +
        "        <button onclick=\"bulk(50)\">50</button>" +
        "        <button id=\"resetBtn\">Reset</button>" +
        "        <span id=\"status\" class=\"muted\"></span>" +
        "      </div>" +
        "      <div class=\"legend\" id=\"legend\"></div>" +
        "      <svg id=\"flow\" viewBox=\"0 0 1200 420\"></svg>" +
        "    </div>" +
        "    <div class=\"card\">" +
        "      <h2>Consumer <span class=\"badge consumer\">receive messages</span></h2>" +
        "      <div class=\"tableWrap\">" +
        "      <table id=\"tbl\"><thead>" +
        "        <tr><th>Time</th><th>Partition</th><th>Offset</th><th>Key</th><th>Value</th></tr>" +
        "      </thead><tbody></tbody></table>" +
        "      </div>" +
        "      <div class=\"pager\">" +
        "        <label class=\"muted\">Rows: <select id=\"pageSize\"><option>10</option><option selected>20</option><option>50</option><option>100</option></select></label>" +
        "        <button id=\"prevBtn\">Prev</button>" +
        "        <span id=\"pageInfo\" class=\"muted\"></span>" +
        "        <button id=\"nextBtn\">Next</button>" +
        "      </div>" +
        "      <footer>Latest first. Auto-refresh every 1.5s. Use pagination to browse.</footer>" +
        "    </div>" +
        "  </div>" +
        "</div>" +
        "<script>" +
        "let timer=null;" +
        "const seen=new Set();let flowInit=false;let laneIndexByPartition={};let lanesY=[];let parts=[];let countEls=[];let keysEls=[];let geom=null;let producedTotal=0;let consumedTotal=0;let prodText=null;let consText=null;let knownIds=new Set();let baseOffsetByPartition={};let baseCountByPartition={};" +
        "const KEY_POOL=['k0','k1','k2','k3','k4','k5','k6','k7'];" +
        "const PALETTE=['#2563eb','#f97316','#16a34a','#7c3aed','#ef4444','#14b8a6','#eab308','#ec4899'];" +
        "const TOPIC='${TOPIC}';let initialized=false;let maxOffsetByPartition={};try{const s=sessionStorage.getItem('maxSeen:'+TOPIC);if(s)maxOffsetByPartition=JSON.parse(s)||{};}catch(_){}" +
        "function clearState(){try{sessionStorage.removeItem('maxSeen:'+TOPIC);}catch(_){}maxOffsetByPartition={};initialized=false;producedTotal=0;consumedTotal=0;knownIds=new Set();if(prodText)prodText.textContent='0 sent';if(consText)consText.textContent='0 received';lastData=[];const tbody=document.querySelector('#tbl tbody');if(tbody)tbody.innerHTML='';flowInit=false;}" +
        "let page=1;let pageSize=20;let lastData=[];" +
        "function createSvg(tag,attrs){const el=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const k in attrs){el.setAttribute(k,attrs[k]);}return el;}" +
        "function hashIndex(str){if(!str)return 0;let h=0;for(let i=0;i<str.length;i++){h=(h*31+str.charCodeAt(i))|0;}return Math.abs(h)%PALETTE.length;}" +
        "function colorForKey(str){return PALETTE[hashIndex(str)];}" +
        "function ensureFlowFrame(data){const svg=document.getElementById('flow');if(!svg)return;const ps=[...new Set(data.map(m=>m.partition))];if(ps.length===0){ps.push(0);}ps.sort((a,b)=>a-b);const changed=JSON.stringify(ps)!==JSON.stringify(parts);if(flowInit&&!changed){return;}parts=ps;svg.innerHTML='';laneIndexByPartition={};lanesY=[];countEls=[];keysEls=[];const vb=svg.viewBox&&svg.viewBox.baseVal?svg.viewBox.baseVal:null;const W=vb&&vb.width?vb.width:1200;const H=vb&&vb.height?vb.height:420;const left=Math.round(0.08*W);const right=Math.round(0.92*W);const top=Math.round(0.2*H);const bottom=Math.round(0.88*H);const centerY=Math.round((top+bottom)/2);const r=Math.max(8,Math.round(0.03*H));const nodeR=Math.round(r*1.6);const laneLeft=left+Math.round(0.07*W);const laneRight=right-Math.round(0.07*W);geom={left,right,top,bottom,W,H,r,nodeR,laneLeft,laneRight,producerX:left,producerY:centerY,consumerX:right,consumerY:centerY};const gap=(bottom-top)/(parts.length>0?parts.length:1);for(let i=0;i<parts.length;i++){laneIndexByPartition[parts[i]]=i;const y=Math.round(top+i*gap);lanesY.push(y);const l1=createSvg('line',{x1:laneLeft,y1:y,x2:laneRight,y2:y,stroke:'#dbe1e8','stroke-width':4});svg.appendChild(l1);const connL=createSvg('line',{x1:left+nodeR+2,y1:centerY,x2:laneLeft,y2:y,stroke:'#e5e7eb','stroke-width':2});svg.appendChild(connL);const connR=createSvg('line',{x1:laneRight,y1:y,x2:right-nodeR-2,y2:centerY,stroke:'#e5e7eb','stroke-width':2});svg.appendChild(connR);const pw=Math.max(Math.round(0.34*W),320);const ph=Math.max(Math.round(0.2*H),56);const px=Math.round((W/2)-(pw/2));const rect=createSvg('rect',{x:px,y:y-Math.round(ph/2),width:pw,height:ph,rx:12,ry:12,class:'part'});svg.appendChild(rect);const pl=createSvg('text',{x:px+Math.round(pw/2),y:y-8,'text-anchor':'middle',class:'plabel'});pl.textContent='P'+parts[i];svg.appendChild(pl);const cnt=createSvg('text',{x:px+Math.round(pw/2),y:y+8,'text-anchor':'middle',class:'pcount'});cnt.textContent='0 msgs';svg.appendChild(cnt);countEls.push(cnt);const keys=createSvg('text',{x:px+Math.round(pw/2),y:y+26,'text-anchor':'middle',class:'keys'});keys.textContent='';svg.appendChild(keys);keysEls.push(keys);}const prodNode=createSvg('circle',{cx:left,cy:centerY,r:nodeR,fill:'#e2e8f0',stroke:'#cbd5e1'});svg.appendChild(prodNode);const consNode=createSvg('circle',{cx:right,cy:centerY,r:nodeR,fill:'#e2e8f0',stroke:'#cbd5e1'});svg.appendChild(consNode);const prod=createSvg('text',{x:left,y:centerY-nodeR-10,'text-anchor':'middle',class:'label'});prod.textContent='Producer';svg.appendChild(prod);const cons=createSvg('text',{x:right,y:centerY-nodeR-10,'text-anchor':'middle',class:'label'});cons.textContent='Consumer';svg.appendChild(cons);prodText=createSvg('text',{x:left,y:centerY+nodeR+14,'text-anchor':'middle',class:'pcount'});prodText.textContent=producedTotal+' sent';svg.appendChild(prodText);consText=createSvg('text',{x:right,y:centerY+nodeR+14,'text-anchor':'middle',class:'pcount'});consText.textContent=consumedTotal+' received';svg.appendChild(consText);const mid=createSvg('text',{x:W/2,y:Math.round(0.14*H),'text-anchor':'middle',class:'label'});mid.textContent='Topic: ${TOPIC}';svg.appendChild(mid);const legend=document.getElementById('legend');legend.innerHTML='';for(const k of KEY_POOL){const div=document.createElement('div');div.className='item';const sw=document.createElement('span');sw.className='swatch';sw.style.background=colorForKey(k);const label=document.createElement('span');label.textContent=k;div.appendChild(sw);div.appendChild(label);legend.appendChild(div);}flowInit=true;}" +
        "function laneYFor(partition){if(!(partition in laneIndexByPartition)){return 180;}return lanesY[laneIndexByPartition[partition]]||180;}" +
        "function animatePill(m,delay){const svg=document.getElementById('flow');if(!svg||!geom)return;const baseY=laneYFor(m.partition);const y=baseY+(Math.random()*12-6);const circle=createSvg('circle',{cx:geom.laneLeft,cy:y,r:geom.r,fill:colorForKey(m.key||''),opacity:0.95});setTimeout(()=>{svg.appendChild(circle);const start=performance.now();const duration=1200+Math.random()*600;const total=geom.laneRight-geom.laneLeft;function step(ts){const p=Math.min(1,(ts-start)/duration);const x=geom.laneLeft+p*total;circle.setAttribute('cx',x);if(p<1){requestAnimationFrame(step);}else{setTimeout(()=>{try{svg.removeChild(circle);}catch(e){}},800);}}requestAnimationFrame(step);},delay||0);}" +
        "function updateFlow(data){ensureFlowFrame(data);if(!initialized){const highest={};for(const m of data){const id=m.partition+':'+m.offset;if(!knownIds.has(id))knownIds.add(id);highest[m.partition]=Math.max(highest[m.partition]??-1,m.offset);}for(const p of parts){maxOffsetByPartition[p]=highest[p]??-1;}try{sessionStorage.setItem('maxSeen:'+TOPIC,JSON.stringify(maxOffsetByPartition));}catch(_){}initialized=true;return;}const counts={};const keysByPart={};for(const p of parts){counts[p]=0;keysByPart[p]=new Set();}let newSeen=0;for(const m of data){counts[m.partition]++;if(m.key)keysByPart[m.partition].add(m.key);const id=m.partition+':'+m.offset;if(!knownIds.has(id)){knownIds.add(id);newSeen++;}}if(newSeen>0){consumedTotal+=newSeen;if(consText){consText.textContent=consumedTotal+' received';}}for(let i=0;i<parts.length;i++){const p=parts[i];if(countEls[i])countEls[i].textContent=counts[p]+\" msgs\";if(keysEls[i]){const ks=[...keysByPart[p]].slice(0,8).join(', ');keysEls[i].textContent=ks;}}const toAnimate=[];for(let i=data.length-1;i>=0;i--){const m=data[i];const cur=maxOffsetByPartition[m.partition]??-1;if(m.offset>cur){toAnimate.push(m);maxOffsetByPartition[m.partition]=m.offset;}}try{sessionStorage.setItem('maxSeen:'+TOPIC,JSON.stringify(maxOffsetByPartition));}catch(_){}let delay=0;const inc=Math.max(2,Math.floor(900/Math.max(1,toAnimate.length)));for(let i=toAnimate.length-1;i>=0;i--){animatePill(toAnimate[i],delay);delay+=inc;}}" +
        "async function send(){const key=document.getElementById('key').value;const value=document.getElementById('value').value;" +
        "if(!value){alert('Please enter a value');return;}const btn=document.getElementById('sendBtn');btn.disabled=true;btn.textContent='Sending...';" +
        "try{const params=new URLSearchParams({key,value});const res=await fetch('/produce',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:params});const txt=await res.text();document.getElementById('status').textContent=txt;document.getElementById('value').value='';producedTotal++;if(prodText){prodText.textContent=producedTotal+' sent';}refresh();}catch(e){document.getElementById('status').textContent='Error: '+e;}finally{btn.disabled=false;btn.textContent='Send';}}" +
        "async function bulk(n){const btn=document.getElementById('sendBtn');btn.disabled=true;const info=document.getElementById('status');info.textContent='Sending '+n+' messages...';try{for(let i=0;i<n;i++){const key=KEY_POOL[i%KEY_POOL.length];const value='auto-'+Date.now()+'-'+i;const params=new URLSearchParams({key,value});fetch('/produce',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:params});await new Promise(r=>setTimeout(r,12));}producedTotal+=n;if(prodText){prodText.textContent=producedTotal+' sent';}info.textContent='Sent '+n+' messages.';refresh();}catch(e){info.textContent='Error: '+e;}finally{btn.disabled=false;}}" +
        "function renderTable(){const tbody=document.querySelector('#tbl tbody');tbody.innerHTML='';const ps=pageSize;const start=(page-1)*ps;const slice=lastData.slice(start,start+ps);for(const m of slice){const tr=document.createElement('tr');const dt=new Date(m.ts).toLocaleString();tr.innerHTML='<td>'+dt+'</td><td>'+m.partition+'</td><td>'+m.offset+'</td><td>'+(m.key||'')+'</td><td>'+(m.value||'')+'</td>';tbody.appendChild(tr);}const total=lastData.length;const pages=Math.max(1,Math.ceil(total/ps));if(page>pages)page=pages;document.getElementById('pageInfo').textContent='Page '+page+' / '+pages+' · '+total+' rows';document.getElementById('prevBtn').disabled=page<=1;document.getElementById('nextBtn').disabled=page>=pages;}" +
        "async function refresh(){const res=await fetch('/messages');const data=await res.json();if(!initialized){updateFlow(data);lastData=[];renderTable();}else{updateFlow(data);lastData=data;renderTable();}}" +
        "async function resetAndRefresh(){try{await fetch('/reset',{method:'POST'});clearState();refresh();}catch(e){console.error('Reset failed:',e);}}" +
        "document.getElementById('prevBtn').addEventListener('click',()=>{if(page>1){page--;renderTable();}});" +
        "document.getElementById('nextBtn').addEventListener('click',()=>{page++;renderTable();});" +
        "document.getElementById('pageSize').addEventListener('change',e=>{pageSize=parseInt(e.target.value,10);page=1;renderTable();});" +
        "document.getElementById('value').addEventListener('keydown',e=>{if(e.key==='Enter'){send();}});" +
        "document.getElementById('key').addEventListener('keydown',e=>{if(e.key==='Enter'){send();}});" +
        "document.getElementById('resetBtn').addEventListener('click',resetAndRefresh);" +
        "resetAndRefresh();" +
        "timer=setInterval(refresh,1500);" +
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


