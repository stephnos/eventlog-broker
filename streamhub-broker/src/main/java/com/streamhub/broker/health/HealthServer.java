package com.streamhub.broker.health;

import com.streamhub.broker.BrokerEngine;
import com.streamhub.broker.metrics.BrokerMetrics;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public final class HealthServer implements AutoCloseable {
    private final HttpServer server;

    public HealthServer(int port, BrokerEngine engine) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> {
            byte[] body = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/metrics", exchange -> {
            BrokerMetrics m = engine.metrics();
            String json = String.format("""
                    {
                      "append_count": %d,
                      "fetch_count": %d,
                      "segment_rolls": %d,
                      "buffered_bytes": %d,
                      "rebalance_count": %d,
                      "append_latency_p50_ms": %.2f,
                      "append_latency_p95_ms": %.2f,
                      "append_latency_p99_ms": %.2f,
                      "fetch_latency_p50_ms": %.2f,
                      "fetch_latency_p95_ms": %.2f,
                      "fetch_latency_p99_ms": %.2f
                    }
                    """,
                    m.appendRate(), m.fetchRate(), m.segmentRollCount(), m.bufferedBytes(),
                    engine.coordinator().rebalanceCount(),
                    m.appendLatency().percentile(0.50),
                    m.appendLatency().percentile(0.95),
                    m.appendLatency().percentile(0.99),
                    m.fetchLatency().percentile(0.50),
                    m.fetchLatency().percentile(0.95),
                    m.fetchLatency().percentile(0.99));
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.setExecutor(null);
        server.start();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
