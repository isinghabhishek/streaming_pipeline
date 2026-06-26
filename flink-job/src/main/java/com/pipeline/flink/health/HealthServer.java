package com.pipeline.flink.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lightweight HTTP server exposing two endpoints:
 *
 * <ul>
 *   <li>{@code GET /health} — returns {@code 200 OK} when the job is running,
 *       {@code 503 Service Unavailable} when stopped or in an error state.</li>
 *   <li>{@code GET /metrics} — returns Prometheus text-format metrics collected
 *       from the {@link MetricsRegistry}.</li>
 * </ul>
 *
 * <p>The server runs on a single-threaded executor and is started / stopped
 * by the main {@link com.pipeline.flink.StreamingJob}.
 *
 * <p>Requirement 3.8: health endpoint on port 8081.
 * Requirement 9.2: Prometheus-compatible metrics.
 */
public class HealthServer {

    private static final Logger LOG = LoggerFactory.getLogger(HealthServer.class);

    public enum JobStatus { RUNNING, STOPPED, ERROR }

    private final int port;
    private final AtomicReference<JobStatus> status =
            new AtomicReference<>(JobStatus.STOPPED);
    private HttpServer server;

    public HealthServer(int port) {
        this.port = port;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /** Start the HTTP server. Call once before {@code env.execute()}. */
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), /*backlog*/ 8);
        server.createContext("/health",  new HealthHandler());
        server.createContext("/metrics", new MetricsHandler());
        server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "health-server");
            t.setDaemon(true);
            return t;
        }));
        server.start();
        LOG.info("{\"component\":\"HealthServer\",\"level\":\"INFO\","
                + "\"message\":\"Health server started\",\"port\":{}}",
                port);
    }

    /** Shut down the HTTP server gracefully. */
    public void stop() {
        if (server != null) {
            server.stop(1 /* delay seconds */);
        }
    }

    // -------------------------------------------------------------------------
    // Status management
    // -------------------------------------------------------------------------

    public void setRunning() { status.set(JobStatus.RUNNING); }
    public void setStopped() { status.set(JobStatus.STOPPED); }
    public void setError()   { status.set(JobStatus.ERROR); }
    public JobStatus getStatus() { return status.get(); }

    // -------------------------------------------------------------------------
    // HTTP handlers
    // -------------------------------------------------------------------------

    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            JobStatus current = status.get();
            int httpCode;
            String body;

            if (current == JobStatus.RUNNING) {
                httpCode = 200;
                body = "{\"status\":\"running\"}";
            } else {
                httpCode = 503;
                body = "{\"status\":\"" + current.name().toLowerCase() + "\"}";
            }

            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(httpCode, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String metrics = MetricsRegistry.toPrometheusText();
            byte[] bytes = metrics.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type",
                    "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}
