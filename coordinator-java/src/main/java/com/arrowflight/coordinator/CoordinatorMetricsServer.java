package com.arrowflight.coordinator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class CoordinatorMetricsServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor;

    private CoordinatorMetricsServer(HttpServer server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
    }

    static CoordinatorMetricsServer start(Config config, CoordinatorMetrics metrics) throws IOException {
        if (!config.coordinatorMetricsEnabled) {
            return new CoordinatorMetricsServer(null, null);
        }

        HttpServer server = HttpServer.create(config.coordinatorMetricsAddress, 0);
        server.createContext("/metrics", exchange -> respond(
                exchange,
                "text/plain; version=0.0.4; charset=utf-8",
                metrics.renderPrometheus()
        ));
        server.createContext("/healthz", exchange -> respond(
                exchange,
                "text/plain; charset=utf-8",
                "ok\n"
        ));
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "coordinator-metrics");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.start();
        System.out.printf("coordinator metrics endpoint listening on %s%n", config.coordinatorMetricsAddress);
        return new CoordinatorMetricsServer(server, executor);
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static void respond(HttpExchange exchange, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, bytes.length);
        try (var response = exchange.getResponseBody()) {
            response.write(bytes);
        }
    }
}
