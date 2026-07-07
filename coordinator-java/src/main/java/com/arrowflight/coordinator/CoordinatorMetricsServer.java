package com.arrowflight.coordinator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class CoordinatorMetricsServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor;

    private CoordinatorMetricsServer(HttpServer server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
    }

    static CoordinatorMetricsServer start(
            Config config,
            CoordinatorMetrics metrics,
            CoordinatorMetadataStore metadataStore
    ) throws IOException {
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
        server.createContext("/workers", exchange -> respondWorkers(exchange, metadataStore));
        server.createContext("/worker-endpoints", exchange -> respondWorkers(exchange, metadataStore));
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "coordinator-metrics");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.start();
        CoordinatorLog.info("coordinator_metrics_started", Map.of(
                "address", config.coordinatorMetricsAddress.toString()
        ));
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

    private static void respondWorkers(HttpExchange exchange, CoordinatorMetadataStore metadataStore) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            respondWithStatus(exchange, 405, "application/json; charset=utf-8", Json.stringify(Map.of(
                    "error", "method_not_allowed",
                    "message", "worker endpoint registry only supports GET"
            )));
            return;
        }
        try {
            List<WorkerEndpointSnapshot> workers = metadataStore.listWorkerEndpoints();
            LinkedHashMap<String, Object> uris = new LinkedHashMap<>();
            LinkedHashMap<String, Object> addresses = new LinkedHashMap<>();
            for (WorkerEndpointSnapshot worker : workers) {
                uris.put(worker.workerId(), worker.flightUri());
                addresses.put(worker.workerId(), WorkerFlightUri.parse(worker.flightUri()).address());
            }
            LinkedHashMap<String, Object> body = new LinkedHashMap<>();
            body.put("generatedAtMs", Instant.now().toEpochMilli());
            body.put("count", workers.size());
            body.put("uris", uris);
            body.put("addresses", addresses);
            body.put("workers", workers.stream().map(WorkerEndpointSnapshot::toJson).toList());
            respondWithStatus(exchange, 200, "application/json; charset=utf-8", Json.stringify(body));
        } catch (RuntimeException error) {
            int status = error instanceof CoordinatorException coordinatorError ? coordinatorError.status : 500;
            String message = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
            respondWithStatus(exchange, status, "application/json; charset=utf-8", Json.stringify(Map.of(
                    "error", "worker_endpoint_registry_failed",
                    "message", message
            )));
        }
    }

    private static void respondWithStatus(HttpExchange exchange, int status, String contentType, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var response = exchange.getResponseBody()) {
            response.write(bytes);
        }
    }
}
