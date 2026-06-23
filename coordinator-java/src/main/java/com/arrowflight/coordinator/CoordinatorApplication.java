package com.arrowflight.coordinator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

public final class CoordinatorApplication {
    private final Config config;
    private final TrinoClient trinoClient;
    private final CapabilitySigner capabilitySigner;

    private CoordinatorApplication(Config config) {
        this.config = config;
        this.trinoClient = new TrinoClient(config);
        this.capabilitySigner = new CapabilitySigner(config);
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        CoordinatorApplication app = new CoordinatorApplication(config);
        HttpServer server = HttpServer.create(config.listenAddress, 128);
        server.createContext("/healthz", app::handleHealth);
        server.createContext("/v1/ctas", app.withErrors(app::handleCtas));
        server.createContext("/v1/flight/put-ticket", app.withErrors(app::handlePutTicket));
        server.createContext("/v1/flight/get-ticket", app.withErrors(app::handleGetTicket));
        server.createContext("/v1/config", app.withErrors(app::handleConfig));
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();

        System.out.printf(
                "coordinator listening on %s, trino=%s, worker=%s %s%n",
                config.listenAddress,
                config.trinoUri,
                config.workerId,
                config.workerFlightUri
        );
    }

    private Handler withErrors(ThrowingHandler handler) {
        return exchange -> {
            try {
                handler.handle(exchange);
            } catch (HttpSupport.HttpError error) {
                HttpSupport.sendJson(exchange, error.status, HttpSupport.errorBody(error.getMessage()));
            } catch (IllegalArgumentException error) {
                HttpSupport.sendJson(exchange, 400, HttpSupport.errorBody(error.getMessage()));
            } catch (IllegalStateException error) {
                HttpSupport.sendJson(exchange, 502, HttpSupport.errorBody(error.getMessage()));
            } catch (Exception error) {
                error.printStackTrace(System.err);
                HttpSupport.sendJson(exchange, 500, HttpSupport.errorBody(error.getMessage()));
            }
        };
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            HttpSupport.sendJson(exchange, 405, HttpSupport.errorBody("method must be GET"));
            return;
        }
        HttpSupport.sendJson(exchange, 200, Map.of("status", "ok"));
    }

    private void handleConfig(HttpExchange exchange) throws IOException {
        HttpSupport.requireMethod(exchange, "GET");
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("trinoUri", config.trinoUri.toString());
        body.put("trinoCatalog", config.trinoCatalog);
        body.put("trinoSchema", config.trinoSchema);
        body.put("ctasCatalog", config.ctasCatalog);
        body.put("ctasSchema", config.ctasSchema);
        body.put("workerId", config.workerId);
        body.put("workerFlightUri", config.workerFlightUri);
        body.put("capabilitySigningConfigured", config.capabilitySecret.isPresent());
        body.put("adminTokenConfigured", config.adminToken.isPresent());
        HttpSupport.sendJson(exchange, 200, body);
    }

    private void handleCtas(HttpExchange exchange) throws Exception {
        HttpSupport.requireMethod(exchange, "POST");
        Map<String, Object> request = HttpSupport.readJson(exchange);
        String sql = Json.requiredString(request, "sql");
        String targetTable = Optional.ofNullable(Json.string(request, "targetTable"))
                .filter(value -> !value.isBlank())
                .orElseGet(config::generatedCtasTable);
        String ctasSql = SqlPlanner.buildCtas(targetTable, sql);
        String trinoUser = HttpSupport.header(exchange, "X-Trino-User")
                .or(() -> Optional.ofNullable(Json.string(request, "user")))
                .filter(value -> !value.isBlank())
                .orElse("anonymous");
        Optional<String> authorization = HttpSupport.header(exchange, "Authorization");

        TrinoClient.QueryResult result = trinoClient.executeStatement(ctasSql, trinoUser, authorization);

        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("targetTable", targetTable);
        body.put("submittedSql", ctasSql);
        body.put("trino", result.toJson());
        HttpSupport.sendJson(exchange, 200, body);
    }

    private void handlePutTicket(HttpExchange exchange) throws IOException {
        HttpSupport.requireMethod(exchange, "POST");
        HttpSupport.requireAdminIfConfigured(exchange, config);
        Map<String, Object> request = HttpSupport.readJson(exchange);
        HttpSupport.sendJson(exchange, 200, capabilitySigner.putPayload(request));
    }

    private void handleGetTicket(HttpExchange exchange) throws IOException {
        HttpSupport.requireMethod(exchange, "POST");
        HttpSupport.requireAdminIfConfigured(exchange, config);
        Map<String, Object> request = HttpSupport.readJson(exchange);
        HttpSupport.sendJson(exchange, 200, capabilitySigner.getPayload(request));
    }

    @FunctionalInterface
    private interface ThrowingHandler {
        void handle(HttpExchange exchange) throws Exception;
    }

    @FunctionalInterface
    private interface Handler extends com.sun.net.httpserver.HttpHandler {
        @Override
        void handle(HttpExchange exchange) throws IOException;
    }
}
