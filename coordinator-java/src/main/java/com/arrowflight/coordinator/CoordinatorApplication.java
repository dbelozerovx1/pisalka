package com.arrowflight.coordinator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;

public final class CoordinatorApplication {
    private final Config config;
    private final TrinoClient trinoClient;
    private final CapabilitySigner capabilitySigner;
    private final CoordinatorMetadataStore metadataStore;

    private CoordinatorApplication(Config config) {
        this.config = config;
        this.trinoClient = new TrinoClient(config);
        this.capabilitySigner = new CapabilitySigner(config);
        this.metadataStore = new CoordinatorMetadataStore(config);
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        CoordinatorApplication app = new CoordinatorApplication(config);
        HttpServer server = HttpServer.create(config.listenAddress, 128);
        server.createContext("/healthz", app::handleHealth);
        server.createContext("/v1/ctas", app.withErrors(app::handleCtas));
        server.createContext("/v1/flight/create-upload", app.withErrors(app::handleCreateUpload));
        server.createContext("/v1/flight/upload-status", app.withErrors(app::handleUploadStatus));
        server.createContext("/v1/flight/finish-upload", app.withErrors(app::handleFinishUpload));
        server.createContext("/v1/flight/abort-upload", app.withErrors(app::handleAbortUpload));
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
        body.put("metadataDatabaseConfigured", metadataStore.enabled());
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

    private void handleCreateUpload(HttpExchange exchange) throws IOException {
        HttpSupport.requireMethod(exchange, "POST");
        HttpSupport.requireAdminIfConfigured(exchange, config);
        Map<String, Object> request = HttpSupport.readJson(exchange);

        String operationId = stringOrDefault(request, "operationId", UUID.randomUUID().toString());
        String uploadId = stringOrDefault(request, "uploadId", operationId);
        String stagingPrefix = request.containsKey("stagingPrefix")
                ? Config.normalizePrefix(String.valueOf(request.get("stagingPrefix")))
                : config.stagingPrefixForOperation(operationId);
        Optional<String> tableName = Optional.ofNullable(Json.string(request, "tableName"))
                .filter(value -> !value.isBlank());
        int streams = Math.max(1, Math.min(
                Json.intValue(request, "streams", config.defaultUploadStreams),
                config.defaultMaxUploadStreams
        ));
        long targetFileSize = Json.longValue(request, "targetFileSizeBytes", config.defaultTargetFileSizeBytes);
        Optional<Long> maxStreamBytes = optionalLong(request, "maxStreamBytes", config.defaultMaxStreamBytes);
        Optional<Long> maxRecordBatchBytes = optionalLong(
                request,
                "maxRecordBatchBytes",
                config.defaultPutMaxRecordBatchBytes
        );
        Instant expiresAt = Instant.now().plusMillis(Json.longValue(
                request,
                "ttlMs",
                config.uploadSessionTtlMs
        ));

        ArrayList<PlannedUploadStream> plannedStreams = new ArrayList<>();
        ArrayList<Map<String, Object>> ticketBodies = new ArrayList<>();
        for (int index = 0; index < streams; index++) {
            String streamId = "stream-%05d".formatted(index);
            String attemptId = UUID.randomUUID().toString();
            String descriptorPath = stagingPrefix + "/" + streamId + ".parquet";
            LinkedHashMap<String, Object> ticketRequest = new LinkedHashMap<>();
            ticketRequest.put("operationId", operationId);
            ticketRequest.put("attemptId", attemptId);
            ticketRequest.put("uploadId", uploadId);
            ticketRequest.put("streamId", streamId);
            ticketRequest.put("workerId", config.workerId);
            ticketRequest.put("flightUri", config.workerFlightUri);
            ticketRequest.put("stagingPrefix", stagingPrefix);
            ticketRequest.put("path", descriptorPath);
            ticketRequest.put("targetFileSizeBytes", targetFileSize);
            maxStreamBytes.ifPresent(value -> ticketRequest.put("maxStreamBytes", value));
            maxRecordBatchBytes.ifPresent(value -> ticketRequest.put("maxRecordBatchBytes", value));
            ticketRequest.put("maxUploadStreams", streams);
            ticketRequest.put("ttlMs", Math.max(1, expiresAt.toEpochMilli() - Instant.now().toEpochMilli()));

            Map<String, Object> ticket = capabilitySigner.putPayload(ticketRequest);
            plannedStreams.add(new PlannedUploadStream(
                    streamId,
                    attemptId,
                    config.workerId,
                    config.workerFlightUri,
                    descriptorPath,
                    ticket
            ));
            ticketBodies.add(ticket);
        }

        PlannedUploadSession session = new PlannedUploadSession(
                uploadId,
                operationId,
                tableName,
                streams,
                stagingPrefix,
                targetFileSize,
                maxStreamBytes,
                maxRecordBatchBytes,
                expiresAt
        );
        metadataStore.createUpload(session, plannedStreams);

        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", uploadId);
        body.put("operationId", operationId);
        tableName.ifPresent(value -> body.put("tableName", value));
        body.put("status", "PLANNED");
        body.put("expectedStreams", streams);
        body.put("stagingPrefix", stagingPrefix + "/");
        body.put("targetFileSizeBytes", targetFileSize);
        body.put("expiresAtMs", expiresAt.toEpochMilli());
        body.put("tickets", ticketBodies);
        HttpSupport.sendJson(exchange, 200, body);
    }

    private void handleUploadStatus(HttpExchange exchange) throws IOException {
        HttpSupport.requireMethod(exchange, "POST");
        HttpSupport.requireAdminIfConfigured(exchange, config);
        Map<String, Object> request = HttpSupport.readJson(exchange);
        String uploadId = Json.requiredString(request, "uploadId");
        HttpSupport.sendJson(exchange, 200, metadataStore.loadUpload(uploadId).toJson());
    }

    private void handleFinishUpload(HttpExchange exchange) throws Exception {
        HttpSupport.requireMethod(exchange, "POST");
        HttpSupport.requireAdminIfConfigured(exchange, config);
        Map<String, Object> request = HttpSupport.readJson(exchange);
        String uploadId = Json.requiredString(request, "uploadId");
        UploadSnapshot snapshot = metadataStore.loadUpload(uploadId);

        Optional<UploadStreamState> failed = snapshot.firstFailedStream();
        if (failed.isPresent()) {
            String message = "upload stream " + failed.get().streamId() + " failed with status " + failed.get().status()
                    + failed.get().errorMessage().map(error -> ": " + error).orElse("");
            metadataStore.markFailed(uploadId, message);
            throw new HttpSupport.HttpError(409, message);
        }
        if (!snapshot.allStreamsSucceeded()) {
            metadataStore.markRunning(uploadId);
            Optional<UploadStreamState> incomplete = snapshot.firstIncompleteStream();
            String message = incomplete
                    .map(stream -> "upload stream " + stream.streamId() + " is not complete yet; status=" + stream.status())
                    .orElse("upload has fewer worker stream rows than expected");
            throw new HttpSupport.HttpError(409, message);
        }
        if (snapshot.files().isEmpty()) {
            throw new HttpSupport.HttpError(409, "upload streams succeeded but no parquet files were recorded");
        }

        String tableName = Optional.ofNullable(Json.string(request, "tableName"))
                .filter(value -> !value.isBlank())
                .or(() -> snapshot.session().tableName())
                .orElseGet(() -> config.generatedUploadTable(uploadId));
        Map<String, Object> arrowSchema = Json.parseObject(snapshot.canonicalSchemaJson());
        String createTableSql = TrinoDdlPlanner.createTableSql(
                tableName,
                arrowSchema,
                config.objectUriForPrefix(snapshot.session().stagingPrefix())
        );
        metadataStore.markReadyToCommit(uploadId, createTableSql);

        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", uploadId);
        body.put("status", "READY_TO_COMMIT");
        body.put("tableName", tableName);
        body.put("createTableSql", createTableSql);
        body.put("files", snapshot.files().stream().map(UploadFile::toJson).toList());
        body.put("streams", snapshot.streams().stream().map(UploadStreamState::toJson).toList());
        body.put("arrowSchema", arrowSchema);

        HttpSupport.sendJson(exchange, 200, body);
    }

    private void handleAbortUpload(HttpExchange exchange) throws IOException {
        HttpSupport.requireMethod(exchange, "POST");
        HttpSupport.requireAdminIfConfigured(exchange, config);
        Map<String, Object> request = HttpSupport.readJson(exchange);
        String uploadId = Json.requiredString(request, "uploadId");
        String reason = Optional.ofNullable(Json.string(request, "reason"))
                .filter(value -> !value.isBlank())
                .orElse("aborted by coordinator request");
        metadataStore.markAborted(uploadId, reason);
        HttpSupport.sendJson(exchange, 200, Map.of("uploadId", uploadId, "status", "ABORTED"));
    }

    private void handleGetTicket(HttpExchange exchange) throws IOException {
        HttpSupport.requireMethod(exchange, "POST");
        HttpSupport.requireAdminIfConfigured(exchange, config);
        Map<String, Object> request = HttpSupport.readJson(exchange);
        HttpSupport.sendJson(exchange, 200, capabilitySigner.getPayload(request));
    }

    private static String stringOrDefault(Map<String, Object> request, String key, String defaultValue) {
        String value = Json.string(request, key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static Optional<Long> optionalLong(Map<String, Object> request, String key, long defaultValue) {
        if (!request.containsKey(key)) {
            return Optional.of(defaultValue);
        }
        long value = Json.longValue(request, key, defaultValue);
        return value <= 0 ? Optional.empty() : Optional.of(value);
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
