package com.arrowflight.coordinator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

final class CoordinatorService {
    private static final String PHASE_CTAS = "ctas";
    private static final String PHASE_FILES = "files";
    private static final String PHASE_READY = "ready";

    private final Config config;
    private final TrinoClient trinoClient;
    private final IcebergCommitter icebergCommitter;
    private final CapabilitySigner capabilitySigner;
    private final CoordinatorMetadataStore metadataStore;
    private final AtomicLong lastQueryCleanupMs = new AtomicLong(0);

    CoordinatorService(Config config) {
        this.config = config;
        this.trinoClient = new TrinoClient(config);
        this.icebergCommitter = new IcebergCommitter(config);
        this.capabilitySigner = new CapabilitySigner(config);
        this.metadataStore = new CoordinatorMetadataStore(config);
    }

    boolean metadataEnabled() {
        return metadataStore.enabled();
    }

    Map<String, Object> configJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("protocol", "arrow-flight");
        body.put("trinoUri", config.trinoUri.toString());
        body.put("trinoCatalog", config.trinoCatalog);
        body.put("trinoSchema", config.trinoSchema);
        body.put("ctasCatalog", config.ctasCatalog);
        body.put("ctasSchema", config.ctasSchema);
        body.put("icebergCatalogName", config.icebergCatalogName);
        body.put("icebergCatalogUri", config.icebergCatalogUri);
        body.put("icebergWarehouse", config.icebergWarehouse);
        body.put("capabilitySigningConfigured", config.capabilitySecret.isPresent());
        body.put("adminTokenConfigured", config.adminToken.isPresent());
        body.put("metadataDatabaseConfigured", metadataStore.enabled());
        body.put("queryRegistryTtlMs", config.queryRegistryTtlMs);
        return body;
    }

    Map<String, Object> createUpload(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        cleanupQueriesIfDue();

        String operationId = stringOrDefault(request, "operationId", UUID.randomUUID().toString());
        String uploadId = stringOrDefault(request, "uploadId", operationId);
        String stagingPrefix = request.containsKey("stagingPrefix")
                ? Config.normalizePrefix(String.valueOf(request.get("stagingPrefix")))
                : config.stagingPrefixForOperation(operationId);
        Optional<String> tableName = Optional.ofNullable(Json.string(request, "tableName"))
                .filter(value -> !value.isBlank());
        int requestedStreams = Math.max(1, Math.min(
                Json.intValue(request, "streams", config.defaultUploadStreams),
                config.defaultMaxUploadStreams
        ));
        List<WorkerAssignment> workerAssignments = metadataStore.selectPutWorkers(requestedStreams);
        int streams = workerAssignments.size();
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
            WorkerAssignment worker = workerAssignments.get(index);
            String streamId = "stream-%05d".formatted(index);
            String attemptId = UUID.randomUUID().toString();
            String descriptorPath = stagingPrefix + "/" + streamId + ".parquet";
            LinkedHashMap<String, Object> ticketRequest = new LinkedHashMap<>();
            ticketRequest.put("operationId", operationId);
            ticketRequest.put("attemptId", attemptId);
            ticketRequest.put("uploadId", uploadId);
            ticketRequest.put("streamId", streamId);
            ticketRequest.put("workerId", worker.workerId());
            ticketRequest.put("flightUri", worker.flightUri());
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
                    worker.workerId(),
                    worker.flightUri(),
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
        body.put("requestedStreams", requestedStreams);
        body.put("grantedStreams", streams);
        body.put("expectedStreams", streams);
        body.put("stagingPrefix", stagingPrefix + "/");
        body.put("targetFileSizeBytes", targetFileSize);
        body.put("expiresAtMs", expiresAt.toEpochMilli());
        body.put("selectedWorkers", workerAssignments.stream().map(WorkerAssignment::toJson).toList());
        body.put("tickets", ticketBodies);
        return body;
    }

    Map<String, Object> uploadStatus(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        return metadataStore.loadUpload(Json.requiredString(request, "uploadId")).toJson();
    }

    Map<String, Object> finishUpload(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        String uploadId = Json.requiredString(request, "uploadId");
        UploadSnapshot snapshot = metadataStore.loadUpload(uploadId);
        if (snapshot.session().status().equals("COMMITTED")) {
            return committedUploadResponse(snapshot);
        }
        if (snapshot.session().status().equals("COMMITTING") || snapshot.session().status().equals("ABORTED")) {
            throw new CoordinatorException(
                    409,
                    "upload session " + uploadId + " cannot be finalized; status=" + snapshot.session().status()
            );
        }
        UploadReadyPlan plan = uploadReadyPlan(snapshot, request);
        metadataStore.markReadyToCommit(uploadId, plan.tableName(), plan.createTableSql());
        return readyUploadResponse(plan);
    }

    Map<String, Object> commitUpload(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        String uploadId = Json.requiredString(request, "uploadId");
        UploadSnapshot snapshot = metadataStore.loadUpload(uploadId);
        if (snapshot.session().status().equals("COMMITTED")) {
            return committedUploadResponse(snapshot);
        }
        if (snapshot.session().status().equals("COMMITTING") || snapshot.session().status().equals("ABORTED")) {
            throw new CoordinatorException(
                    409,
                    "upload session " + uploadId + " is not available for commit; status=" + snapshot.session().status()
            );
        }

        UploadReadyPlan plan = uploadReadyPlan(snapshot, request);
        metadataStore.markReadyToCommit(uploadId, plan.tableName(), plan.createTableSql());
        if (!metadataStore.tryMarkCommitting(uploadId, plan.tableName(), plan.createTableSql())) {
            UploadSnapshot current = metadataStore.loadUpload(uploadId);
            if (current.session().status().equals("COMMITTED")) {
                return committedUploadResponse(current);
            }
            throw new CoordinatorException(
                    409,
                    "upload session " + uploadId + " is not available for commit; status=" + current.session().status()
            );
        }

        String mode = Optional.ofNullable(Json.string(request, "mode"))
                .or(() -> Optional.ofNullable(Json.string(request, "commitMode")))
                .filter(value -> !value.isBlank())
                .orElse("append");
        String trinoUser = stringOrDefault(request, "user", "anonymous");
        Optional<String> authorization = authorizationFrom(request);
        try {
            TrinoClient.QueryHandle ddl = trinoClient.runStatement(plan.createTableSql(), trinoUser, authorization);
            CommitOutcome outcome = icebergCommitter.commit(uploadId, plan.tableName(), mode, snapshot.files());
            LinkedHashMap<String, Object> summary = new LinkedHashMap<>(outcome.summary());
            ddl.queryId().ifPresent(value -> summary.put("trinoDdlQueryId", value));
            metadataStore.markCommitted(uploadId, new CommitMetadata(
                    plan.tableName(),
                    outcome.mode(),
                    outcome.snapshotId(),
                    Optional.of(plan.createTableSql()),
                    summary
            ));

            LinkedHashMap<String, Object> body = readyUploadResponse(plan);
            body.put("status", "COMMITTED");
            body.put("mode", outcome.mode());
            body.put("snapshotId", outcome.snapshotId());
            body.put("recordCount", outcome.recordCount());
            body.put("parquetObjectBytes", outcome.parquetObjectBytes());
            body.put("commitSummary", summary);
            return body;
        } catch (Exception error) {
            metadataStore.markFailed(uploadId, error.getMessage());
            throw new CoordinatorException(500, "failed to commit upload " + uploadId + ": " + error.getMessage(), error);
        }
    }

    private UploadReadyPlan uploadReadyPlan(UploadSnapshot snapshot, Map<String, Object> request) {
        String uploadId = snapshot.session().uploadId();
        Optional<UploadStreamState> failed = snapshot.firstFailedStream();
        if (failed.isPresent()) {
            String message = "upload stream " + failed.get().streamId() + " failed with status " + failed.get().status()
                    + failed.get().errorMessage().map(error -> ": " + error).orElse("");
            metadataStore.markFailed(uploadId, message);
            throw new CoordinatorException(409, message);
        }
        if (!snapshot.allStreamsSucceeded()) {
            metadataStore.markRunning(uploadId);
            Optional<UploadStreamState> incomplete = snapshot.firstIncompleteStream();
            String message = incomplete
                    .map(stream -> "upload stream " + stream.streamId() + " is not complete yet; status=" + stream.status())
                    .orElse("upload has fewer worker stream rows than expected");
            throw new CoordinatorException(409, message);
        }
        if (snapshot.files().isEmpty()) {
            throw new CoordinatorException(409, "upload streams succeeded but no parquet files were recorded");
        }

        String tableName = Optional.ofNullable(Json.string(request, "tableName"))
                .filter(value -> !value.isBlank())
                .or(() -> snapshot.session().tableName())
                .or(() -> snapshot.session().commitTableName())
                .orElseGet(() -> config.generatedUploadTable(uploadId));
        Map<String, Object> arrowSchema = Json.parseObject(snapshot.canonicalSchemaJson());
        String createTableSql = TrinoDdlPlanner.createTableSql(
                tableName,
                arrowSchema,
                tableLocation(tableName),
                true
        );
        return new UploadReadyPlan(snapshot, tableName, createTableSql, arrowSchema);
    }

    private String tableLocation(String tableName) {
        String[] parts = SqlPlanner.validateTableName(tableName).split("\\.");
        String schema;
        String table;
        if (parts.length == 1) {
            schema = config.ctasSchema;
            table = parts[0];
        } else if (parts.length == 2) {
            schema = parts[0];
            table = parts[1];
        } else if (parts.length == 3) {
            schema = parts[1];
            table = parts[2];
        } else {
            throw new CoordinatorException(400, "tableName must be table, schema.table, or catalog.schema.table");
        }
        return config.icebergWarehouse + "/" + schema + "/" + table;
    }

    private LinkedHashMap<String, Object> readyUploadResponse(UploadReadyPlan plan) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", plan.snapshot().session().uploadId());
        body.put("status", "READY_TO_COMMIT");
        body.put("tableName", plan.tableName());
        body.put("createTableSql", plan.createTableSql());
        body.put("files", plan.snapshot().files().stream().map(UploadFile::toJson).toList());
        body.put("streams", plan.snapshot().streams().stream().map(UploadStreamState::toJson).toList());
        body.put("arrowSchema", plan.arrowSchema());
        return body;
    }

    private LinkedHashMap<String, Object> committedUploadResponse(UploadSnapshot snapshot) {
        UploadSessionRecord session = snapshot.session();
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", session.uploadId());
        body.put("status", "COMMITTED");
        session.commitTableName().or(session::tableName).ifPresent(value -> body.put("tableName", value));
        session.createTableSql().ifPresent(value -> body.put("createTableSql", value));
        session.commitMode().ifPresent(value -> body.put("mode", value));
        session.commitSnapshotId().ifPresent(value -> body.put("snapshotId", value));
        session.commitSummary().ifPresent(value -> body.put("commitSummary", value));
        body.put("files", snapshot.files().stream().map(UploadFile::toJson).toList());
        body.put("streams", snapshot.streams().stream().map(UploadStreamState::toJson).toList());
        body.put("alreadyCommitted", true);
        return body;
    }

    Map<String, Object> abortUpload(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        String uploadId = Json.requiredString(request, "uploadId");
        String reason = Optional.ofNullable(Json.string(request, "reason"))
                .filter(value -> !value.isBlank())
                .orElse("aborted by coordinator request");
        metadataStore.markAborted(uploadId, reason);
        return Map.of("uploadId", uploadId, "status", "ABORTED");
    }

    Map<String, Object> putTicket(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        WorkerAssignment worker = metadataStore.selectPutWorkers(1).getFirst();
        LinkedHashMap<String, Object> signedRequest = new LinkedHashMap<>(request);
        signedRequest.put("workerId", worker.workerId());
        signedRequest.put("flightUri", worker.flightUri());
        return capabilitySigner.putPayload(signedRequest);
    }

    Map<String, Object> getTicket(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        return planReadEndpoint(
                stringOrDefault(request, "operationId", "read-" + UUID.randomUUID()),
                Config.normalizePath(Json.requiredString(request, "path")),
                request
        );
    }

    FlightPlan startFlight(Map<String, Object> request) {
        cleanupQueriesIfDue();
        String type = requestType(request);
        return switch (type) {
            case "read" -> startRead(request);
            case "ctas" -> startCtas(request);
            default -> throw new CoordinatorException(400, "unsupported GetFlightInfo request type: " + type);
        };
    }

    PollResult pollFlight(Map<String, Object> request) {
        cleanupQueriesIfDue();
        String queryId = Json.requiredString(request, "queryId");
        QueryRegistryRecord record = metadataStore.loadQuery(queryId);
        if (record.terminal()) {
            return new PollResult(flightPlanFromRecord(record), true, record.progress(), record.expiresAt());
        }
        if (Instant.now().isAfter(record.expiresAt())) {
            metadataStore.markQueryFailed(queryId, "query registry entry expired", record.trinoStatsJson());
            QueryRegistryRecord failed = metadataStore.loadQuery(queryId);
            return new PollResult(flightPlanFromRecord(failed), true, failed.progress(), failed.expiresAt());
        }
        if (!record.queryType().equals("ctas")) {
            return new PollResult(flightPlanFromRecord(record), record.terminal(), record.progress(), record.expiresAt());
        }

        try {
            QueryRegistryRecord updated = advanceCtas(record, request);
            return new PollResult(flightPlanFromRecord(updated), updated.terminal(), updated.progress(), updated.expiresAt());
        } catch (Exception error) {
            metadataStore.markQueryFailed(queryId, error.getMessage(), record.trinoStatsJson());
            QueryRegistryRecord failed = metadataStore.loadQuery(queryId);
            return new PollResult(flightPlanFromRecord(failed), true, failed.progress(), failed.expiresAt());
        }
    }

    private FlightPlan startRead(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        String queryId = newQueryId("read");
        List<String> paths = requestPaths(request);
        List<Map<String, Object>> endpoints = new ArrayList<>();
        for (String path : paths) {
            endpoints.add(planReadEndpoint(queryId, path, request));
        }
        Instant expiresAt = Instant.now().plusMillis(config.queryRegistryTtlMs);
        LinkedHashMap<String, Object> metadata = baseFlightMetadata(queryId, "read", "SUCCEEDED", Optional.of(1.0));
        metadata.put("paths", paths);
        metadata.put("endpointCount", endpoints.size());
        Map<String, Object> ticketsJson = Map.of("tickets", endpoints);
        Map<String, Object> filesJson = Map.of("files", paths.stream().map(path -> Map.of("filePath", path)).toList());

        metadataStore.createQuery(new QueryRegistryRecord(
                queryId,
                "read",
                "SUCCEEDED",
                descriptorForRegistry(request, queryId),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(1.0),
                Optional.empty(),
                Optional.of(metadata),
                Optional.of(ticketsJson),
                Optional.of(filesJson),
                Instant.now(),
                Instant.now(),
                expiresAt,
                Optional.of(Instant.now())
        ));
        return new FlightPlan(queryId, "SUCCEEDED", metadata, endpoints, -1, -1, expiresAt);
    }

    private FlightPlan startCtas(Map<String, Object> request) {
        String queryId = newQueryId("ctas");
        String targetTable = qualifiedTargetTable(Optional.ofNullable(Json.string(request, "targetTable"))
                .filter(value -> !value.isBlank())
                .orElseGet(config::generatedCtasTable));
        String ctasLocation = config.objectUriForPrefix("coordinator/ctas/" + queryId);
        String ctasSql = SqlPlanner.buildCtas(targetTable, Json.requiredString(request, "sql"), ctasLocation);
        String trinoUser = stringOrDefault(request, "user", "anonymous");
        Optional<String> authorization = Optional.ofNullable(Json.string(request, "authorization"))
                .filter(value -> !value.isBlank());
        Instant now = Instant.now();
        Instant expiresAt = now.plusMillis(config.queryRegistryTtlMs);
        LinkedHashMap<String, Object> metadata = baseFlightMetadata(queryId, "ctas", "RUNNING", Optional.of(0.0));
        metadata.put("phase", PHASE_CTAS);
        metadata.put("targetTable", targetTable);
        metadata.put("submittedSql", ctasSql);
        metadata.put("location", ctasLocation);

        metadataStore.createQuery(new QueryRegistryRecord(
                queryId,
                "ctas",
                "RUNNING",
                descriptorForRegistry(request, queryId),
                Optional.of(targetTable),
                Optional.of(ctasSql),
                Optional.of(trinoUser),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(0.0),
                Optional.empty(),
                Optional.of(metadata),
                Optional.empty(),
                Optional.empty(),
                now,
                now,
                expiresAt,
                Optional.empty()
        ));

        try {
            TrinoClient.QueryHandle handle = trinoClient.submitStatement(ctasSql, trinoUser, authorization);
            metadataStore.markQueryRunning(
                    queryId,
                    handle.queryId(),
                    handle.infoUri(),
                    handle.nextUri(),
                    handle.stats(),
                    progressFromStats(handle.stats())
            );
            QueryRegistryRecord current = metadataStore.loadQuery(queryId);
            if (handle.nextUri().isEmpty()) {
                return flightPlanFromRecord(startFileCollection(current, request, authorization));
            }
            return flightPlanFromRecord(current);
        } catch (Exception error) {
            metadataStore.markQueryFailed(queryId, error.getMessage(), Optional.empty());
            return flightPlanFromRecord(metadataStore.loadQuery(queryId));
        }
    }

    private QueryRegistryRecord advanceCtas(QueryRegistryRecord record, Map<String, Object> request)
            throws Exception {
        if (queryPhase(record).equals(PHASE_FILES)) {
            return advanceFileCollection(record, request, authorizationFrom(request));
        }
        Optional<String> nextUri = record.trinoNextUri();
        if (nextUri.isEmpty()) {
            return startFileCollection(record, request, authorizationFrom(request));
        }

        String trinoUser = stringOrDefault(request, "user", record.trinoUser().orElse("anonymous"));
        Optional<String> authorization = authorizationFrom(request);
        TrinoClient.QueryHandle handle = trinoClient.pollNext(nextUri.orElseThrow(), trinoUser, authorization);
        metadataStore.markQueryRunning(
                record.queryId(),
                handle.queryId().or(() -> record.trinoQueryId()),
                handle.infoUri().or(() -> record.trinoInfoUri()),
                handle.nextUri(),
                handle.stats(),
                progressFromStats(handle.stats())
        );
        QueryRegistryRecord updated = metadataStore.loadQuery(record.queryId());
        if (handle.nextUri().isEmpty()) {
            return startFileCollection(updated, request, authorization);
        }
        return updated;
    }

    private QueryRegistryRecord startFileCollection(
            QueryRegistryRecord record,
            Map<String, Object> request,
            Optional<String> authorization
    ) throws Exception {
        String targetTable = record.targetTable().orElseThrow();
        String trinoUser = stringOrDefault(request, "user", record.trinoUser().orElse("anonymous"));
        String filesSql = SqlPlanner.buildIcebergFilesQuery(targetTable);
        TrinoClient.QueryHandle handle = trinoClient.submitStatement(filesSql, trinoUser, authorization);
        Map<String, Object> filesJson = appendFileQueryPage(record.resultFilesJson(), handle.response());
        LinkedHashMap<String, Object> metadata = baseFlightMetadata(
                record.queryId(),
                "ctas",
                "RUNNING",
                progressFromStats(handle.stats()).or(() -> Optional.of(0.95))
        );
        metadata.put("phase", PHASE_FILES);
        metadata.put("targetTable", targetTable);
        record.submittedSql().ifPresent(value -> metadata.put("submittedSql", value));
        metadata.put("fileQuerySql", filesSql);
        metadata.put("discoveredFileRows", collectedFileRows(Optional.of(filesJson)).size());

        metadataStore.markQueryRunning(
                record.queryId(),
                handle.queryId(),
                handle.infoUri(),
                handle.nextUri(),
                handle.stats(),
                progressFromStats(handle.stats()).or(() -> Optional.of(0.95)),
                Optional.of(metadata),
                Optional.of(filesJson)
        );
        QueryRegistryRecord updated = metadataStore.loadQuery(record.queryId());
        if (handle.nextUri().isEmpty()) {
            return completeFileCollection(updated, request);
        }
        return updated;
    }

    private QueryRegistryRecord advanceFileCollection(
            QueryRegistryRecord record,
            Map<String, Object> request,
            Optional<String> authorization
    ) throws Exception {
        Optional<String> nextUri = record.trinoNextUri();
        if (nextUri.isEmpty()) {
            return completeFileCollection(record, request);
        }

        String trinoUser = stringOrDefault(request, "user", record.trinoUser().orElse("anonymous"));
        TrinoClient.QueryHandle handle = trinoClient.pollNext(nextUri.orElseThrow(), trinoUser, authorization);
        Map<String, Object> filesJson = appendFileQueryPage(record.resultFilesJson(), handle.response());
        LinkedHashMap<String, Object> metadata = baseFlightMetadata(
                record.queryId(),
                "ctas",
                "RUNNING",
                progressOrDefault(handle.stats(), record.progress(), 0.95)
        );
        metadata.put("phase", PHASE_FILES);
        record.targetTable().ifPresent(value -> metadata.put("targetTable", value));
        record.submittedSql().ifPresent(value -> metadata.put("submittedSql", value));
        metadata.put("discoveredFileRows", collectedFileRows(Optional.of(filesJson)).size());

        metadataStore.markQueryRunning(
                record.queryId(),
                handle.queryId().or(() -> record.trinoQueryId()),
                handle.infoUri().or(() -> record.trinoInfoUri()),
                handle.nextUri(),
                handle.stats(),
                progressOrDefault(handle.stats(), record.progress(), 0.95),
                Optional.of(metadata),
                Optional.of(filesJson)
        );
        QueryRegistryRecord updated = metadataStore.loadQuery(record.queryId());
        if (handle.nextUri().isEmpty()) {
            return completeFileCollection(updated, request);
        }
        return updated;
    }

    private QueryRegistryRecord completeFileCollection(
            QueryRegistryRecord record,
            Map<String, Object> request
    ) {
        String targetTable = record.targetTable().orElseThrow();
        ArrayList<String> paths = new ArrayList<>();
        ArrayList<Map<String, Object>> fileRows = new ArrayList<>();
        for (Map<String, Object> row : collectedFileRows(record.resultFilesJson())) {
            Object rawSourceUri = row.get("file_path");
            if (rawSourceUri == null || String.valueOf(rawSourceUri).isBlank()) {
                throw new CoordinatorException(502, "Iceberg files query returned a row without file_path");
            }
            String sourceUri = String.valueOf(rawSourceUri);
            String path = objectKeyFromUri(sourceUri);
            paths.add(path);
            LinkedHashMap<String, Object> file = new LinkedHashMap<>();
            file.put("filePath", path);
            file.put("sourceUri", sourceUri);
            copyIfPresent(row, file, "record_count", "rows");
            copyIfPresent(row, file, "file_size_in_bytes", "bytes");
            fileRows.add(file);
        }

        ArrayList<Map<String, Object>> endpoints = new ArrayList<>();
        for (String path : paths) {
            endpoints.add(planReadEndpoint(record.queryId(), path, request));
        }
        LinkedHashMap<String, Object> metadata = baseFlightMetadata(record.queryId(), "ctas", "SUCCEEDED", Optional.of(1.0));
        metadata.put("phase", PHASE_READY);
        metadata.put("targetTable", targetTable);
        record.submittedSql().ifPresent(value -> metadata.put("submittedSql", value));
        metadata.put("endpointCount", endpoints.size());
        metadata.put("fileCount", fileRows.size());

        metadataStore.markQuerySucceeded(
                record.queryId(),
                metadata,
                Map.of("tickets", endpoints),
                Map.of("files", fileRows),
                record.trinoStatsJson(),
                Optional.of(1.0)
        );
        return metadataStore.loadQuery(record.queryId());
    }

    private Map<String, Object> planReadEndpoint(String operationId, String path, Map<String, Object> request) {
        WorkerAssignment worker = metadataStore.selectReadWorker();
        LinkedHashMap<String, Object> signedRequest = new LinkedHashMap<>(request);
        signedRequest.put("operationId", operationId);
        signedRequest.put("path", Config.normalizePath(path));
        signedRequest.put("workerId", worker.workerId());
        signedRequest.put("flightUri", worker.flightUri());
        Map<String, Object> response = new LinkedHashMap<>(capabilitySigner.getPayload(signedRequest));
        response.put("selectedWorker", worker.toJson());
        return response;
    }

    private FlightPlan flightPlanFromRecord(QueryRegistryRecord record) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        record.resultFlightInfoJson().ifPresent(metadata::putAll);
        metadata.putAll(record.statusJson());
        List<Map<String, Object>> endpoints = endpointPlans(record);
        long totalRows = sumLong(record.resultFilesJson(), "files", "rows").orElse(-1L);
        long totalBytes = sumLong(record.resultFilesJson(), "files", "bytes").orElse(-1L);
        return new FlightPlan(record.queryId(), record.status(), metadata, endpoints, totalRows, totalBytes, record.expiresAt());
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> endpointPlans(QueryRegistryRecord record) {
        return record.resultTicketsJson()
                .map(value -> Json.listValue(value, "tickets"))
                .orElse(List.of())
                .stream()
                .filter(value -> value instanceof Map<?, ?>)
                .map(value -> (Map<String, Object>) value)
                .toList();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> appendFileQueryPage(
            Optional<Map<String, Object>> current,
            Map<String, Object> response
    ) {
        List<String> previousColumns = current.map(value -> stringList(value, "columns")).orElse(List.of());
        List<String> columns = trinoColumnNames(response, previousColumns);
        ArrayList<Map<String, Object>> rows = new ArrayList<>();
        current.ifPresent(value -> {
            for (Object row : Json.listValue(value, "rows")) {
                if (row instanceof Map<?, ?> raw) {
                    rows.add(new LinkedHashMap<>((Map<String, Object>) raw));
                }
            }
        });

        for (Object rowValue : Json.listValue(response, "data")) {
            if (!(rowValue instanceof List<?> rawRow)) {
                continue;
            }
            LinkedHashMap<String, Object> row = new LinkedHashMap<>();
            for (int index = 0; index < rawRow.size() && index < columns.size(); index++) {
                row.put(columns.get(index), rawRow.get(index));
            }
            rows.add(row);
        }

        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        out.put("phase", PHASE_FILES);
        out.put("columns", columns);
        out.put("rows", rows);
        return out;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectedFileRows(Optional<Map<String, Object>> filesJson) {
        if (filesJson.isEmpty()) {
            return List.of();
        }
        ArrayList<Map<String, Object>> rows = new ArrayList<>();
        for (Object row : Json.listValue(filesJson.get(), "rows")) {
            if (row instanceof Map<?, ?> raw) {
                rows.add((Map<String, Object>) raw);
            }
        }
        return rows;
    }

    private List<String> trinoColumnNames(Map<String, Object> response, List<String> fallback) {
        List<Object> rawColumns = Json.listValue(response, "columns");
        if (rawColumns.isEmpty()) {
            return fallback;
        }
        ArrayList<String> columns = new ArrayList<>();
        for (Object value : rawColumns) {
            if (value instanceof Map<?, ?> rawColumn) {
                @SuppressWarnings("unchecked")
                Map<String, Object> column = (Map<String, Object>) rawColumn;
                String name = Json.string(column, "name");
                if (name != null && !name.isBlank()) {
                    columns.add(name);
                }
            }
        }
        return columns.isEmpty() ? fallback : columns;
    }

    private List<String> stringList(Map<String, Object> value, String key) {
        ArrayList<String> out = new ArrayList<>();
        for (Object item : Json.listValue(value, key)) {
            if (item != null) {
                out.add(String.valueOf(item));
            }
        }
        return out;
    }

    private Optional<Long> sumLong(Optional<Map<String, Object>> object, String listKey, String valueKey) {
        if (object.isEmpty()) {
            return Optional.empty();
        }
        long sum = 0;
        boolean found = false;
        for (Object item : Json.listValue(object.get(), listKey)) {
            if (item instanceof Map<?, ?> raw) {
                Object value = raw.get(valueKey);
                if (value instanceof Number number) {
                    sum += number.longValue();
                    found = true;
                }
            }
        }
        return found ? Optional.of(sum) : Optional.empty();
    }

    private void cleanupQueriesIfDue() {
        long interval = config.queryRegistryCleanupIntervalMs;
        if (interval <= 0 || !metadataStore.enabled()) {
            return;
        }
        long now = System.currentTimeMillis();
        long previous = lastQueryCleanupMs.get();
        if (now - previous < interval || !lastQueryCleanupMs.compareAndSet(previous, now)) {
            return;
        }
        metadataStore.cleanupExpiredQueries(Instant.ofEpochMilli(now));
    }

    private void requireAdminIfConfigured(Map<String, Object> request) {
        if (config.adminToken.isEmpty()) {
            return;
        }
        String actual = Json.string(request, "adminToken");
        if (!config.adminToken.get().equals(actual)) {
            throw new CoordinatorException(403, "invalid coordinator admin token");
        }
    }

    private List<String> requestPaths(Map<String, Object> request) {
        ArrayList<String> paths = new ArrayList<>();
        String path = Json.string(request, "path");
        if (path != null && !path.isBlank()) {
            paths.add(Config.normalizePath(path));
        }
        for (Object value : Json.listValue(request, "paths")) {
            if (value != null && !String.valueOf(value).isBlank()) {
                paths.add(Config.normalizePath(String.valueOf(value)));
            }
        }
        if (paths.isEmpty()) {
            throw new CoordinatorException(400, "GetFlightInfo read request requires path or paths");
        }
        return paths;
    }

    private Optional<String> authorizationFrom(Map<String, Object> request) {
        return Optional.ofNullable(Json.string(request, "authorization")).filter(value -> !value.isBlank());
    }

    private String requestType(Map<String, Object> request) {
        return Optional.ofNullable(Json.string(request, "type"))
                .or(() -> Optional.ofNullable(Json.string(request, "kind")))
                .filter(value -> !value.isBlank())
                .map(value -> value.trim().toLowerCase(java.util.Locale.ROOT))
                .orElse("read");
    }

    private LinkedHashMap<String, Object> descriptorForRegistry(Map<String, Object> request, String queryId) {
        LinkedHashMap<String, Object> descriptor = new LinkedHashMap<>(request);
        descriptor.remove("authorization");
        descriptor.put("queryId", queryId);
        return descriptor;
    }

    private LinkedHashMap<String, Object> baseFlightMetadata(
            String queryId,
            String queryType,
            String status,
            Optional<Double> progress
    ) {
        LinkedHashMap<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("queryId", queryId);
        metadata.put("queryType", queryType);
        metadata.put("status", status);
        progress.ifPresent(value -> metadata.put("progress", value));
        metadata.put("pollDescriptor", Map.of("type", "poll", "queryId", queryId));
        return metadata;
    }

    private Optional<Double> progressFromStats(Optional<Map<String, Object>> stats) {
        if (stats.isEmpty()) {
            return Optional.empty();
        }
        Object progress = stats.get().get("progressPercentage");
        if (progress instanceof Number number) {
            return Optional.of(Math.max(0.0, Math.min(1.0, number.doubleValue() / 100.0)));
        }
        return Optional.empty();
    }

    private Optional<Double> progressOrDefault(
            Optional<Map<String, Object>> stats,
            Optional<Double> existing,
            double fallback
    ) {
        return progressFromStats(stats).or(() -> existing).or(() -> Optional.of(fallback));
    }

    private String queryPhase(QueryRegistryRecord record) {
        return record.resultFlightInfoJson()
                .map(value -> Json.string(value, "phase"))
                .filter(value -> !value.isBlank())
                .orElse(PHASE_CTAS);
    }

    private String qualifiedTargetTable(String raw) {
        String[] parts = SqlPlanner.validateTableName(raw).split("\\.");
        return switch (parts.length) {
            case 1 -> config.ctasCatalog + "." + config.ctasSchema + "." + parts[0];
            case 2 -> config.ctasCatalog + "." + parts[0] + "." + parts[1];
            case 3 -> raw;
            default -> throw new IllegalArgumentException("targetTable must be table, schema.table, or catalog.schema.table");
        };
    }

    private String objectKeyFromUri(String raw) {
        for (String prefix : objectStoreUriPrefixes()) {
            String normalizedPrefix = prefix + "/";
            if (raw.startsWith(normalizedPrefix)) {
                return Config.normalizePath(raw.substring(normalizedPrefix.length()));
            }
        }
        return Config.normalizePath(raw);
    }

    private List<String> objectStoreUriPrefixes() {
        ArrayList<String> prefixes = new ArrayList<>();
        prefixes.add(config.objectStoreUriPrefix);
        if (config.objectStoreUriPrefix.startsWith("s3a://")) {
            prefixes.add("s3://" + config.objectStoreUriPrefix.substring("s3a://".length()));
        } else if (config.objectStoreUriPrefix.startsWith("s3://")) {
            prefixes.add("s3a://" + config.objectStoreUriPrefix.substring("s3://".length()));
        }
        return prefixes.stream().distinct().toList();
    }

    private void copyIfPresent(Map<String, Object> source, Map<String, Object> target, String sourceKey, String targetKey) {
        Object value = source.get(sourceKey);
        if (value != null) {
            target.put(targetKey, value);
        }
    }

    private String newQueryId(String prefix) {
        return prefix + "-" + UUID.randomUUID();
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
}

record FlightPlan(
        String queryId,
        String status,
        Map<String, Object> metadata,
        List<Map<String, Object>> endpoints,
        long totalRecords,
        long totalBytes,
        Instant expiresAt
) {
}

record PollResult(
        FlightPlan plan,
        boolean complete,
        Optional<Double> progress,
        Instant expiresAt
) {
}

record UploadReadyPlan(
        UploadSnapshot snapshot,
        String tableName,
        String createTableSql,
        Map<String, Object> arrowSchema
) {
}
