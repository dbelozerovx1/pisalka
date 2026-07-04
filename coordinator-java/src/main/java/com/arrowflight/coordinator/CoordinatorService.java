package com.arrowflight.coordinator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final ObjectStoreCleaner objectStoreCleaner;
    private final CapabilitySigner capabilitySigner;
    private final CoordinatorMetadataStore metadataStore;
    private final AtomicLong lastQueryCleanupMs = new AtomicLong(0);

    CoordinatorService(Config config) {
        this.config = config;
        this.trinoClient = new TrinoClient(config);
        this.icebergCommitter = new IcebergCommitter(config);
        this.objectStoreCleaner = new ObjectStoreCleaner(config);
        this.capabilitySigner = new CapabilitySigner(config);
        this.metadataStore = new CoordinatorMetadataStore(config);
    }

    boolean metadataEnabled() {
        return metadataStore.enabled();
    }

    CoordinatorMetadataStore metadataStore() {
        return metadataStore;
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
        body.put("icebergHiveLockEnabled", config.icebergHiveLockEnabled);
        body.put("capabilitySigningConfigured", config.capabilitySecret.isPresent());
        body.put("adminTokenConfigured", config.adminToken.isPresent());
        body.put("metadataDatabaseConfigured", metadataStore.enabled());
        body.put("metricsEnabled", config.coordinatorMetricsEnabled);
        body.put("metricsAddress", config.coordinatorMetricsAddress.toString());
        body.put("queryRegistryTtlMs", config.queryRegistryTtlMs);
        body.put("workerClientEndpointsRequired", config.workerClientEndpointsRequired);
        body.put("workerSelectionGraceMs", config.workerSelectionGraceMs);
        body.put("k8sWorkerDiscoveryEnabled", config.k8sWorkerDiscoveryEnabled);
        body.put("k8sNamespace", config.k8sNamespace);
        body.put("k8sWorkerServiceSelector", config.k8sWorkerServiceSelector);
        body.put("workerClientUriScheme", config.workerClientUriScheme);
        body.put("tempCleanupAction", "coordinator.drop-temp");
        return body;
    }

    Map<String, Object> createUpload(Map<String, Object> request) {
        return createUpload(request, WorkerEndpointRewrite.NONE);
    }

    Map<String, Object> createUpload(Map<String, Object> request, WorkerEndpointRewrite endpointRewrite) {
        requireAdminIfConfigured(request);
        cleanupQueriesIfDue();

        String operationId = stringOrDefault(request, "operationId", UUID.randomUUID().toString());
        String uploadId = stringOrDefault(request, "uploadId", operationId);
        String tableName = Optional.ofNullable(Json.string(request, "tableName"))
                .filter(value -> !value.isBlank())
                .orElseGet(() -> config.generatedUploadTable(uploadId));
        String outputPrefix = tableDataPrefix(tableName);
        String mode = requestedCommitMode(request).orElse("append");
        String trinoUser = stringOrDefault(request, "user", "anonymous");
        Optional<String> authorization = authorizationFrom(request);
        long targetFileSize = Json.longValue(request, "targetFileSizeBytes", config.defaultTargetFileSizeBytes);
        Optional<Long> maxStreamBytes = optionalLong(request, "maxStreamBytes", config.defaultMaxStreamBytes);
        Optional<Long> maxRecordBatchBytes = optionalLong(
                request,
                "maxRecordBatchBytes",
                config.defaultPutMaxRecordBatchBytes
        );
        Optional<UploadSnapshot> existing = metadataStore.loadUploadIfExists(uploadId);
        if (existing.isPresent()) {
            return createUploadResponse(
                    existing.get(),
                    request,
                    tableName,
                    mode,
                    targetFileSize,
                    maxStreamBytes,
                    maxRecordBatchBytes,
                    Optional.empty(),
                    true,
                    endpointRewrite
            );
        }

        int requestedStreams = Math.max(1, Math.min(
                Json.intValue(request, "streams", config.defaultUploadStreams),
                config.defaultMaxUploadStreams
        ));
        List<WorkerAssignment> workerAssignments = metadataStore.selectPutWorkers(requestedStreams);
        int streams = workerAssignments.size();
        Instant expiresAt = Instant.now().plusMillis(Json.longValue(
                request,
                "ttlMs",
                config.uploadSessionTtlMs
        ));

        ArrayList<PlannedUploadStream> plannedStreams = new ArrayList<>();
        for (int index = 0; index < streams; index++) {
            WorkerAssignment worker = workerAssignments.get(index);
            String streamId = "stream-%05d".formatted(index);
            String attemptId = UUID.randomUUID().toString();
            String descriptorPath = outputPrefix + "/flight-" + UUID.randomUUID() + ".parquet";
            plannedStreams.add(new PlannedUploadStream(
                    streamId,
                    attemptId,
                    worker.workerId(),
                    worker.flightUri(),
                    descriptorPath,
                    Map.of()
            ));
        }

        PlannedUploadSession session = new PlannedUploadSession(
                uploadId,
                operationId,
                Optional.of(tableName),
                mode.equals("overwrite") ? "PREPARING" : "PLANNED",
                streams,
                outputPrefix,
                targetFileSize,
                maxStreamBytes,
                maxRecordBatchBytes,
                mode,
                expiresAt
        );
        if (!metadataStore.tryCreateUpload(session, plannedStreams)) {
            return createUploadResponse(
                    metadataStore.loadUpload(uploadId),
                    request,
                    tableName,
                    mode,
                    targetFileSize,
                    maxStreamBytes,
                    maxRecordBatchBytes,
                    Optional.empty(),
                    true,
                    endpointRewrite
            );
        }

        Optional<Map<String, Object>> overwritePreparation = Optional.empty();
        try {
            if (mode.equals("overwrite")) {
                overwritePreparation = Optional.of(prepareOverwriteUpload(tableName, trinoUser, authorization));
                metadataStore.markPlanned(uploadId);
            }
        } catch (RuntimeException error) {
            metadataStore.markFailed(uploadId, "failed to prepare overwrite upload: " + error.getMessage());
            throw error;
        }

        return createUploadResponse(
                metadataStore.loadUpload(uploadId),
                request,
                tableName,
                mode,
                targetFileSize,
                maxStreamBytes,
                maxRecordBatchBytes,
                overwritePreparation,
                false,
                endpointRewrite
        );
    }

    private LinkedHashMap<String, Object> createUploadResponse(
            UploadSnapshot snapshot,
            Map<String, Object> request,
            String requestedTableName,
            String requestedMode,
            long requestedTargetFileSize,
            Optional<Long> requestedMaxStreamBytes,
            Optional<Long> requestedMaxRecordBatchBytes,
            Optional<Map<String, Object>> overwritePreparation,
            boolean alreadyCreated,
            WorkerEndpointRewrite endpointRewrite
    ) {
        UploadSessionRecord session = snapshot.session();
        validateCreateUploadRetry(
                session,
                requestedTableName,
                requestedMode,
                requestedTargetFileSize,
                requestedMaxStreamBytes,
                requestedMaxRecordBatchBytes
        );
        if (session.status().equals("PREPARING")) {
            LinkedHashMap<String, Object> body =
                    baseCreateUploadResponse(session, snapshot, request, alreadyCreated, endpointRewrite);
            body.put("status", "PREPARING");
            body.put("retryAfterMs", 500);
            body.put("tickets", List.of());
            return body;
        }
        if (session.status().equals("FAILED")) {
            return failedUploadResponse(snapshot, endpointRewrite);
        }
        if (session.status().equals("COMMITTING")) {
            throw new CoordinatorException(
                    409,
                    "upload session " + session.uploadId()
                            + " is already committing; wait for commit-upload to finish or retry commit-upload with the same uploadId"
            );
        }
        if (session.status().equals("COMMITTED")) {
            throw new CoordinatorException(
                    409,
                    "upload session " + session.uploadId() + " is already committed; create a new uploadId"
            );
        }
        if (session.status().equals("ABORTED")) {
            throw new CoordinatorException(
                    409,
                    "upload session " + session.uploadId() + " was aborted and cannot issue upload tickets"
            );
        }

        List<Map<String, Object>> ticketBodies = snapshot.streams().stream()
                .map(stream -> uploadTicket(session, stream, endpointRewrite))
                .toList();
        LinkedHashMap<String, Object> body =
                baseCreateUploadResponse(session, snapshot, request, alreadyCreated, endpointRewrite);
        body.put("status", session.status());
        overwritePreparation.ifPresent(value -> body.put("overwritePreparation", value));
        body.put("tickets", ticketBodies);
        return body;
    }

    private LinkedHashMap<String, Object> baseCreateUploadResponse(
            UploadSessionRecord session,
            UploadSnapshot snapshot,
            Map<String, Object> request,
            boolean alreadyCreated,
            WorkerEndpointRewrite endpointRewrite
    ) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", session.uploadId());
        body.put("operationId", session.operationId());
        session.tableName().ifPresent(value -> body.put("tableName", value));
        session.commitMode().ifPresent(value -> body.put("mode", value));
        body.put("requestedStreams", Json.intValue(request, "streams", session.expectedStreams()));
        body.put("grantedStreams", session.expectedStreams());
        body.put("expectedStreams", session.expectedStreams());
        body.put("stagingPrefix", session.stagingPrefix() + "/");
        body.put("outputPrefix", session.stagingPrefix() + "/");
        body.put("targetFileSizeBytes", session.targetFileSize());
        body.put("expiresAtMs", session.expiresAt().toEpochMilli());
        body.put("selectedWorkers", snapshot.streams().stream()
                .map(stream -> selectedWorkerJson(stream, endpointRewrite))
                .toList());
        body.put("alreadyCreated", alreadyCreated);
        return body;
    }

    private void validateCreateUploadRetry(
            UploadSessionRecord session,
            String requestedTableName,
            String requestedMode,
            long requestedTargetFileSize,
            Optional<Long> requestedMaxStreamBytes,
            Optional<Long> requestedMaxRecordBatchBytes
    ) {
        if (session.tableName().isPresent() && !session.tableName().get().equals(requestedTableName)) {
            throw new CoordinatorException(
                    409,
                    "create-upload retry conflicts with existing uploadId " + session.uploadId()
                            + ": tableName was " + session.tableName().get()
                            + " but request asked for " + requestedTableName
            );
        }
        if (session.commitMode().isPresent() && !session.commitMode().get().equals(requestedMode)) {
            throw new CoordinatorException(
                    409,
                    "create-upload retry conflicts with existing uploadId " + session.uploadId()
                            + ": mode was " + session.commitMode().get()
                            + " but request asked for " + requestedMode
            );
        }
        if (session.targetFileSize() != requestedTargetFileSize) {
            throw new CoordinatorException(
                    409,
                    "create-upload retry conflicts with existing uploadId " + session.uploadId()
                            + ": targetFileSizeBytes was " + session.targetFileSize()
                            + " but request asked for " + requestedTargetFileSize
            );
        }
        if (!Objects.equals(session.maxStreamBytes(), requestedMaxStreamBytes)) {
            throw new CoordinatorException(
                    409,
                    "create-upload retry conflicts with existing uploadId " + session.uploadId()
                            + ": maxStreamBytes was " + optionalLongForMessage(session.maxStreamBytes())
                            + " but request asked for " + optionalLongForMessage(requestedMaxStreamBytes)
            );
        }
        if (!Objects.equals(session.maxRecordBatchBytes(), requestedMaxRecordBatchBytes)) {
            throw new CoordinatorException(
                    409,
                    "create-upload retry conflicts with existing uploadId " + session.uploadId()
                            + ": maxRecordBatchBytes was " + optionalLongForMessage(session.maxRecordBatchBytes())
                            + " but request asked for " + optionalLongForMessage(requestedMaxRecordBatchBytes)
            );
        }
    }

    private Map<String, Object> uploadTicket(
            UploadSessionRecord session,
            UploadStreamState stream,
            WorkerEndpointRewrite endpointRewrite
    ) {
        long ttlMs = session.expiresAt().toEpochMilli() - Instant.now().toEpochMilli();
        if (ttlMs <= 0) {
            throw new CoordinatorException(
                    409,
                    "upload session " + session.uploadId() + " is expired; create a new upload"
            );
        }

        LinkedHashMap<String, Object> ticketRequest = new LinkedHashMap<>();
        ticketRequest.put("operationId", session.operationId());
        ticketRequest.put("attemptId", stream.attemptId());
        ticketRequest.put("uploadId", session.uploadId());
        ticketRequest.put("streamId", stream.streamId());
        ticketRequest.put("workerId", stream.workerId());
        ticketRequest.put("flightUri", endpointRewrite.rewrite(stream.workerId(), stream.flightUri()));
        ticketRequest.put("stagingPrefix", session.stagingPrefix());
        ticketRequest.put("path", stream.descriptorPath());
        ticketRequest.put("targetFileSizeBytes", session.targetFileSize());
        session.maxStreamBytes().ifPresent(value -> ticketRequest.put("maxStreamBytes", value));
        session.maxRecordBatchBytes().ifPresent(value -> ticketRequest.put("maxRecordBatchBytes", value));
        ticketRequest.put("maxUploadStreams", session.expectedStreams());
        ticketRequest.put("ttlMs", ttlMs);
        return capabilitySigner.putPayload(ticketRequest);
    }

    private Map<String, Object> selectedWorkerJson(UploadStreamState stream, WorkerEndpointRewrite endpointRewrite) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("streamId", stream.streamId());
        body.put("workerId", stream.workerId());
        body.put("flightUri", endpointRewrite.rewrite(stream.workerId(), stream.flightUri()));
        return body;
    }

    Map<String, Object> commitUpload(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        String uploadId = Json.requiredString(request, "uploadId");
        UploadSnapshot snapshot = metadataStore.loadUpload(uploadId);
        if (snapshot.session().status().equals("COMMITTED")) {
            return committedUploadResponse(snapshot);
        }
        if (snapshot.session().status().equals("FAILED")) {
            return failedUploadResponse(snapshot);
        }
        if (snapshot.session().status().equals("COMMITTING")) {
            Optional<Map<String, Object>> recovered = recoverCommittedUpload(snapshot, request);
            if (recovered.isPresent()) {
                return recovered.get();
            }
            throw new CoordinatorException(
                    409,
                    "upload session " + uploadId + " is already committing; retry commit-upload with the same uploadId"
            );
        }
        if (snapshot.session().status().equals("ABORTED")) {
            throw new CoordinatorException(
                    409,
                    "upload session " + uploadId + " is not available for commit; status=" + snapshot.session().status()
            );
        }

        UploadReadyPlan plan = uploadReadyPlan(snapshot, request);
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

        String mode = effectiveCommitMode(request, snapshot.session().commitMode());
        String trinoUser = stringOrDefault(request, "user", "anonymous");
        Optional<String> authorization = authorizationFrom(request);
        boolean tableExistedBeforeCommit = true;
        boolean tableCreatedByCoordinator = false;
        boolean icebergCommitStarted = false;
        boolean icebergCommitCompleted = false;
        try {
            tableExistedBeforeCommit = icebergCommitter.tableExists(plan.tableName());
            Optional<Map<String, Object>> appendCompatibility = Optional.empty();
            if (mode.equals("append")) {
                if (!tableExistedBeforeCommit) {
                    throw new CoordinatorException(
                            409,
                            "append commit requires an existing Iceberg table when worker files are written directly "
                                    + "under table data location; use overwrite to recreate the table"
                    );
                }
                appendCompatibility = Optional.of(icebergCommitter.validateAppendSchema(
                        plan.tableName(),
                        plan.arrowSchema()
                ));
            } else if (!tableExistedBeforeCommit) {
                tableCreatedByCoordinator = icebergCommitter.createTableIfMissing(
                        plan.tableName(),
                        plan.arrowSchema(),
                        tableLocation(plan.tableName())
                );
            }
            icebergCommitStarted = true;
            CommitOutcome outcome = icebergCommitter.commit(uploadId, plan.tableName(), mode, snapshot.files());
            icebergCommitCompleted = true;
            LinkedHashMap<String, Object> summary = new LinkedHashMap<>(outcome.summary());
            appendCompatibility.ifPresent(value -> summary.put("appendSchemaCompatibility", value));
            summary.put("tableExistedBeforeCommit", tableExistedBeforeCommit);
            summary.put("tableCreatedByCoordinator", tableCreatedByCoordinator);
            metadataStore.markCommitted(uploadId, new CommitMetadata(
                    plan.tableName(),
                    outcome.mode(),
                    outcome.snapshotId(),
                    Optional.of(plan.createTableSql()),
                    summary
            ));

            LinkedHashMap<String, Object> body = uploadPlanResponse(plan);
            body.put("status", "COMMITTED");
            body.put("mode", outcome.mode());
            body.put("snapshotId", outcome.snapshotId());
            body.put("recordCount", outcome.recordCount());
            body.put("parquetObjectBytes", outcome.parquetObjectBytes());
            body.put("commitSummary", summary);
            return body;
        } catch (Exception error) {
            if (icebergCommitStarted) {
                try {
                    Optional<Map<String, Object>> recovered = recoverCommittedUpload(snapshot, request);
                    if (recovered.isPresent()) {
                        return recovered.get();
                    }
                } catch (RuntimeException recoveryError) {
                    String commitState = icebergCommitCompleted ? "completed" : "started";
                    throw new CoordinatorException(
                            500,
                            "Iceberg commit for upload " + uploadId + " " + commitState
                                    + ", but coordinator failed while reconciling the committed snapshot: "
                                    + recoveryError.getMessage()
                                    + "; no staged files were deleted because they may now be table data",
                            recoveryError
                    );
                }
            }
            if (icebergCommitCompleted) {
                throw new CoordinatorException(
                        500,
                        "Iceberg commit for upload " + uploadId + " completed"
                                + ", but coordinator failed at or after the snapshot commit boundary: "
                                + error.getMessage()
                                + "; no staged files were deleted because they may now be table data",
                        error
                );
            }
            CleanupResult stagedCleanup = objectStoreCleaner.deleteUploadObjects(snapshot);
            Optional<Map<String, Object>> tableCleanup = mode.equals("overwrite") && !tableExistedBeforeCommit
                    ? Optional.of(dropTableBestEffort(plan.tableName(), trinoUser, authorization))
                    : Optional.empty();
            int status = error instanceof CoordinatorException coordinatorError ? coordinatorError.status : 500;
            String message = icebergCommitStarted
                    ? "failed to commit upload " + uploadId
                            + " before a committed Iceberg snapshot became visible: " + error.getMessage()
                    : "failed to commit upload " + uploadId + ": " + error.getMessage();
            metadataStore.markFailed(uploadId, message);
            LinkedHashMap<String, Object> cleanup = new LinkedHashMap<>();
            cleanup.put("staging", stagedCleanup.toJson());
            tableCleanup.ifPresent(value -> cleanup.put("table", value));
            throw new CoordinatorException(status, message + "; cleanup=" + cleanup, error);
        }
    }

    private Optional<Map<String, Object>> recoverCommittedUpload(UploadSnapshot snapshot, Map<String, Object> request) {
        UploadSessionRecord session = snapshot.session();
        Optional<String> tableName = session.commitTableName()
                .or(session::tableName)
                .or(() -> Optional.ofNullable(Json.string(request, "tableName")).filter(value -> !value.isBlank()));
        if (tableName.isEmpty()) {
            return Optional.empty();
        }

        Optional<CommitOutcome> recovered = icebergCommitter.committedUpload(
                tableName.get(),
                session.uploadId(),
                snapshot.files()
        );
        if (recovered.isEmpty()) {
            return Optional.empty();
        }

        CommitOutcome outcome = recovered.get();
        metadataStore.markCommitted(session.uploadId(), new CommitMetadata(
                tableName.get(),
                outcome.mode(),
                outcome.snapshotId(),
                session.createTableSql(),
                outcome.summary()
        ));
        LinkedHashMap<String, Object> body = committedUploadResponse(metadataStore.loadUpload(session.uploadId()));
        body.put("recoveredFromIcebergSnapshot", true);
        return Optional.of(body);
    }

    private UploadReadyPlan uploadReadyPlan(UploadSnapshot snapshot, Map<String, Object> request) {
        String uploadId = snapshot.session().uploadId();
        if (snapshot.files().isEmpty()) {
            throw new CoordinatorException(
                    409,
                    "upload " + uploadId + " has no recorded parquet files yet; finish DoPut before commit-upload"
            );
        }

        String tableName = Optional.ofNullable(Json.string(request, "tableName"))
                .filter(value -> !value.isBlank())
                .or(() -> snapshot.session().tableName())
                .or(() -> snapshot.session().commitTableName())
                .orElseGet(() -> config.generatedUploadTable(uploadId));
        Map<String, Object> arrowSchema = Json.parseObject(snapshot.canonicalSchemaJsonForFiles());
        String createTableSql;
        try {
            createTableSql = TrinoDdlPlanner.createTableSql(
                    tableName,
                    arrowSchema,
                    tableLocation(tableName),
                    true
            );
        } catch (RuntimeException error) {
            objectStoreCleaner.deleteUploadObjects(snapshot);
            String message = "upload schema is not supported for Iceberg table planning: " + error.getMessage();
            metadataStore.markFailed(uploadId, message);
            throw new CoordinatorException(400, message, error);
        }
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

    private String tableDataPrefix(String tableName) {
        return objectPrefixFromUri(tableLocation(tableName) + "/data");
    }

    private String tablePrefix(String tableName) {
        return objectPrefixFromUri(tableLocation(tableName));
    }

    private LinkedHashMap<String, Object> uploadPlanResponse(UploadReadyPlan plan) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", plan.snapshot().session().uploadId());
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

    private LinkedHashMap<String, Object> failedUploadResponse(UploadSnapshot snapshot) {
        return failedUploadResponse(snapshot, WorkerEndpointRewrite.NONE);
    }

    private LinkedHashMap<String, Object> failedUploadResponse(
            UploadSnapshot snapshot,
            WorkerEndpointRewrite endpointRewrite
    ) {
        UploadSessionRecord session = snapshot.session();
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", session.uploadId());
        body.put("status", session.status());
        session.errorMessage().ifPresent(value -> body.put("errorMessage", value));
        session.tableName().ifPresent(value -> body.put("tableName", value));
        body.put("files", snapshot.files().stream().map(UploadFile::toJson).toList());
        body.put("streams", snapshot.streams().stream()
                .map(stream -> streamJson(stream, endpointRewrite))
                .toList());
        return body;
    }

    private Map<String, Object> streamJson(UploadStreamState stream, WorkerEndpointRewrite endpointRewrite) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>(stream.toJson());
        body.put("flightUri", endpointRewrite.rewrite(stream.workerId(), stream.flightUri()));
        return body;
    }

    Map<String, Object> abortUpload(Map<String, Object> request) {
        requireAdminIfConfigured(request);
        String uploadId = Json.requiredString(request, "uploadId");
        String reason = Optional.ofNullable(Json.string(request, "reason"))
                .filter(value -> !value.isBlank())
                .orElse("aborted by coordinator request");
        UploadSnapshot snapshot = metadataStore.loadUpload(uploadId);
        if (snapshot.session().status().equals("COMMITTED")) {
            throw new CoordinatorException(409, "committed upload " + uploadId + " cannot be aborted");
        }
        CleanupResult cleanup = objectStoreCleaner.deleteUploadObjects(snapshot);
        metadataStore.markAborted(uploadId, reason);
        return Map.of(
                "uploadId", uploadId,
                "status", "ABORTED",
                "cleanup", Map.of("staging", cleanup.toJson())
        );
    }

    Map<String, Object> dropTemp(Map<String, Object> request) {
        cleanupQueriesIfDue();
        String queryId = Json.requiredString(request, "queryId");
        QueryRegistryRecord record = metadataStore.loadQuery(queryId);
        if (!record.queryType().equals("ctas")) {
            throw new CoordinatorException(400, "drop-temp only applies to CTAS query ids");
        }
        String trinoUser = stringOrDefault(request, "user", record.trinoUser().orElse("anonymous"));
        Optional<String> authorization = authorizationFrom(request);
        Optional<String> targetTable = record.targetTable();
        Optional<Map<String, Object>> tableDrop = targetTable.map(table -> dropTable(table, trinoUser, authorization));
        CleanupResult stagingCleanup = objectStoreCleaner.deleteCtasStaging(queryId);

        LinkedHashMap<String, Object> metadata = baseFlightMetadata(queryId, "ctas", "DROPPED", Optional.of(1.0));
        metadata.put("phase", "dropped");
        targetTable.ifPresent(value -> metadata.put("targetTable", value));
        tableDrop.ifPresent(value -> metadata.put("tableDrop", value));
        metadata.put("stagingCleanup", stagingCleanup.toJson());
        metadataStore.markQueryDropped(queryId, metadata);

        LinkedHashMap<String, Object> body = new LinkedHashMap<>(metadata);
        body.put("queryId", queryId);
        body.put("status", "DROPPED");
        body.put("cleanup", Map.of(
                "table", tableDrop.orElse(Map.of("skipped", true, "reason", "query has no target table")),
                "staging", stagingCleanup.toJson()
        ));
        return body;
    }

    Map<String, Object> putTicket(Map<String, Object> request) {
        return putTicket(request, WorkerEndpointRewrite.NONE);
    }

    Map<String, Object> putTicket(Map<String, Object> request, WorkerEndpointRewrite endpointRewrite) {
        requireAdminIfConfigured(request);
        WorkerAssignment worker = metadataStore.selectPutWorkers(1).getFirst();
        LinkedHashMap<String, Object> signedRequest = new LinkedHashMap<>(request);
        signedRequest.put("workerId", worker.workerId());
        signedRequest.put("flightUri", endpointRewrite.rewrite(worker.workerId(), worker.flightUri()));
        return capabilitySigner.putPayload(signedRequest);
    }

    Map<String, Object> getTicket(Map<String, Object> request) {
        return getTicket(request, WorkerEndpointRewrite.NONE);
    }

    Map<String, Object> getTicket(Map<String, Object> request, WorkerEndpointRewrite endpointRewrite) {
        requireAdminIfConfigured(request);
        return endpointRewrite.rewriteEndpoint(planReadEndpoint(
                stringOrDefault(request, "operationId", "read-" + UUID.randomUUID()),
                Config.normalizePath(Json.requiredString(request, "path")),
                request
        ));
    }

    FlightPlan startFlight(Map<String, Object> request) {
        return startFlight(request, WorkerEndpointRewrite.NONE);
    }

    FlightPlan startFlight(Map<String, Object> request, WorkerEndpointRewrite endpointRewrite) {
        cleanupQueriesIfDue();
        String type = requestType(request);
        FlightPlan plan = switch (type) {
            case "read" -> startRead(request);
            case "ctas" -> startCtas(request);
            default -> throw new CoordinatorException(400, "unsupported GetFlightInfo request type: " + type);
        };
        return rewritePlan(plan, endpointRewrite);
    }

    PollResult pollFlight(Map<String, Object> request) {
        return pollFlight(request, WorkerEndpointRewrite.NONE);
    }

    PollResult pollFlight(Map<String, Object> request, WorkerEndpointRewrite endpointRewrite) {
        cleanupQueriesIfDue();
        String queryId = Json.requiredString(request, "queryId");
        QueryRegistryRecord record = metadataStore.loadQuery(queryId);
        if (record.terminal()) {
            return new PollResult(rewritePlan(flightPlanFromRecord(record), endpointRewrite), true, record.progress(), record.expiresAt());
        }
        if (Instant.now().isAfter(record.expiresAt())) {
            metadataStore.markQueryFailed(queryId, "query registry entry expired", record.trinoStatsJson());
            QueryRegistryRecord failed = metadataStore.loadQuery(queryId);
            return new PollResult(rewritePlan(flightPlanFromRecord(failed), endpointRewrite), true, failed.progress(), failed.expiresAt());
        }
        if (!record.queryType().equals("ctas")) {
            return new PollResult(rewritePlan(flightPlanFromRecord(record), endpointRewrite), record.terminal(), record.progress(), record.expiresAt());
        }

        try {
            QueryRegistryRecord updated = advanceCtas(record, request);
            return new PollResult(rewritePlan(flightPlanFromRecord(updated), endpointRewrite), updated.terminal(), updated.progress(), updated.expiresAt());
        } catch (Exception error) {
            metadataStore.markQueryFailed(queryId, error.getMessage(), record.trinoStatsJson());
            QueryRegistryRecord failed = metadataStore.loadQuery(queryId);
            return new PollResult(rewritePlan(flightPlanFromRecord(failed), endpointRewrite), true, failed.progress(), failed.expiresAt());
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
                .orElseGet(() -> config.generatedCtasTable(queryId)));
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

    private FlightPlan rewritePlan(FlightPlan plan, WorkerEndpointRewrite endpointRewrite) {
        if (!endpointRewrite.enabled() || plan.endpoints().isEmpty()) {
            return plan;
        }
        List<Map<String, Object>> endpoints = plan.endpoints().stream()
                .map(endpointRewrite::rewriteEndpoint)
                .toList();
        return new FlightPlan(
                plan.queryId(),
                plan.status(),
                plan.metadata(),
                endpoints,
                plan.totalRecords(),
                plan.totalBytes(),
                plan.expiresAt()
        );
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

    private String objectPrefixFromUri(String raw) {
        for (String prefix : objectStoreUriPrefixes()) {
            String normalizedPrefix = prefix + "/";
            if (raw.startsWith(normalizedPrefix)) {
                return Config.normalizePrefix(raw.substring(normalizedPrefix.length()));
            }
        }
        return Config.normalizePrefix(raw);
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
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    private Map<String, Object> dropTable(String tableName, String trinoUser, Optional<String> authorization) {
        try {
            TrinoClient.QueryHandle handle = trinoClient.runStatement(
                    SqlPlanner.buildDropTable(tableName),
                    trinoUser,
                    authorization
            );
            LinkedHashMap<String, Object> body = new LinkedHashMap<>();
            body.put("tableName", tableName);
            body.put("dropped", true);
            handle.queryId().ifPresent(value -> body.put("trinoQueryId", value));
            return body;
        } catch (Exception error) {
            throw new CoordinatorException(500, "failed to drop table " + tableName + ": " + error.getMessage(), error);
        }
    }

    private Map<String, Object> prepareOverwriteUpload(
            String tableName,
            String trinoUser,
            Optional<String> authorization
    ) {
        Map<String, Object> tableDrop = dropTable(tableName, trinoUser, authorization);
        CleanupResult locationCleanup = objectStoreCleaner.deletePrefix(tablePrefix(tableName));
        if (!locationCleanup.succeeded()) {
            throw new CoordinatorException(
                    500,
                    "failed to clean table location before overwrite upload " + tableName
                            + ": " + locationCleanup.errorMessage().orElse("unknown cleanup error")
            );
        }

        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("table", tableDrop);
        body.put("location", locationCleanup.toJson());
        return body;
    }

    private Map<String, Object> dropTableBestEffort(
            String tableName,
            String trinoUser,
            Optional<String> authorization
    ) {
        try {
            return dropTable(tableName, trinoUser, authorization);
        } catch (RuntimeException error) {
            return Map.of(
                    "tableName", tableName,
                    "dropped", false,
                    "errorMessage", error.getMessage()
            );
        }
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

    private static String optionalLongForMessage(Optional<Long> value) {
        return value.map(String::valueOf).orElse("<none>");
    }

    private static String effectiveCommitMode(Map<String, Object> request, Optional<String> persistedMode) {
        Optional<String> requestedMode = requestedCommitMode(request);
        Optional<String> normalizedPersisted = persistedMode
                .filter(value -> !value.isBlank())
                .map(CoordinatorService::normalizeCommitMode);
        if (requestedMode.isPresent()
                && normalizedPersisted.isPresent()
                && !requestedMode.get().equals(normalizedPersisted.get())) {
            throw new CoordinatorException(
                    409,
                    "commit mode " + requestedMode.get()
                            + " conflicts with upload planned mode " + normalizedPersisted.get()
            );
        }
        return requestedMode.or(() -> normalizedPersisted).orElse("append");
    }

    private static Optional<String> requestedCommitMode(Map<String, Object> request) {
        return Optional.ofNullable(Json.string(request, "mode"))
                .or(() -> Optional.ofNullable(Json.string(request, "commitMode")))
                .filter(value -> !value.isBlank())
                .map(CoordinatorService::normalizeCommitMode);
    }

    private static String normalizeCommitMode(String raw) {
        String value = raw.trim().toLowerCase(java.util.Locale.ROOT);
        return switch (value) {
            case "append" -> "append";
            case "overwrite", "replace" -> "overwrite";
            default -> throw new CoordinatorException(400, "commit mode must be append or overwrite");
        };
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
