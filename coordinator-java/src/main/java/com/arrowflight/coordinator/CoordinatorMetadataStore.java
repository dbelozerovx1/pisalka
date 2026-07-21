package com.arrowflight.coordinator;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import org.postgresql.util.PGobject;

final class CoordinatorMetadataStore {
    private final Config config;
    private final Optional<JdbcTarget> jdbcTarget;

    CoordinatorMetadataStore(Config config) {
        this.config = config;
        this.jdbcTarget = config.metadataDatabaseUrl.map(CoordinatorMetadataStore::parseJdbcTarget);
    }

    boolean enabled() {
        return jdbcTarget.isPresent();
    }

    long workerSelectionGraceMs() {
        return config.workerSelectionGraceMs;
    }

    void requireEnabled() {
        if (jdbcTarget.isEmpty()) {
            throw new CoordinatorException(503, "coordinator metadata database is not configured");
        }
    }

    void upsertWorkerClientEndpoint(WorkerClientEndpoint endpoint) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     INSERT INTO worker_client_endpoints (
                         worker_id,
                         flight_uri,
                         source,
                         observed_at,
                         first_observed_at,
                         expires_at,
                         error_message
                     ) VALUES (?, ?, ?, now(), now(), ?, ?)
                     ON CONFLICT (worker_id) DO UPDATE SET
                         flight_uri = EXCLUDED.flight_uri,
                         source = EXCLUDED.source,
                         observed_at = EXCLUDED.observed_at,
                         first_observed_at = CASE
                             WHEN worker_client_endpoints.flight_uri IS DISTINCT FROM EXCLUDED.flight_uri
                               OR worker_client_endpoints.expires_at <= now()
                             THEN EXCLUDED.first_observed_at
                             ELSE worker_client_endpoints.first_observed_at
                         END,
                         expires_at = EXCLUDED.expires_at,
                         error_message = EXCLUDED.error_message
                     """)) {
            statement.setString(1, endpoint.workerId());
            statement.setString(2, endpoint.flightUri());
            statement.setString(3, endpoint.source());
            statement.setTimestamp(4, Timestamp.from(endpoint.expiresAt()));
            statement.setString(5, endpoint.errorMessage().orElse(null));
            statement.executeUpdate();
        } catch (SQLException error) {
            throw new IllegalStateException("failed to upsert worker client endpoint", error);
        }
    }

    void deleteWorkerClientEndpoint(String workerId) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     DELETE FROM worker_client_endpoints
                     WHERE worker_id = ?
                     """)) {
            statement.setString(1, workerId);
            statement.executeUpdate();
        } catch (SQLException error) {
            throw new IllegalStateException("failed to delete worker client endpoint", error);
        }
    }

    boolean tryCreateUpload(
            String uploadId,
            UploadFlavor flavor,
            Function<PutWorkerPlan, PlannedUpload> planner
    ) {
        requireEnabled();
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT pg_advisory_xact_lock(8842213377441)"
                )) {
                    statement.execute();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT 1 FROM coordinator_upload_sessions WHERE upload_id = ?"
                )) {
                    statement.setString(1, uploadId);
                    try (ResultSet rows = statement.executeQuery()) {
                        if (rows.next()) {
                            connection.rollback();
                            return false;
                        }
                    }
                }

                PlannedUpload upload = planner.apply(planPutWorkers(connection, flavor));
                PlannedUploadSession session = upload.session();
                int inserted;
                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO coordinator_upload_sessions (
                            upload_id,
                            operation_id,
                            table_name,
                            status,
                            expected_streams,
                            staging_prefix,
                            upload_bucket,
                            target_file_size,
                            max_stream_bytes,
                            max_record_batch_bytes,
                            commit_mode,
                            upload_flavor,
                            expires_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                        ON CONFLICT (upload_id) DO NOTHING
                        """)) {
                    statement.setString(1, session.uploadId());
                    statement.setString(2, session.operationId());
                    statement.setString(3, session.tableName().orElse(null));
                    statement.setString(4, session.status());
                    statement.setInt(5, session.expectedStreams());
                    statement.setString(6, session.stagingPrefix());
                    statement.setString(7, session.uploadBucket());
                    statement.setLong(8, session.targetFileSize());
                    setNullableLong(statement, 9, session.maxStreamBytes());
                    setNullableLong(statement, 10, session.maxRecordBatchBytes());
                    statement.setString(11, session.commitMode());
                    statement.setString(12, session.uploadFlavor());
                    statement.setTimestamp(13, Timestamp.from(session.expiresAt()));
                    inserted = statement.executeUpdate();
                }

                if (inserted == 0) {
                    connection.rollback();
                    return false;
                }

                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO coordinator_upload_streams (
                            upload_id,
                            stream_id,
                            attempt_id,
                            worker_id,
                            flight_uri,
                            descriptor_path,
                            reservation_expires_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """)) {
                    for (PlannedUploadStream stream : upload.streams()) {
                        statement.setString(1, session.uploadId());
                        statement.setString(2, stream.streamId());
                        statement.setString(3, stream.attemptId());
                        statement.setString(4, stream.workerId());
                        statement.setString(5, stream.flightUri());
                        statement.setString(6, stream.descriptorPath());
                        statement.setTimestamp(7, Timestamp.from(stream.reservationExpiresAt()));
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }

                connection.commit();
                return true;
            } catch (SQLException | RuntimeException error) {
                connection.rollback();
                throw error;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to create upload session metadata", error);
        }
    }

    Optional<UploadSnapshot> loadUploadIfExists(String uploadId) {
        requireEnabled();
        try {
            return Optional.of(loadUpload(uploadId));
        } catch (CoordinatorException error) {
            if (error.status == 404) {
                return Optional.empty();
            }
            throw error;
        }
    }

    UploadSnapshot loadUpload(String uploadId) {
        requireEnabled();
        try (Connection connection = connect()) {
            UploadSessionRecord session = loadSession(connection, uploadId);
            List<UploadStreamState> streams = loadStreams(connection, uploadId);
            List<UploadFile> files = loadFiles(connection, uploadId);
            return new UploadSnapshot(session, streams, files);
        } catch (SQLException error) {
            throw new IllegalStateException("failed to load upload session metadata", error);
        }
    }

    private PutWorkerPlan planPutWorkers(Connection connection, UploadFlavor flavor) throws SQLException {
        List<WorkerRegistryEntry> workers = loadPutCandidates(connection);
        long totalCapacity = workers.stream().mapToLong(WorkerRegistryEntry::putCapacityStreams).sum();
        long totalAvailable = workers.stream().mapToLong(WorkerRegistryEntry::remainingPutStreams).sum();
        int clusterUtilizationPerMille = totalCapacity == 0
                ? 1_000
                : (int) Math.min(1_000L, 1_000L - Math.min(1_000L, totalAvailable * 1_000L / totalCapacity));
        int targetStreams = flavor.targetStreams(clusterUtilizationPerMille);
        ArrayList<WorkerAssignment> assignments = new ArrayList<>();

        while (assignments.size() < targetStreams) {
            boolean progressed = false;
            for (WorkerRegistryEntry worker : workers) {
                if (assignments.size() >= targetStreams) {
                    break;
                }
                if (!worker.canAssignPut()) {
                    continue;
                }
                assignments.add(worker.assignPut());
                progressed = true;
            }
            if (!progressed) {
                break;
            }
        }

        if (assignments.isEmpty()) {
            if (config.workerClientEndpointsRequired) {
                throw new CoordinatorException(
                        503,
                        "no live data-plane worker has available DoPut capacity and a fresh client endpoint"
                );
            }
            throw new CoordinatorException(503, "no live data-plane worker has available DoPut capacity");
        }
        return new PutWorkerPlan(assignments, clusterUtilizationPerMille);
    }

    WorkerAssignment selectReadWorker() {
        requireEnabled();
        String sql = config.workerClientEndpointsRequired
                ? """
                SELECT registry.worker_id,
                       client.flight_uri,
                       registry.read_recommended_streams,
                       registry.read_selection_score,
                       registry.read_utilization_per_mille,
                       registry.read_admission_wait_ms_ewma,
                       registry.read_throughput_bytes_per_sec_ewma,
                       registry.last_heartbeat_at
                FROM worker_registry registry
                JOIN worker_client_endpoints client
                  ON client.worker_id = registry.worker_id
                 AND client.expires_at > now()
                WHERE registry.state = 'ACTIVE'
                  AND registry.draining = false
                  AND registry.read_recommended_streams > 0
                  AND extract(epoch FROM (now() - registry.last_heartbeat_at)) * 1000 <= registry.registry_ttl_ms
                  AND extract(epoch FROM (now() - registry.first_seen_at)) * 1000 >= ?
                  AND extract(epoch FROM (now() - client.first_observed_at)) * 1000 >= ?
                ORDER BY registry.read_selection_score DESC,
                         registry.read_recommended_streams DESC,
                         registry.read_admission_wait_ms_ewma ASC,
                         registry.last_heartbeat_at DESC
                LIMIT 1
                """
                : """
                SELECT worker_id,
                       flight_uri,
                       read_recommended_streams,
                       read_selection_score,
                       read_utilization_per_mille,
                       read_admission_wait_ms_ewma,
                       read_throughput_bytes_per_sec_ewma,
                       last_heartbeat_at
                FROM worker_registry
                WHERE state = 'ACTIVE'
                  AND draining = false
                  AND read_recommended_streams > 0
                  AND extract(epoch FROM (now() - last_heartbeat_at)) * 1000 <= registry_ttl_ms
                  AND extract(epoch FROM (now() - first_seen_at)) * 1000 >= ?
                ORDER BY read_selection_score DESC,
                         read_recommended_streams DESC,
                         read_admission_wait_ms_ewma ASC,
                         last_heartbeat_at DESC
                LIMIT 1
                """;
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, config.workerSelectionGraceMs);
            if (config.workerClientEndpointsRequired) {
                statement.setLong(2, config.workerSelectionGraceMs);
            }
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    if (config.workerClientEndpointsRequired) {
                        throw new CoordinatorException(
                                503,
                                "no live data-plane worker has available DoGet capacity and a fresh client endpoint"
                        );
                    }
                    throw new CoordinatorException(503, "no live data-plane worker has available DoGet capacity");
                }
                return new WorkerAssignment(
                        rows.getString("worker_id"),
                        rows.getString("flight_uri"),
                        rows.getLong("read_selection_score"),
                        rows.getInt("read_utilization_per_mille"),
                        rows.getLong("read_admission_wait_ms_ewma"),
                        rows.getLong("read_throughput_bytes_per_sec_ewma")
                );
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to select DoGet worker from registry", error);
        }
    }

    List<WorkerEndpointSnapshot> listWorkerEndpoints() {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                SELECT registry.worker_id,
                       COALESCE(client.flight_uri, registry.flight_uri) AS flight_uri,
                       registry.flight_uri AS registry_flight_uri,
                       client.flight_uri AS client_flight_uri,
                       registry.put_recommended_streams,
                       registry.read_recommended_streams,
                       GREATEST(registry.put_utilization_per_mille, registry.read_utilization_per_mille) AS utilization_per_mille,
                       registry.first_seen_at,
                       client.first_observed_at,
                       registry.last_heartbeat_at
                FROM worker_registry registry
                LEFT JOIN worker_client_endpoints client
                  ON client.worker_id = registry.worker_id
                 AND client.expires_at > now()
                WHERE registry.state = 'ACTIVE'
                  AND registry.draining = false
                  AND extract(epoch FROM (now() - registry.last_heartbeat_at)) * 1000 <= registry.registry_ttl_ms
                  AND (? = false OR client.worker_id IS NOT NULL)
                ORDER BY registry.worker_id ASC
                """)) {
            statement.setBoolean(1, config.workerClientEndpointsRequired);
            try (ResultSet rows = statement.executeQuery()) {
                ArrayList<WorkerEndpointSnapshot> endpoints = new ArrayList<>();
                while (rows.next()) {
                    endpoints.add(new WorkerEndpointSnapshot(
                            rows.getString("worker_id"),
                            rows.getString("flight_uri"),
                            rows.getString("registry_flight_uri"),
                            Optional.ofNullable(rows.getString("client_flight_uri")),
                            rows.getInt("put_recommended_streams"),
                            rows.getInt("read_recommended_streams"),
                            rows.getInt("utilization_per_mille"),
                            rows.getTimestamp("first_seen_at").toInstant(),
                            nullableTimestamp(rows, "first_observed_at"),
                            workerSelectionEligibleAt(
                                    rows.getTimestamp("first_seen_at").toInstant(),
                                    nullableTimestamp(rows, "first_observed_at")
                            ),
                            rows.getTimestamp("last_heartbeat_at").toInstant()
                    ));
                }
                return endpoints;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to list live worker endpoints", error);
        }
    }

    void createQuery(QueryRegistryRecord record) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     INSERT INTO coordinator_query_registry (
                         query_id,
                         query_type,
                         status,
                         target_table,
                         submitted_sql,
                         trino_user,
                         trino_query_id,
                         trino_info_uri,
                         trino_next_uri,
                         trino_stats_json,
                         progress,
                         error_message,
                         result_flight_info_json,
                         result_tickets_json,
                         result_files_json,
                         expires_at,
                         completed_at,
                         updated_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                     """)) {
            statement.setString(1, record.queryId());
            statement.setString(2, record.queryType());
            statement.setString(3, record.status());
            statement.setString(4, record.targetTable().orElse(null));
            statement.setString(5, record.submittedSql().orElse(null));
            statement.setString(6, record.trinoUser().orElse(null));
            statement.setString(7, record.trinoQueryId().orElse(null));
            statement.setString(8, record.trinoInfoUri().orElse(null));
            statement.setString(9, record.trinoNextUri().orElse(null));
            statement.setObject(10, jsonb(record.trinoStatsJson()), Types.OTHER);
            setNullableDouble(statement, 11, record.progress());
            statement.setString(12, record.errorMessage().orElse(null));
            statement.setObject(13, jsonb(record.resultFlightInfoJson()), Types.OTHER);
            statement.setObject(14, jsonb(record.resultTicketsJson()), Types.OTHER);
            statement.setObject(15, jsonb(record.resultFilesJson()), Types.OTHER);
            statement.setTimestamp(16, Timestamp.from(record.expiresAt()));
            statement.setTimestamp(17, record.completedAt().map(Timestamp::from).orElse(null));
            statement.executeUpdate();
        } catch (SQLException error) {
            throw new IllegalStateException("failed to create query registry row", error);
        }
    }

    QueryRegistryRecord loadQuery(String queryId) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT query_id,
                            query_type,
                            status,
                            target_table,
                            submitted_sql,
                            trino_user,
                            trino_query_id,
                            trino_info_uri,
                            trino_next_uri,
                            trino_stats_json::text AS trino_stats_json,
                            progress,
                            error_message,
                            result_flight_info_json::text AS result_flight_info_json,
                            result_tickets_json::text AS result_tickets_json,
                            result_files_json::text AS result_files_json,
                            created_at,
                            updated_at,
                            expires_at,
                            completed_at
                     FROM coordinator_query_registry
                     WHERE query_id = ?
                     """)) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new CoordinatorException(404, "query registry row was not found");
                }
                return queryRecord(rows);
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to load query registry row", error);
        }
    }

    void markQueryRunning(
            String queryId,
            Optional<String> trinoQueryId,
            Optional<String> trinoInfoUri,
            Optional<String> trinoNextUri,
            Optional<Map<String, Object>> trinoStatsJson,
            Optional<Double> progress
    ) {
        markQueryRunning(
                queryId,
                trinoQueryId,
                trinoInfoUri,
                trinoNextUri,
                trinoStatsJson,
                progress,
                Optional.empty(),
                Optional.empty()
        );
    }

    void markQueryRunning(
            String queryId,
            Optional<String> trinoQueryId,
            Optional<String> trinoInfoUri,
            Optional<String> trinoNextUri,
            Optional<Map<String, Object>> trinoStatsJson,
            Optional<Double> progress,
            Optional<Map<String, Object>> resultFlightInfoJson,
            Optional<Map<String, Object>> resultFilesJson
    ) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_query_registry
                     SET status = 'RUNNING',
                         trino_query_id = COALESCE(?, trino_query_id),
                         trino_info_uri = COALESCE(?, trino_info_uri),
                         trino_next_uri = ?,
                         trino_stats_json = COALESCE(?, trino_stats_json),
                         progress = COALESCE(?, progress),
                         error_message = NULL,
                         result_flight_info_json = COALESCE(?, result_flight_info_json),
                         result_files_json = COALESCE(?, result_files_json),
                         updated_at = now()
                     WHERE query_id = ?
                     """)) {
            statement.setString(1, trinoQueryId.orElse(null));
            statement.setString(2, trinoInfoUri.orElse(null));
            statement.setString(3, trinoNextUri.orElse(null));
            statement.setObject(4, jsonb(trinoStatsJson), Types.OTHER);
            setNullableDouble(statement, 5, progress);
            statement.setObject(6, jsonb(resultFlightInfoJson), Types.OTHER);
            statement.setObject(7, jsonb(resultFilesJson), Types.OTHER);
            statement.setString(8, queryId);
            if (statement.executeUpdate() == 0) {
                throw new CoordinatorException(404, "query registry row was not found");
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to mark query running", error);
        }
    }

    void markQuerySucceeded(
            String queryId,
            Map<String, Object> resultFlightInfoJson,
            Map<String, Object> resultTicketsJson,
            Map<String, Object> resultFilesJson,
            Optional<Map<String, Object>> trinoStatsJson,
            Optional<Double> progress
    ) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_query_registry
                     SET status = 'SUCCEEDED',
                         trino_next_uri = NULL,
                         trino_stats_json = COALESCE(?, trino_stats_json),
                         progress = COALESCE(?, 1.0),
                         error_message = NULL,
                         result_flight_info_json = ?,
                         result_tickets_json = ?,
                         result_files_json = ?,
                         updated_at = now(),
                         completed_at = COALESCE(completed_at, now())
                     WHERE query_id = ?
                     """)) {
            statement.setObject(1, jsonb(trinoStatsJson), Types.OTHER);
            setNullableDouble(statement, 2, progress);
            statement.setObject(3, jsonb(Optional.of(resultFlightInfoJson)), Types.OTHER);
            statement.setObject(4, jsonb(Optional.of(resultTicketsJson)), Types.OTHER);
            statement.setObject(5, jsonb(Optional.of(resultFilesJson)), Types.OTHER);
            statement.setString(6, queryId);
            if (statement.executeUpdate() == 0) {
                throw new CoordinatorException(404, "query registry row was not found");
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to mark query succeeded", error);
        }
    }

    void markQueryFailed(String queryId, String errorMessage, Optional<Map<String, Object>> trinoStatsJson) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_query_registry
                     SET status = 'FAILED',
                         trino_next_uri = NULL,
                         trino_stats_json = COALESCE(?, trino_stats_json),
                         error_message = ?,
                         updated_at = now(),
                         completed_at = COALESCE(completed_at, now())
                     WHERE query_id = ?
                     """)) {
            statement.setObject(1, jsonb(trinoStatsJson), Types.OTHER);
            statement.setString(2, errorMessage);
            statement.setString(3, queryId);
            if (statement.executeUpdate() == 0) {
                throw new CoordinatorException(404, "query registry row was not found");
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to mark query failed", error);
        }
    }

    void markQueryDropped(String queryId, Map<String, Object> resultFlightInfoJson) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_query_registry
                     SET status = 'DROPPED',
                         trino_next_uri = NULL,
                         progress = 1.0,
                         error_message = NULL,
                         result_flight_info_json = ?,
                         result_tickets_json = ?,
                         updated_at = now(),
                         completed_at = COALESCE(completed_at, now())
                     WHERE query_id = ?
                     """)) {
            statement.setObject(1, jsonb(resultFlightInfoJson), Types.OTHER);
            statement.setObject(2, jsonb(Map.of("tickets", List.of())), Types.OTHER);
            statement.setString(3, queryId);
            if (statement.executeUpdate() == 0) {
                throw new CoordinatorException(404, "query registry row was not found");
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to mark query dropped", error);
        }
    }

    int cleanupExpiredQueries(Instant now) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     DELETE FROM coordinator_query_registry
                     WHERE expires_at < ?
                     """)) {
            statement.setTimestamp(1, Timestamp.from(now));
            return statement.executeUpdate();
        } catch (SQLException error) {
            throw new IllegalStateException("failed to cleanup expired query registry rows", error);
        }
    }

    WorkerRegistryCleanupResult cleanupStaleWorkers(long retentionMs) {
        requireEnabled();
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try {
                int markedStale;
                try (PreparedStatement statement = connection.prepareStatement("""
                        UPDATE worker_registry
                        SET state = 'STALE',
                            put_recommended_streams = 0,
                            put_available_streams = 0,
                            put_selection_score = 0,
                            read_recommended_streams = 0,
                            read_selection_score = 0
                        WHERE extract(epoch FROM (now() - last_heartbeat_at)) * 1000 > registry_ttl_ms
                          AND state <> 'STALE'
                        """)) {
                    markedStale = statement.executeUpdate();
                }

                int deleted;
                try (PreparedStatement statement = connection.prepareStatement("""
                        DELETE FROM worker_registry
                        WHERE extract(epoch FROM (now() - last_heartbeat_at)) * 1000
                            > registry_ttl_ms + ?
                        """)) {
                    statement.setLong(1, retentionMs);
                    deleted = statement.executeUpdate();
                }
                connection.commit();
                return new WorkerRegistryCleanupResult(markedStale, deleted);
            } catch (SQLException error) {
                connection.rollback();
                throw error;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to cleanup stale worker registry rows", error);
        }
    }

    Optional<String> tryClaimExpiredUpload(Instant now, Instant staleCleanupBefore) {
        requireEnabled();
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement("""
                    WITH candidate AS (
                        SELECT session.upload_id
                        FROM coordinator_upload_sessions session
                        WHERE session.expires_at < ?
                          AND (
                              session.status IN ('PREPARING', 'PLANNED', 'FAILED', 'ABORTED')
                              OR (session.status = 'CLEANING' AND session.updated_at < ?)
                              OR (
                                  session.status = 'CLEANUP_FAILED'
                                  AND session.upload_bucket IS NOT NULL
                                  AND session.updated_at < ?
                              )
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM coordinator_upload_streams planned
                              JOIN worker_put_streams actual
                                ON actual.attempt_id = planned.attempt_id
                              JOIN worker_registry registry
                                ON registry.worker_id = planned.worker_id
                              WHERE planned.upload_id = session.upload_id
                                AND actual.status IN ('ADMITTED', 'WRITING')
                                AND extract(epoch FROM (now() - registry.last_heartbeat_at)) * 1000
                                    <= registry.registry_ttl_ms
                          )
                        ORDER BY session.expires_at, session.upload_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE coordinator_upload_sessions session
                    SET status = 'CLEANING',
                        error_message = 'upload expired before commit; cleaning staged files',
                        updated_at = now()
                    FROM candidate
                    WHERE session.upload_id = candidate.upload_id
                    RETURNING session.upload_id
                    """)) {
                statement.setTimestamp(1, Timestamp.from(now));
                statement.setTimestamp(2, Timestamp.from(staleCleanupBefore));
                statement.setTimestamp(3, Timestamp.from(staleCleanupBefore));
                try (ResultSet rows = statement.executeQuery()) {
                    Optional<String> uploadId = rows.next()
                            ? Optional.of(rows.getString("upload_id"))
                            : Optional.empty();
                    connection.commit();
                    return uploadId;
                }
            } catch (SQLException error) {
                connection.rollback();
                throw error;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to claim expired upload for cleanup", error);
        }
    }

    void markExpired(String uploadId, String message) {
        updateCleanupStatus(uploadId, "EXPIRED", message, true);
    }

    void markCleanupFailed(String uploadId, String message) {
        updateCleanupStatus(uploadId, "CLEANUP_FAILED", message, false);
    }

    private void updateCleanupStatus(String uploadId, String status, String message, boolean completed) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_upload_sessions
                     SET status = ?,
                         error_message = ?,
                         updated_at = now(),
                         completed_at = CASE WHEN ? THEN COALESCE(completed_at, now()) ELSE completed_at END
                     WHERE upload_id = ?
                       AND status = 'CLEANING'
                     """)) {
            statement.setString(1, status);
            statement.setString(2, message);
            statement.setBoolean(3, completed);
            statement.setString(4, uploadId);
            if (statement.executeUpdate() == 0) {
                throw new IllegalStateException("expired upload cleanup claim was lost for " + uploadId);
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to update expired upload cleanup status", error);
        }
    }

    void markPlanned(String uploadId) {
        requireEnabled();
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement statement = connection.prepareStatement("""
                        UPDATE coordinator_upload_sessions
                        SET status = 'PLANNED',
                            error_message = NULL,
                            updated_at = now()
                        WHERE upload_id = ?
                        """)) {
                    statement.setString(1, uploadId);
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement("""
                        UPDATE coordinator_upload_streams
                        SET reservation_expires_at = ?
                        WHERE upload_id = ?
                        """)) {
                    statement.setTimestamp(
                            1,
                            Timestamp.from(Instant.now().plusMillis(config.putReservationTtlMs))
                    );
                    statement.setString(2, uploadId);
                    statement.executeUpdate();
                }
                connection.commit();
            } catch (SQLException error) {
                connection.rollback();
                throw error;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to mark upload session planned", error);
        }
    }

    boolean tryMarkCommitting(String uploadId, String tableName, String createTableSql) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_upload_sessions
                     SET status = 'COMMITTING',
                         table_name = COALESCE(?, table_name),
                         create_table_sql = COALESCE(?, create_table_sql),
                         error_message = NULL,
                         updated_at = now()
                     WHERE upload_id = ?
                       AND status IN ('PREPARING', 'PLANNED', 'FAILED')
                       AND expires_at > now()
                     """)) {
            statement.setString(1, tableName);
            statement.setString(2, createTableSql);
            statement.setString(3, uploadId);
            return statement.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new IllegalStateException("failed to mark upload session committing", error);
        }
    }

    void markCommitted(String uploadId, CommitMetadata metadata) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_upload_sessions
                     SET status = 'COMMITTED',
                         table_name = COALESCE(?, table_name),
                         create_table_sql = COALESCE(?, create_table_sql),
                         error_message = NULL,
                         commit_mode = ?,
                         commit_table_name = ?,
                         commit_snapshot_id = ?,
                         commit_summary_json = ?,
                         committed_at = now(),
                         updated_at = now(),
                         completed_at = COALESCE(completed_at, now())
                     WHERE upload_id = ?
                     """)) {
            statement.setString(1, metadata.tableName());
            statement.setString(2, metadata.createTableSql().orElse(null));
            statement.setString(3, metadata.mode());
            statement.setString(4, metadata.tableName());
            statement.setLong(5, metadata.snapshotId());
            statement.setObject(6, jsonb(metadata.summary()), Types.OTHER);
            statement.setString(7, uploadId);
            if (statement.executeUpdate() == 0) {
                throw new CoordinatorException(404, "upload session was not found");
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to mark upload session committed", error);
        }
    }

    void markFailed(String uploadId, String errorMessage) {
        updateStatus(uploadId, "FAILED", Optional.empty(), Optional.empty(), Optional.of(errorMessage), true);
    }

    void markAborted(String uploadId, String errorMessage) {
        updateStatus(uploadId, "ABORTED", Optional.empty(), Optional.empty(), Optional.of(errorMessage), true);
    }

    private void updateStatus(
            String uploadId,
            String status,
            Optional<String> tableName,
            Optional<String> createTableSql,
            Optional<String> errorMessage,
            boolean completed
    ) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_upload_sessions
                     SET status = ?,
                         table_name = COALESCE(?, table_name),
                         create_table_sql = COALESCE(?, create_table_sql),
                         error_message = ?,
                         updated_at = now(),
                         completed_at = CASE WHEN ? THEN COALESCE(completed_at, now()) ELSE completed_at END
                     WHERE upload_id = ?
                     """)) {
            statement.setString(1, status);
            statement.setString(2, tableName.orElse(null));
            statement.setString(3, createTableSql.orElse(null));
            statement.setString(4, errorMessage.orElse(null));
            statement.setBoolean(5, completed);
            statement.setString(6, uploadId);
            if (statement.executeUpdate() == 0) {
                throw new CoordinatorException(404, "upload session was not found");
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to update upload session metadata", error);
        }
    }

    private UploadSessionRecord loadSession(Connection connection, String uploadId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT upload_id,
                       operation_id,
                       table_name,
                       status,
                       expected_streams,
                       upload_flavor,
                       staging_prefix,
                       upload_bucket,
                       target_file_size,
                       max_stream_bytes,
                       max_record_batch_bytes,
                       create_table_sql,
                       commit_mode,
                       commit_table_name,
                       commit_snapshot_id,
                       commit_summary_json::text AS commit_summary_json,
                       error_message,
                       created_at,
                       updated_at,
                       expires_at,
                       committed_at,
                       completed_at
                FROM coordinator_upload_sessions
                WHERE upload_id = ?
                """)) {
            statement.setString(1, uploadId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new CoordinatorException(404, "upload session was not found");
                }
                return new UploadSessionRecord(
                        rows.getString("upload_id"),
                        rows.getString("operation_id"),
                        Optional.ofNullable(rows.getString("table_name")),
                        rows.getString("status"),
                        rows.getInt("expected_streams"),
                        rows.getString("upload_flavor"),
                        rows.getString("staging_prefix"),
                        Optional.ofNullable(rows.getString("upload_bucket")),
                        rows.getLong("target_file_size"),
                        nullableLong(rows, "max_stream_bytes"),
                        nullableLong(rows, "max_record_batch_bytes"),
                        Optional.ofNullable(rows.getString("create_table_sql")),
                        Optional.ofNullable(rows.getString("commit_mode")),
                        Optional.ofNullable(rows.getString("commit_table_name")),
                        nullableLong(rows, "commit_snapshot_id"),
                        nullableJsonObject(rows, "commit_summary_json"),
                        Optional.ofNullable(rows.getString("error_message")),
                        rows.getTimestamp("created_at").toInstant(),
                        rows.getTimestamp("updated_at").toInstant(),
                        rows.getTimestamp("expires_at").toInstant(),
                        Optional.ofNullable(rows.getTimestamp("committed_at")).map(Timestamp::toInstant),
                        Optional.ofNullable(rows.getTimestamp("completed_at")).map(Timestamp::toInstant)
                );
            }
        }
    }

    private List<WorkerRegistryEntry> loadPutCandidates(Connection connection) throws SQLException {
        String sql = config.workerClientEndpointsRequired
                ? """
                SELECT registry.worker_id,
                       client.flight_uri,
                       registry.put_capacity_streams,
                       GREATEST(
                           registry.put_available_streams - COALESCE(pending.reserved_streams, 0),
                           0
                       ) AS put_available_streams,
                       registry.put_max_streams_per_upload,
                       registry.put_selection_score,
                       registry.put_utilization_per_mille,
                       registry.put_admission_wait_ms_ewma,
                       registry.put_throughput_bytes_per_sec_ewma,
                       registry.last_heartbeat_at
                FROM worker_registry registry
                JOIN worker_client_endpoints client
                  ON client.worker_id = registry.worker_id
                 AND client.expires_at > now()
                LEFT JOIN LATERAL (
                    SELECT count(*) AS reserved_streams
                    FROM coordinator_upload_streams planned
                    JOIN coordinator_upload_sessions session
                      ON session.upload_id = planned.upload_id
                    LEFT JOIN worker_put_streams actual
                      ON actual.attempt_id = planned.attempt_id
                    WHERE planned.worker_id = registry.worker_id
                      AND planned.reservation_expires_at > now()
                      AND session.status IN ('PREPARING', 'PLANNED')
                      AND (
                          actual.attempt_id IS NULL
                          OR (
                              actual.status IN ('ADMITTED', 'WRITING')
                              AND actual.updated_at > registry.last_heartbeat_at
                          )
                      )
                ) pending ON true
                WHERE registry.state = 'ACTIVE'
                  AND registry.draining = false
                  AND extract(epoch FROM (now() - registry.last_heartbeat_at)) * 1000 <= registry.registry_ttl_ms
                  AND extract(epoch FROM (now() - registry.first_seen_at)) * 1000 >= ?
                  AND extract(epoch FROM (now() - client.first_observed_at)) * 1000 >= ?
                ORDER BY registry.put_selection_score DESC,
                         put_available_streams DESC,
                         registry.put_admission_wait_ms_ewma ASC,
                         registry.last_heartbeat_at DESC
                """
                : """
                SELECT worker_id,
                       flight_uri,
                       put_capacity_streams,
                       GREATEST(
                           put_available_streams - COALESCE(pending.reserved_streams, 0),
                           0
                       ) AS put_available_streams,
                       put_max_streams_per_upload,
                       put_selection_score,
                       put_utilization_per_mille,
                       put_admission_wait_ms_ewma,
                       put_throughput_bytes_per_sec_ewma,
                       last_heartbeat_at
                FROM worker_registry registry
                LEFT JOIN LATERAL (
                    SELECT count(*) AS reserved_streams
                    FROM coordinator_upload_streams planned
                    JOIN coordinator_upload_sessions session
                      ON session.upload_id = planned.upload_id
                    LEFT JOIN worker_put_streams actual
                      ON actual.attempt_id = planned.attempt_id
                    WHERE planned.worker_id = registry.worker_id
                      AND planned.reservation_expires_at > now()
                      AND session.status IN ('PREPARING', 'PLANNED')
                      AND (
                          actual.attempt_id IS NULL
                          OR (
                              actual.status IN ('ADMITTED', 'WRITING')
                              AND actual.updated_at > registry.last_heartbeat_at
                          )
                      )
                ) pending ON true
                WHERE registry.state = 'ACTIVE'
                  AND registry.draining = false
                  AND extract(epoch FROM (now() - registry.last_heartbeat_at)) * 1000 <= registry.registry_ttl_ms
                  AND extract(epoch FROM (now() - registry.first_seen_at)) * 1000 >= ?
                ORDER BY registry.put_selection_score DESC,
                         put_available_streams DESC,
                         registry.put_admission_wait_ms_ewma ASC,
                         registry.last_heartbeat_at DESC
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, config.workerSelectionGraceMs);
            if (config.workerClientEndpointsRequired) {
                statement.setLong(2, config.workerSelectionGraceMs);
            }
            try (ResultSet rows = statement.executeQuery()) {
                ArrayList<WorkerRegistryEntry> workers = new ArrayList<>();
                while (rows.next()) {
                    workers.add(new WorkerRegistryEntry(
                            rows.getString("worker_id"),
                            rows.getString("flight_uri"),
                            rows.getInt("put_capacity_streams"),
                            rows.getInt("put_available_streams"),
                            rows.getInt("put_max_streams_per_upload"),
                            rows.getLong("put_selection_score"),
                            rows.getInt("put_utilization_per_mille"),
                            rows.getLong("put_admission_wait_ms_ewma"),
                            rows.getLong("put_throughput_bytes_per_sec_ewma")
                    ));
                }
                return workers;
            }
        }
    }

    private List<UploadStreamState> loadStreams(Connection connection, String uploadId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT planned.stream_id,
                       planned.attempt_id,
                       planned.worker_id,
                       planned.flight_uri,
                       planned.descriptor_path,
                       planned.reservation_expires_at,
                       COALESCE(actual.status, 'PENDING') AS worker_status,
                       actual.error_message,
                       actual.rows,
                       actual.batches,
                       actual.parts,
                       actual.flight_stream_bytes,
                       actual.parquet_object_bytes,
                       actual.elapsed_ms,
                       actual.arrow_schema_json::text AS arrow_schema_json
                FROM coordinator_upload_streams planned
                LEFT JOIN worker_put_streams actual
                  ON actual.attempt_id = planned.attempt_id
                WHERE planned.upload_id = ?
                ORDER BY planned.stream_id
                """)) {
            statement.setString(1, uploadId);
            try (ResultSet rows = statement.executeQuery()) {
                ArrayList<UploadStreamState> streams = new ArrayList<>();
                while (rows.next()) {
                    streams.add(new UploadStreamState(
                            rows.getString("stream_id"),
                            rows.getString("attempt_id"),
                            rows.getString("worker_id"),
                            rows.getString("flight_uri"),
                            rows.getString("descriptor_path"),
                            rows.getTimestamp("reservation_expires_at").toInstant(),
                            rows.getString("worker_status"),
                            Optional.ofNullable(rows.getString("error_message")),
                            rows.getLong("rows"),
                            rows.getLong("batches"),
                            rows.getInt("parts"),
                            rows.getLong("flight_stream_bytes"),
                            nullableLong(rows, "parquet_object_bytes"),
                            nullableLong(rows, "elapsed_ms"),
                            Optional.ofNullable(rows.getString("arrow_schema_json"))
                    ));
                }
                return streams;
            }
        }
    }

    private List<UploadFile> loadFiles(Connection connection, String uploadId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT planned.stream_id,
                       planned.worker_id,
                       files.part_index,
                       files.file_path,
                       files.rows,
                       files.batches,
                       files.flight_stream_bytes,
                       files.parquet_object_bytes
                FROM worker_put_files files
                JOIN coordinator_upload_streams planned
                  ON planned.attempt_id = files.attempt_id
                WHERE planned.upload_id = ?
                ORDER BY planned.stream_id, files.part_index
                """)) {
            statement.setString(1, uploadId);
            try (ResultSet rows = statement.executeQuery()) {
                ArrayList<UploadFile> files = new ArrayList<>();
                while (rows.next()) {
                    files.add(new UploadFile(
                            rows.getString("stream_id"),
                            rows.getString("worker_id"),
                            rows.getInt("part_index"),
                            rows.getString("file_path"),
                            rows.getLong("rows"),
                            rows.getLong("batches"),
                            rows.getLong("flight_stream_bytes"),
                            rows.getLong("parquet_object_bytes")
                    ));
                }
                return files;
            }
        }
    }

    private Connection connect() throws SQLException {
        JdbcTarget target = jdbcTarget.orElseThrow();
        return DriverManager.getConnection(target.jdbcUrl(), target.properties());
    }

    static JdbcTarget parseJdbcTarget(String raw) {
        if (raw.startsWith("jdbc:")) {
            return new JdbcTarget(raw, new Properties());
        }

        URI uri = URI.create(raw);
        if (!uri.getScheme().equals("postgres") && !uri.getScheme().equals("postgresql")) {
            throw new IllegalArgumentException("metadata database URL must use postgres://, postgresql://, or jdbc:postgresql:");
        }

        StringBuilder jdbcUrl = new StringBuilder("jdbc:postgresql://")
                .append(uri.getHost());
        if (uri.getPort() > 0) {
            jdbcUrl.append(':').append(uri.getPort());
        }
        jdbcUrl.append(uri.getRawPath() == null || uri.getRawPath().isBlank() ? "/" : uri.getRawPath());
        if (uri.getRawQuery() != null && !uri.getRawQuery().isBlank()) {
            jdbcUrl.append('?').append(uri.getRawQuery());
        }

        Properties properties = new Properties();
        if (uri.getRawUserInfo() != null) {
            String[] userInfo = uri.getRawUserInfo().split(":", 2);
            properties.setProperty("user", decode(userInfo[0]));
            if (userInfo.length > 1) {
                properties.setProperty("password", decode(userInfo[1]));
            }
        }
        return new JdbcTarget(jdbcUrl.toString(), properties);
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static void setNullableLong(PreparedStatement statement, int index, Optional<Long> value) throws SQLException {
        if (value.isPresent()) {
            statement.setLong(index, value.get());
        } else {
            statement.setObject(index, null);
        }
    }

    private static Optional<Long> nullableLong(ResultSet rows, String name) throws SQLException {
        long value = rows.getLong(name);
        return rows.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private static Optional<Double> nullableDouble(ResultSet rows, String name) throws SQLException {
        double value = rows.getDouble(name);
        return rows.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private static Optional<Instant> nullableTimestamp(ResultSet rows, String name) throws SQLException {
        Timestamp value = rows.getTimestamp(name);
        return value == null ? Optional.empty() : Optional.of(value.toInstant());
    }

    private Instant workerSelectionEligibleAt(Instant firstSeenAt, Optional<Instant> firstObservedAt) {
        Instant base = firstObservedAt
                .filter(value -> value.isAfter(firstSeenAt))
                .orElse(firstSeenAt);
        return base.plusMillis(config.workerSelectionGraceMs);
    }

    private static void setNullableDouble(PreparedStatement statement, int index, Optional<Double> value) throws SQLException {
        if (value.isPresent()) {
            statement.setDouble(index, value.get());
        } else {
            statement.setObject(index, null);
        }
    }

    private static PGobject jsonb(Optional<Map<String, Object>> value) throws SQLException {
        if (value.isEmpty()) {
            return null;
        }
        return jsonb(value.get());
    }

    private static PGobject jsonb(Map<String, Object> value) throws SQLException {
        PGobject object = new PGobject();
        object.setType("jsonb");
        object.setValue(Json.stringify(value));
        return object;
    }

    private static Optional<Map<String, Object>> nullableJsonObject(ResultSet rows, String name) throws SQLException {
        String value = rows.getString(name);
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(Json.parseObject(value));
    }

    private static QueryRegistryRecord queryRecord(ResultSet rows) throws SQLException {
        return new QueryRegistryRecord(
                rows.getString("query_id"),
                rows.getString("query_type"),
                rows.getString("status"),
                Optional.ofNullable(rows.getString("target_table")),
                Optional.ofNullable(rows.getString("submitted_sql")),
                Optional.ofNullable(rows.getString("trino_user")),
                Optional.ofNullable(rows.getString("trino_query_id")),
                Optional.ofNullable(rows.getString("trino_info_uri")),
                Optional.ofNullable(rows.getString("trino_next_uri")),
                nullableJsonObject(rows, "trino_stats_json"),
                nullableDouble(rows, "progress"),
                Optional.ofNullable(rows.getString("error_message")),
                nullableJsonObject(rows, "result_flight_info_json"),
                nullableJsonObject(rows, "result_tickets_json"),
                nullableJsonObject(rows, "result_files_json"),
                rows.getTimestamp("created_at").toInstant(),
                rows.getTimestamp("updated_at").toInstant(),
                rows.getTimestamp("expires_at").toInstant(),
                Optional.ofNullable(rows.getTimestamp("completed_at")).map(Timestamp::toInstant)
        );
    }
}

record QueryRegistryRecord(
        String queryId,
        String queryType,
        String status,
        Optional<String> targetTable,
        Optional<String> submittedSql,
        Optional<String> trinoUser,
        Optional<String> trinoQueryId,
        Optional<String> trinoInfoUri,
        Optional<String> trinoNextUri,
        Optional<Map<String, Object>> trinoStatsJson,
        Optional<Double> progress,
        Optional<String> errorMessage,
        Optional<Map<String, Object>> resultFlightInfoJson,
        Optional<Map<String, Object>> resultTicketsJson,
        Optional<Map<String, Object>> resultFilesJson,
        Instant createdAt,
        Instant updatedAt,
        Instant expiresAt,
        Optional<Instant> completedAt
) {
    boolean terminal() {
        return status.equals("SUCCEEDED") || status.equals("FAILED") || status.equals("EXPIRED") || status.equals("DROPPED");
    }

    Map<String, Object> statusJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("queryId", queryId);
        body.put("queryType", queryType);
        body.put("status", status);
        targetTable.ifPresent(value -> body.put("targetTable", value));
        submittedSql.ifPresent(value -> body.put("submittedSql", value));
        trinoQueryId.ifPresent(value -> body.put("trinoQueryId", value));
        trinoInfoUri.ifPresent(value -> body.put("trinoInfoUri", value));
        progress.ifPresent(value -> body.put("progress", value));
        errorMessage.ifPresent(value -> body.put("errorMessage", value));
        body.put("createdAtMs", createdAt.toEpochMilli());
        body.put("updatedAtMs", updatedAt.toEpochMilli());
        body.put("expiresAtMs", expiresAt.toEpochMilli());
        completedAt.ifPresent(value -> body.put("completedAtMs", value.toEpochMilli()));
        return body;
    }
}

record PlannedUploadSession(
        String uploadId,
        String operationId,
        Optional<String> tableName,
        String status,
        int expectedStreams,
        String uploadFlavor,
        String stagingPrefix,
        String uploadBucket,
        long targetFileSize,
        Optional<Long> maxStreamBytes,
        Optional<Long> maxRecordBatchBytes,
        String commitMode,
        Instant expiresAt
) {
}

record PlannedUploadStream(
        String streamId,
        String attemptId,
        String workerId,
        String flightUri,
        String descriptorPath,
        Instant reservationExpiresAt,
        Map<String, Object> ticket
) {
}

record PlannedUpload(
        PlannedUploadSession session,
        List<PlannedUploadStream> streams
) {
}

record PutWorkerPlan(
        List<WorkerAssignment> assignments,
        int clusterUtilizationPerMille
) {
}

record UploadSessionRecord(
        String uploadId,
        String operationId,
        Optional<String> tableName,
        String status,
        int expectedStreams,
        String uploadFlavor,
        String stagingPrefix,
        Optional<String> uploadBucket,
        long targetFileSize,
        Optional<Long> maxStreamBytes,
        Optional<Long> maxRecordBatchBytes,
        Optional<String> createTableSql,
        Optional<String> commitMode,
        Optional<String> commitTableName,
        Optional<Long> commitSnapshotId,
        Optional<Map<String, Object>> commitSummary,
        Optional<String> errorMessage,
        Instant createdAt,
        Instant updatedAt,
        Instant expiresAt,
        Optional<Instant> committedAt,
        Optional<Instant> completedAt
) {
    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", uploadId);
        body.put("operationId", operationId);
        tableName.ifPresent(value -> body.put("tableName", value));
        body.put("status", status);
        body.put("expectedStreams", expectedStreams);
        body.put("uploadFlavor", uploadFlavor);
        body.put("stagingPrefix", stagingPrefix);
        uploadBucket.ifPresent(value -> body.put("uploadBucket", value));
        body.put("targetFileSizeBytes", targetFileSize);
        maxStreamBytes.ifPresent(value -> body.put("maxStreamBytes", value));
        maxRecordBatchBytes.ifPresent(value -> body.put("maxRecordBatchBytes", value));
        createTableSql.ifPresent(value -> body.put("createTableSql", value));
        commitMode.ifPresent(value -> body.put("commitMode", value));
        commitTableName.ifPresent(value -> body.put("commitTableName", value));
        commitSnapshotId.ifPresent(value -> body.put("commitSnapshotId", value));
        commitSummary.ifPresent(value -> body.put("commitSummary", value));
        errorMessage.ifPresent(value -> body.put("errorMessage", value));
        body.put("createdAtMs", createdAt.toEpochMilli());
        body.put("updatedAtMs", updatedAt.toEpochMilli());
        body.put("expiresAtMs", expiresAt.toEpochMilli());
        committedAt.ifPresent(value -> body.put("committedAtMs", value.toEpochMilli()));
        completedAt.ifPresent(value -> body.put("completedAtMs", value.toEpochMilli()));
        return body;
    }
}

record CommitMetadata(
        String tableName,
        String mode,
        long snapshotId,
        Optional<String> createTableSql,
        Map<String, Object> summary
) {
}

record UploadStreamState(
        String streamId,
        String attemptId,
        String workerId,
        String flightUri,
        String descriptorPath,
        Instant reservationExpiresAt,
        String status,
        Optional<String> errorMessage,
        long rows,
        long batches,
        int parts,
        long flightStreamBytes,
        Optional<Long> parquetObjectBytes,
        Optional<Long> elapsedMs,
        Optional<String> arrowSchemaJson
) {
    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("streamId", streamId);
        body.put("attemptId", attemptId);
        body.put("workerId", workerId);
        body.put("flightUri", flightUri);
        body.put("descriptorPath", descriptorPath);
        body.put("startBeforeMs", reservationExpiresAt.toEpochMilli());
        body.put("status", status);
        errorMessage.ifPresent(value -> body.put("errorMessage", value));
        body.put("rows", rows);
        body.put("batches", batches);
        body.put("parts", parts);
        body.put("flightStreamBytes", flightStreamBytes);
        parquetObjectBytes.ifPresent(value -> body.put("parquetObjectBytes", value));
        elapsedMs.ifPresent(value -> body.put("elapsedMs", value));
        return body;
    }
}

record UploadFile(
        String streamId,
        String workerId,
        int partIndex,
        String filePath,
        long rows,
        long batches,
        long flightStreamBytes,
        long parquetObjectBytes
) {
    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("streamId", streamId);
        body.put("workerId", workerId);
        body.put("partIndex", partIndex);
        body.put("filePath", filePath);
        body.put("rows", rows);
        body.put("batches", batches);
        body.put("flightStreamBytes", flightStreamBytes);
        body.put("parquetObjectBytes", parquetObjectBytes);
        return body;
    }
}

record UploadSnapshot(
        UploadSessionRecord session,
        List<UploadStreamState> streams,
        List<UploadFile> files
) {
    String canonicalSchemaJsonForFiles() {
        List<String> fileStreamIds = files.stream()
                .map(UploadFile::streamId)
                .distinct()
                .toList();
        List<String> schemas = streams.stream()
                .filter(stream -> fileStreamIds.contains(stream.streamId()))
                .flatMap(stream -> stream.arrowSchemaJson().stream())
                .distinct()
                .toList();
        if (schemas.isEmpty()) {
            throw new CoordinatorException(409, "upload files have no persisted Arrow schema yet");
        }
        if (schemas.size() > 1) {
            throw new CoordinatorException(409, "uploaded files were produced from different Arrow schemas");
        }
        return schemas.getFirst();
    }
}

final class WorkerRegistryEntry {
    private final String workerId;
    private final String flightUri;
    private final int putCapacityStreams;
    private int remainingPutStreams;
    private final int maxPutStreamsPerUpload;
    private int assignedPutStreams;
    private final long selectionScore;
    private final int utilizationPerMille;
    private final long admissionWaitMsEwma;
    private final long throughputBytesPerSecEwma;

    WorkerRegistryEntry(
            String workerId,
            String flightUri,
            int putCapacityStreams,
            int remainingPutStreams,
            int maxPutStreamsPerUpload,
            long selectionScore,
            int utilizationPerMille,
            long admissionWaitMsEwma,
            long throughputBytesPerSecEwma
    ) {
        this.workerId = workerId;
        this.flightUri = flightUri;
        this.putCapacityStreams = putCapacityStreams;
        this.remainingPutStreams = remainingPutStreams;
        this.maxPutStreamsPerUpload = maxPutStreamsPerUpload;
        this.selectionScore = selectionScore;
        this.utilizationPerMille = utilizationPerMille;
        this.admissionWaitMsEwma = admissionWaitMsEwma;
        this.throughputBytesPerSecEwma = throughputBytesPerSecEwma;
    }

    int remainingPutStreams() {
        return remainingPutStreams;
    }

    int putCapacityStreams() {
        return putCapacityStreams;
    }

    boolean canAssignPut() {
        return remainingPutStreams > 0 && assignedPutStreams < maxPutStreamsPerUpload;
    }

    WorkerAssignment assignPut() {
        remainingPutStreams--;
        assignedPutStreams++;
        return new WorkerAssignment(
                workerId,
                flightUri,
                selectionScore,
                utilizationPerMille,
                admissionWaitMsEwma,
                throughputBytesPerSecEwma
        );
    }
}

record WorkerAssignment(
        String workerId,
        String flightUri,
        long selectionScore,
        int utilizationPerMille,
        long admissionWaitMsEwma,
        long throughputBytesPerSecEwma
) {
    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("workerId", workerId);
        body.put("flightUri", flightUri);
        body.put("selectionScore", selectionScore);
        body.put("utilizationPerMille", utilizationPerMille);
        body.put("admissionWaitMsEwma", admissionWaitMsEwma);
        body.put("throughputBytesPerSecEwma", throughputBytesPerSecEwma);
        return body;
    }
}

record WorkerEndpointSnapshot(
        String workerId,
        String flightUri,
        String registryFlightUri,
        Optional<String> clientFlightUri,
        int putRecommendedStreams,
        int readRecommendedStreams,
        int utilizationPerMille,
        Instant firstSeenAt,
        Optional<Instant> firstObservedAt,
        Instant selectionEligibleAt,
        Instant lastHeartbeatAt
) {
    Map<String, Object> toJson() {
        WorkerFlightUri parsed = WorkerFlightUri.parse(flightUri);
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("workerId", workerId);
        body.put("uri", flightUri);
        body.put("flightUri", flightUri);
        body.put("address", parsed.address());
        body.put("scheme", parsed.scheme());
        body.put("host", parsed.host());
        body.put("port", parsed.port());
        body.put("registryFlightUri", registryFlightUri);
        clientFlightUri.ifPresent(value -> body.put("clientFlightUri", value));
        body.put("putRecommendedStreams", putRecommendedStreams);
        body.put("readRecommendedStreams", readRecommendedStreams);
        body.put("utilizationPerMille", utilizationPerMille);
        body.put("firstSeenAtMs", firstSeenAt.toEpochMilli());
        firstObservedAt.ifPresent(value -> body.put("firstObservedAtMs", value.toEpochMilli()));
        body.put("selectionEligibleAtMs", selectionEligibleAt.toEpochMilli());
        body.put("selectionGraceRemainingMs", Math.max(
                0L,
                selectionEligibleAt.toEpochMilli() - Instant.now().toEpochMilli()
        ));
        body.put("lastHeartbeatAtMs", lastHeartbeatAt.toEpochMilli());
        return body;
    }
}

record WorkerClientEndpoint(
        String workerId,
        String flightUri,
        String source,
        Instant expiresAt,
        Optional<String> errorMessage
) {
}

record WorkerRegistryCleanupResult(int markedStale, int deleted) {
}

record JdbcTarget(String jdbcUrl, Properties properties) {
}
