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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

final class CoordinatorMetadataStore {
    private final Optional<JdbcTarget> jdbcTarget;

    CoordinatorMetadataStore(Config config) {
        this.jdbcTarget = config.metadataDatabaseUrl.map(CoordinatorMetadataStore::parseJdbcTarget);
    }

    boolean enabled() {
        return jdbcTarget.isPresent();
    }

    void requireEnabled() {
        if (jdbcTarget.isEmpty()) {
            throw new HttpSupport.HttpError(503, "coordinator metadata database is not configured");
        }
    }

    void createUpload(PlannedUploadSession session, List<PlannedUploadStream> streams) {
        requireEnabled();
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO coordinator_upload_sessions (
                            upload_id,
                            operation_id,
                            table_name,
                            status,
                            expected_streams,
                            staging_prefix,
                            target_file_size,
                            max_stream_bytes,
                            max_record_batch_bytes,
                            expires_at,
                            updated_at
                        ) VALUES (?, ?, ?, 'PLANNED', ?, ?, ?, ?, ?, ?, now())
                        """)) {
                    statement.setString(1, session.uploadId());
                    statement.setString(2, session.operationId());
                    statement.setString(3, session.tableName().orElse(null));
                    statement.setInt(4, session.expectedStreams());
                    statement.setString(5, session.stagingPrefix());
                    statement.setLong(6, session.targetFileSize());
                    setNullableLong(statement, 7, session.maxStreamBytes());
                    setNullableLong(statement, 8, session.maxRecordBatchBytes());
                    statement.setTimestamp(9, Timestamp.from(session.expiresAt()));
                    statement.executeUpdate();
                }

                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO coordinator_upload_streams (
                            upload_id,
                            stream_id,
                            attempt_id,
                            worker_id,
                            flight_uri,
                            descriptor_path,
                            status,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 'PLANNED', now())
                        """)) {
                    for (PlannedUploadStream stream : streams) {
                        statement.setString(1, session.uploadId());
                        statement.setString(2, stream.streamId());
                        statement.setString(3, stream.attemptId());
                        statement.setString(4, stream.workerId());
                        statement.setString(5, stream.flightUri());
                        statement.setString(6, stream.descriptorPath());
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }

                connection.commit();
            } catch (SQLException error) {
                connection.rollback();
                throw error;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("failed to create upload session metadata", error);
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

    void markRunning(String uploadId) {
        updateStatus(uploadId, "RUNNING", Optional.empty(), Optional.empty(), false);
    }

    void markReadyToCommit(String uploadId, String createTableSql) {
        updateStatus(uploadId, "READY_TO_COMMIT", Optional.of(createTableSql), Optional.empty(), true);
    }

    void markCommitted(String uploadId) {
        updateStatus(uploadId, "COMMITTED", Optional.empty(), Optional.empty(), true);
    }

    void markFailed(String uploadId, String errorMessage) {
        updateStatus(uploadId, "FAILED", Optional.empty(), Optional.of(errorMessage), true);
    }

    void markAborted(String uploadId, String errorMessage) {
        updateStatus(uploadId, "ABORTED", Optional.empty(), Optional.of(errorMessage), true);
    }

    private void updateStatus(
            String uploadId,
            String status,
            Optional<String> createTableSql,
            Optional<String> errorMessage,
            boolean completed
    ) {
        requireEnabled();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE coordinator_upload_sessions
                     SET status = ?,
                         create_table_sql = COALESCE(?, create_table_sql),
                         error_message = ?,
                         updated_at = now(),
                         completed_at = CASE WHEN ? THEN COALESCE(completed_at, now()) ELSE completed_at END
                     WHERE upload_id = ?
                     """)) {
            statement.setString(1, status);
            statement.setString(2, createTableSql.orElse(null));
            statement.setString(3, errorMessage.orElse(null));
            statement.setBoolean(4, completed);
            statement.setString(5, uploadId);
            if (statement.executeUpdate() == 0) {
                throw new HttpSupport.HttpError(404, "upload session was not found");
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
                       staging_prefix,
                       target_file_size,
                       max_stream_bytes,
                       max_record_batch_bytes,
                       create_table_sql,
                       error_message,
                       created_at,
                       updated_at,
                       expires_at,
                       completed_at
                FROM coordinator_upload_sessions
                WHERE upload_id = ?
                """)) {
            statement.setString(1, uploadId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new HttpSupport.HttpError(404, "upload session was not found");
                }
                return new UploadSessionRecord(
                        rows.getString("upload_id"),
                        rows.getString("operation_id"),
                        Optional.ofNullable(rows.getString("table_name")),
                        rows.getString("status"),
                        rows.getInt("expected_streams"),
                        rows.getString("staging_prefix"),
                        rows.getLong("target_file_size"),
                        nullableLong(rows, "max_stream_bytes"),
                        nullableLong(rows, "max_record_batch_bytes"),
                        Optional.ofNullable(rows.getString("create_table_sql")),
                        Optional.ofNullable(rows.getString("error_message")),
                        rows.getTimestamp("created_at").toInstant(),
                        rows.getTimestamp("updated_at").toInstant(),
                        rows.getTimestamp("expires_at").toInstant(),
                        Optional.ofNullable(rows.getTimestamp("completed_at")).map(Timestamp::toInstant)
                );
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
                SELECT files.stream_id,
                       files.worker_id,
                       files.logical_key,
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
                ORDER BY files.stream_id, files.part_index
                """)) {
            statement.setString(1, uploadId);
            try (ResultSet rows = statement.executeQuery()) {
                ArrayList<UploadFile> files = new ArrayList<>();
                while (rows.next()) {
                    files.add(new UploadFile(
                            rows.getString("stream_id"),
                            rows.getString("worker_id"),
                            rows.getString("logical_key"),
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

    private static JdbcTarget parseJdbcTarget(String raw) {
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
}

record PlannedUploadSession(
        String uploadId,
        String operationId,
        Optional<String> tableName,
        int expectedStreams,
        String stagingPrefix,
        long targetFileSize,
        Optional<Long> maxStreamBytes,
        Optional<Long> maxRecordBatchBytes,
        Instant expiresAt
) {
}

record PlannedUploadStream(
        String streamId,
        String attemptId,
        String workerId,
        String flightUri,
        String descriptorPath,
        Map<String, Object> ticket
) {
}

record UploadSessionRecord(
        String uploadId,
        String operationId,
        Optional<String> tableName,
        String status,
        int expectedStreams,
        String stagingPrefix,
        long targetFileSize,
        Optional<Long> maxStreamBytes,
        Optional<Long> maxRecordBatchBytes,
        Optional<String> createTableSql,
        Optional<String> errorMessage,
        Instant createdAt,
        Instant updatedAt,
        Instant expiresAt,
        Optional<Instant> completedAt
) {
    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("uploadId", uploadId);
        body.put("operationId", operationId);
        tableName.ifPresent(value -> body.put("tableName", value));
        body.put("status", status);
        body.put("expectedStreams", expectedStreams);
        body.put("stagingPrefix", stagingPrefix);
        body.put("targetFileSizeBytes", targetFileSize);
        maxStreamBytes.ifPresent(value -> body.put("maxStreamBytes", value));
        maxRecordBatchBytes.ifPresent(value -> body.put("maxRecordBatchBytes", value));
        createTableSql.ifPresent(value -> body.put("createTableSql", value));
        errorMessage.ifPresent(value -> body.put("errorMessage", value));
        body.put("createdAtMs", createdAt.toEpochMilli());
        body.put("updatedAtMs", updatedAt.toEpochMilli());
        body.put("expiresAtMs", expiresAt.toEpochMilli());
        completedAt.ifPresent(value -> body.put("completedAtMs", value.toEpochMilli()));
        return body;
    }
}

record UploadStreamState(
        String streamId,
        String attemptId,
        String workerId,
        String flightUri,
        String descriptorPath,
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
    boolean succeeded() {
        return status.equals("SUCCEEDED");
    }

    boolean failed() {
        return status.equals("FAILED") || status.equals("REJECTED");
    }

    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("streamId", streamId);
        body.put("attemptId", attemptId);
        body.put("workerId", workerId);
        body.put("flightUri", flightUri);
        body.put("descriptorPath", descriptorPath);
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
        String logicalKey,
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
        body.put("logicalKey", logicalKey);
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
    boolean allStreamsSucceeded() {
        return streams.size() == session.expectedStreams() && streams.stream().allMatch(UploadStreamState::succeeded);
    }

    Optional<UploadStreamState> firstFailedStream() {
        return streams.stream().filter(UploadStreamState::failed).findFirst();
    }

    Optional<UploadStreamState> firstIncompleteStream() {
        return streams.stream().filter(stream -> !stream.succeeded()).findFirst();
    }

    String canonicalSchemaJson() {
        List<String> schemas = streams.stream()
                .flatMap(stream -> stream.arrowSchemaJson().stream())
                .distinct()
                .toList();
        if (schemas.isEmpty()) {
            throw new HttpSupport.HttpError(409, "upload has no persisted Arrow schema yet");
        }
        if (schemas.size() > 1) {
            throw new HttpSupport.HttpError(409, "upload streams produced different Arrow schemas");
        }
        return schemas.getFirst();
    }

    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("session", session.toJson());
        body.put("streams", streams.stream().map(UploadStreamState::toJson).toList());
        body.put("files", files.stream().map(UploadFile::toJson).toList());
        body.put("allStreamsSucceeded", allStreamsSucceeded());
        return body;
    }
}

record JdbcTarget(String jdbcUrl, Properties properties) {
}
