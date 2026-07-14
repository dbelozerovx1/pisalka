package com.arrowflight.coordinator;

import org.apache.arrow.flight.CallStatus;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

final class CoordinatorErrorFormatter {
    private CoordinatorErrorFormatter() {
    }

    static RuntimeException toFlight(RuntimeException error, ErrorContext context) {
        return toFlight(error, context, null);
    }

    static RuntimeException toFlight(RuntimeException error, ErrorContext context, CoordinatorMetrics metrics) {
        ErrorEnvelope envelope = envelope(error, context);
        if (metrics != null) {
            metrics.recordError(envelope);
        }
        log(envelope, error);
        CallStatus status = callStatus(envelope.flightStatus());
        RuntimeException runtime = status.withDescription(envelope.userDescription()).toRuntimeException();
        if (envelope.includeCause()) {
            runtime = status.withDescription(envelope.userDescription()).withCause(error).toRuntimeException();
        }
        return runtime;
    }

    static ErrorEnvelope envelope(RuntimeException error, ErrorContext context) {
        int status = statusCode(error);
        String code = errorCode(status, error.getMessage());
        String message = userMessage(status, error.getMessage());
        String hint = hint(code, status);
        String errorId = "coord-err-" + UUID.randomUUID().toString().replace("-", "");

        LinkedHashMap<String, String> ids = context.ids();
        StringBuilder description = new StringBuilder();
        description.append("errorId=").append(errorId)
                .append(" code=").append(code)
                .append(" method=").append(context.method());
        context.action().ifPresent(value -> description.append(" action=").append(value));
        for (Map.Entry<String, String> entry : ids.entrySet()) {
            description.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
        }
        description.append(": ").append(message);
        if (!hint.isBlank()) {
            description.append(" hint=").append(hint);
        }

        return new ErrorEnvelope(
                errorId,
                status,
                code,
                message,
                hint,
                description.toString(),
                context,
                status >= 500
        );
    }

    private static int statusCode(RuntimeException error) {
        if (error instanceof CoordinatorException coordinatorError) {
            return coordinatorError.status;
        }
        if (error instanceof IllegalArgumentException) {
            return 400;
        }
        return 500;
    }

    private static CallStatus callStatus(int status) {
        return switch (status) {
            case 400, 409 -> CallStatus.INVALID_ARGUMENT;
            case 401, 403 -> CallStatus.UNAUTHORIZED;
            case 404 -> CallStatus.NOT_FOUND;
            case 502, 503, 504 -> CallStatus.UNAVAILABLE;
            default -> CallStatus.INTERNAL;
        };
    }

    private static String errorCode(int status, String rawMessage) {
        String message = rawMessage == null ? "" : rawMessage.toLowerCase(java.util.Locale.ROOT);
        if (message.contains("schema is not supported") || message.contains("unsupported arrow timestamp")
                || message.contains("unsupported arrow date64")) {
            return "UNSUPPORTED_ARROW_SCHEMA";
        }
        if (message.contains("no live data-plane worker")) {
            return "NO_WORKER_CAPACITY";
        }
        if (message.contains("has no recorded parquet files yet")) {
            return "UPLOAD_HAS_NO_FILES";
        }
        if (message.contains("not complete yet") || message.contains("fewer worker stream rows")) {
            return "UPLOAD_NOT_COMPLETE";
        }
        if (message.contains("produced different arrow schemas")
                || message.contains("produced from different arrow schemas")) {
            return "UPLOAD_SCHEMA_MISMATCH";
        }
        if (message.contains("uploaded schema is not compatible with existing iceberg table")) {
            return "APPEND_SCHEMA_INCOMPATIBLE";
        }
        if (message.contains("no persisted arrow schema")) {
            return "UPLOAD_SCHEMA_NOT_READY";
        }
        if (message.contains("append commit requires an existing iceberg table")) {
            return "APPEND_REQUIRES_EXISTING_TABLE";
        }
        if (message.contains("conflicts with upload planned mode")) {
            return "COMMIT_MODE_CONFLICT";
        }
        if (message.contains("invalid coordinator admin token")) {
            return "INVALID_ADMIN_TOKEN";
        }
        if (message.contains("unknown coordinator action")) {
            return "UNKNOWN_ACTION";
        }
        if (message.contains("trino query failed")) {
            return "TRINO_QUERY_FAILED";
        }
        if (message.contains("trino returned http")) {
            return "TRINO_HTTP_ERROR";
        }
        return switch (status) {
            case 400 -> "INVALID_REQUEST";
            case 401, 403 -> "UNAUTHORIZED";
            case 404 -> "NOT_FOUND";
            case 409 -> "CONFLICT";
            case 502, 503, 504 -> "SERVICE_UNAVAILABLE";
            default -> "INTERNAL_ERROR";
        };
    }

    private static String userMessage(int status, String rawMessage) {
        String message = normalize(rawMessage);
        if (message.isBlank()) {
            message = "Coordinator request failed";
        }
        int cleanupIndex = message.indexOf("; cleanup=");
        if (cleanupIndex >= 0) {
            message = message.substring(0, cleanupIndex);
        }
        if (status >= 500 && !message.contains("Trino query failed") && !message.contains("Trino returned HTTP")) {
            return "Coordinator failed while processing the request";
        }
        return message;
    }

    private static String hint(String code, int status) {
        return switch (code) {
            case "UNSUPPORTED_ARROW_SCHEMA" ->
                    "Use Arrow Timestamp(Microsecond); Timestamp(Second/Millisecond/Nanosecond) and Date64 are not accepted.";
            case "NO_WORKER_CAPACITY" ->
                    "Retry later. If it keeps happening, ask the platform team to check worker heartbeats and capacity.";
            case "UPLOAD_HAS_NO_FILES" ->
                    "Finish at least one DoPut stream for this upload, then retry commit-upload with the same uploadId.";
            case "UPLOAD_NOT_COMPLETE", "UPLOAD_SCHEMA_NOT_READY" ->
                    "Wait until DoPut streams for this upload finish, then retry commit-upload with the same uploadId.";
            case "UPLOAD_SCHEMA_MISMATCH" ->
                    "All streams in one upload must use exactly the same Arrow schema.";
            case "APPEND_SCHEMA_INCOMPATIBLE" ->
                    "Use the same table schema for append, or use overwrite to recreate the table with the uploaded schema.";
            case "APPEND_REQUIRES_EXISTING_TABLE" ->
                    "Use overwrite to recreate the table, or create the Iceberg table before append.";
            case "COMMIT_MODE_CONFLICT" ->
                    "Use the same commit mode that was planned at create-upload.";
            case "INVALID_ADMIN_TOKEN" ->
                    "Pass a valid coordinator admin token.";
            case "UNKNOWN_ACTION" ->
                    "Call listActions or use a coordinator.* action supported by this service.";
            case "TRINO_QUERY_FAILED", "TRINO_HTTP_ERROR" ->
                    "Fix the SQL, permissions, or Trino-side error shown in the message; keep the queryId/errorId for support.";
            case "NOT_FOUND" ->
                    "Check the id and TTL; the upload or query may be expired or already cleaned up.";
            default -> status >= 500
                    ? "Retry the request. If it repeats, contact support with the errorId."
                    : "";
        };
    }

    private static String normalize(String raw) {
        if (raw == null) {
            return "";
        }
        return raw.replace('\n', ' ').replace('\r', ' ').replaceAll("\\s+", " ").trim();
    }

    private static void log(ErrorEnvelope envelope, RuntimeException error) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("errorId", envelope.errorId());
        body.put("status", envelope.flightStatus());
        body.put("code", envelope.code());
        body.put("method", envelope.context().method());
        envelope.context().action().ifPresent(value -> body.put("action", value));
        body.put("outcome", "error");
        body.put("phase", "failed");
        body.put("elapsedMs", envelope.context().elapsedMs());
        body.putAll(envelope.context().ids());
        body.put("message", normalize(error.getMessage()));
        body.put("userMessage", envelope.message());
        CoordinatorLog.error("coordinator_request_failed", body, envelope.includeCause() ? error : null);
    }

    record ErrorContext(
            String method,
            Optional<String> action,
            Map<String, Object> request,
            long elapsedMs
    ) {
        static ErrorContext method(String method, Map<String, Object> request, long elapsedMs) {
            return new ErrorContext(method, Optional.empty(), request, elapsedMs);
        }

        static ErrorContext action(String action, Map<String, Object> request, long elapsedMs) {
            return new ErrorContext("DoAction", Optional.of(action), request, elapsedMs);
        }

        LinkedHashMap<String, String> ids() {
            LinkedHashMap<String, String> out = new LinkedHashMap<>();
            copy(out, "requestId");
            copy(out, "operationId");
            copy(out, "uploadId");
            copy(out, "queryId");
            copy(out, "attemptId");
            copy(out, "streamId");
            copy(out, "workerId");
            copy(out, "tableName");
            copy(out, "targetTable");
            return out;
        }

        private void copy(LinkedHashMap<String, String> out, String key) {
            Object value = request.get(key);
            if (value != null && !String.valueOf(value).isBlank()) {
                out.put(key, String.valueOf(value));
            }
        }
    }

    record ErrorEnvelope(
            String errorId,
            int flightStatus,
            String code,
            String message,
            String hint,
            String userDescription,
            ErrorContext context,
            boolean includeCause
    ) {
    }
}
