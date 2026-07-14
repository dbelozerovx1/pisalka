package com.arrowflight.coordinator;

import java.util.LinkedHashMap;
import java.util.Map;

final class CoordinatorRequestLog {
    private static final String[] ID_KEYS = {
            "requestId",
            "operationId",
            "uploadId",
            "queryId",
            "attemptId",
            "streamId",
            "workerId",
            "tableName",
            "targetTable"
    };

    private CoordinatorRequestLog() {
    }

    static void started(String method, String action, Map<String, Object> request) {
        LinkedHashMap<String, Object> body = base(method, action);
        body.put("phase", "started");
        copyIds(body, request);
        CoordinatorLog.info("coordinator_request_started", body);
    }

    static void success(
            String method,
            String action,
            Map<String, Object> request,
            Map<String, Object> response,
            long elapsedMs
    ) {
        LinkedHashMap<String, Object> body = base(method, action);
        body.put("outcome", "success");
        body.put("phase", "completed");
        body.put("elapsedMs", elapsedMs);
        copyIds(body, request);
        copyIds(body, response);
        copyOptional(body, "status", response);
        copyOptional(body, "mode", response);
        copyOptional(body, "requestedFlavor", response);
        copyOptional(body, "grantedStreams", response);
        copyOptional(body, "endpointCount", response);
        CoordinatorLog.info("coordinator_request_completed", body);
    }

    private static LinkedHashMap<String, Object> base(String method, String action) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("method", method);
        if (action != null && !action.isBlank()) {
            body.put("action", action);
        }
        return body;
    }

    static void copyIds(LinkedHashMap<String, Object> target, Map<String, Object> source) {
        for (String key : ID_KEYS) {
            copyOptional(target, key, source);
        }
    }

    private static void copyOptional(LinkedHashMap<String, Object> target, String key, Map<String, Object> source) {
        Object value = source.get(key);
        if (value != null && !String.valueOf(value).isBlank()) {
            target.put(key, value);
        }
    }
}
