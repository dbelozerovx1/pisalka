package com.arrowflight.coordinator;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

final class CapabilitySigner {
    private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();

    private final Config config;

    CapabilitySigner(Config config) {
        this.config = config;
    }

    Map<String, Object> sign(Map<String, Object> payload) {
        String secret = config.capabilitySecret.orElseThrow(() ->
                new IllegalStateException("COORDINATOR_CAPABILITY_SECRET or WORKER_CAPABILITY_SECRET is required to sign worker capabilities")
        );
        String payloadJson = Json.stringify(payload);
        String payloadEncoded = BASE64_URL.encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
        String signature = hmacSha256(secret, payloadEncoded);

        LinkedHashMap<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("version", 1);
        envelope.put("alg", "HS256");
        envelope.put("payload", payloadEncoded);
        envelope.put("signature", signature);
        return envelope;
    }

    Map<String, Object> putPayload(Map<String, Object> request) {
        long now = Instant.now().toEpochMilli();
        String operationId = stringOrDefault(request, "operationId", java.util.UUID.randomUUID().toString());
        String attemptId = stringOrDefault(request, "attemptId", java.util.UUID.randomUUID().toString());
        String uploadId = stringOrDefault(request, "uploadId", operationId);
        String streamId = stringOrDefault(request, "streamId", "stream-0");
        String stagingPrefix = Config.normalizePrefix(Json.requiredString(request, "stagingPrefix"));
        String path = request.containsKey("path")
                ? Config.normalizePath(String.valueOf(request.get("path")))
                : Config.normalizePath(stagingPrefix + "/flight-" + java.util.UUID.randomUUID() + ".parquet");

        if (!path.startsWith(stagingPrefix + "/")) {
            throw new IllegalArgumentException("path must be inside stagingPrefix");
        }

        LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
        payload.put("op", "put");
        payload.put("bucket", Json.requiredString(request, "bucket"));
        payload.put("operation_id", operationId);
        payload.put("attempt_id", attemptId);
        payload.put("upload_id", uploadId);
        payload.put("stream_id", streamId);
        payload.put("worker_id", Json.requiredString(request, "workerId"));
        payload.put("issued_at_ms", now);
        payload.put("expires_at_ms", now + Json.longValue(request, "ttlMs", config.putCapabilityTtlMs));
        payload.put("allowed_output_prefix", stagingPrefix + "/");
        payload.put("staging_prefix", stagingPrefix + "/");
        payload.put("target_file_size", Json.longValue(request, "targetFileSizeBytes", config.defaultTargetFileSizeBytes));
        payload.put("max_stream_bytes", Json.longValue(request, "maxStreamBytes", config.defaultMaxStreamBytes));
        payload.put("max_upload_streams", Json.intValue(request, "maxUploadStreams", config.defaultMaxUploadStreams));
        payload.put("max_record_batch_bytes", Json.longValue(request, "maxRecordBatchBytes", config.defaultPutMaxRecordBatchBytes));

        LinkedHashMap<String, Object> response = new LinkedHashMap<>();
        Map<String, Object> envelope = sign(payload);
        response.put("workerId", payload.get("worker_id"));
        response.put("flightUri", Json.requiredString(request, "flightUri"));
        response.put("bucket", payload.get("bucket"));
        response.put("descriptorPath", path);
        response.put("operationId", operationId);
        response.put("attemptId", attemptId);
        response.put("uploadId", uploadId);
        response.put("streamId", streamId);
        response.put("stagingPrefix", stagingPrefix + "/");
        response.put("expiresAtMs", payload.get("expires_at_ms"));
        response.put("capability", envelope);
        response.put("appMetadata", Json.stringify(envelope));
        return response;
    }

    Map<String, Object> getPayload(Map<String, Object> request) {
        long now = Instant.now().toEpochMilli();
        String operationId = stringOrDefault(request, "operationId", java.util.UUID.randomUUID().toString());
        String path = Config.normalizePath(Json.requiredString(request, "path"));

        LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
        payload.put("op", "get");
        payload.put("bucket", Json.requiredString(request, "bucket"));
        payload.put("operation_id", operationId);
        payload.put("worker_id", Json.requiredString(request, "workerId"));
        payload.put("issued_at_ms", now);
        payload.put("expires_at_ms", now + Json.longValue(request, "ttlMs", config.getCapabilityTtlMs));
        payload.put("path", path);
        payload.put("max_batch_rows", Json.intValue(request, "maxBatchRows", config.defaultGetMaxBatchRows));
        payload.put("max_record_batch_bytes", Json.longValue(request, "maxRecordBatchBytes", config.defaultGetMaxRecordBatchBytes));

        LinkedHashMap<String, Object> response = new LinkedHashMap<>();
        Map<String, Object> envelope = sign(payload);
        response.put("workerId", payload.get("worker_id"));
        response.put("flightUri", Json.requiredString(request, "flightUri"));
        response.put("bucket", payload.get("bucket"));
        response.put("path", path);
        response.put("operationId", operationId);
        response.put("expiresAtMs", payload.get("expires_at_ms"));
        response.put("capability", envelope);
        response.put("ticket", Json.stringify(envelope));
        return response;
    }

    private static String hmacSha256(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return BASE64_URL.encodeToString(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception error) {
            throw new IllegalStateException("failed to sign worker capability", error);
        }
    }

    private static String stringOrDefault(Map<String, Object> request, String key, String defaultValue) {
        String value = Json.string(request, key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
