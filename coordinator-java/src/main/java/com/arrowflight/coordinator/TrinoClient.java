package com.arrowflight.coordinator;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

final class TrinoClient {
    private final Config config;
    private final HttpClient httpClient;

    TrinoClient(Config config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(config.trinoRequestTimeout)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    QueryHandle submitStatement(String sql, String trinoUser, Optional<String> authorization)
            throws IOException, InterruptedException {
        return submitStatement(sql, trinoUser, authorization, Optional.empty());
    }

    QueryHandle submitStatement(
            String sql,
            String trinoUser,
            Optional<String> authorization,
            Optional<String> schema
    ) throws IOException, InterruptedException {
        HttpRequest request = baseRequest(config.trinoUri.resolve("/v1/statement"), trinoUser, authorization, schema)
                .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
                .header("content-type", "text/plain; charset=utf-8")
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return queryHandle(parseTrinoResponse(response));
    }

    QueryHandle runStatement(String sql, String trinoUser, Optional<String> authorization)
            throws IOException, InterruptedException {
        return runStatement(sql, trinoUser, authorization, Optional.empty());
    }

    QueryHandle runStatement(
            String sql,
            String trinoUser,
            Optional<String> authorization,
            Optional<String> schema
    ) throws IOException, InterruptedException {
        QueryHandle handle = submitStatement(sql, trinoUser, authorization, schema);
        while (handle.nextUri().isPresent()) {
            handle = pollNext(handle.nextUri().orElseThrow(), trinoUser, authorization);
        }
        return handle;
    }

    QueryHandle pollNext(String nextUri, String trinoUser, Optional<String> authorization)
            throws IOException, InterruptedException {
        HttpRequest request = baseRequest(URI.create(nextUri), trinoUser, authorization, Optional.empty())
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return queryHandle(parseTrinoResponse(response));
    }

    private HttpRequest.Builder baseRequest(
            URI uri,
            String trinoUser,
            Optional<String> authorization,
            Optional<String> schema
    ) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(config.trinoRequestTimeout)
                .header("X-Trino-User", trinoUser)
                .header("X-Trino-Catalog", config.trinoCatalog)
                .header("X-Trino-Schema", schema.filter(value -> !value.isBlank()).orElse(config.trinoSchema))
                .header("X-Trino-Source", config.trinoSource);
        authorization.ifPresent(value -> builder.header("Authorization", value));
        return builder;
    }

    private static Map<String, Object> parseTrinoResponse(HttpResponse<String> response) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("Trino returned HTTP " + response.statusCode() + ": " + response.body());
        }
        Map<String, Object> body = Json.parseObject(response.body());
        Map<String, Object> error = Json.objectValue(body, "error");
        if (!error.isEmpty()) {
            String message = Json.string(error, "message");
            String errorName = Json.string(error, "errorName");
            throw new IllegalStateException("Trino query failed: " + errorName + ": " + message);
        }
        return body;
    }

    private static QueryHandle queryHandle(Map<String, Object> response) {
        String nextUri = Json.string(response, "nextUri");
        return new QueryHandle(
                Optional.ofNullable(Json.string(response, "id")).filter(value -> !value.isBlank()),
                Optional.ofNullable(Json.string(response, "infoUri")).filter(value -> !value.isBlank()),
                Optional.ofNullable(nextUri).filter(value -> !value.isBlank()),
                optionalObject(response, "stats"),
                response
        );
    }

    private static Optional<Map<String, Object>> optionalObject(Map<String, Object> response, String key) {
        Map<String, Object> value = Json.objectValue(response, key);
        return value.isEmpty() ? Optional.empty() : Optional.of(value);
    }

    record QueryHandle(
            Optional<String> queryId,
            Optional<String> infoUri,
            Optional<String> nextUri,
            Optional<Map<String, Object>> stats,
            Map<String, Object> response
    ) {
    }
}
