package com.arrowflight.coordinator;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

final class HttpSupport {
    private static final int MAX_REQUEST_BYTES = 1024 * 1024;

    private HttpSupport() {
    }

    static Map<String, Object> readJson(HttpExchange exchange) throws IOException {
        byte[] body = exchange.getRequestBody().readNBytes(MAX_REQUEST_BYTES + 1);
        if (body.length > MAX_REQUEST_BYTES) {
            throw new IllegalArgumentException("request body is too large");
        }
        if (body.length == 0) {
            return Map.of();
        }
        return Json.parseObject(new String(body, StandardCharsets.UTF_8));
    }

    static void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = Json.stringify(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("content-type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    static void sendNoContent(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    static void requireMethod(HttpExchange exchange, String method) {
        if (!exchange.getRequestMethod().equalsIgnoreCase(method)) {
            throw new HttpError(405, "method must be " + method);
        }
    }

    static Optional<String> header(HttpExchange exchange, String name) {
        String value = exchange.getRequestHeaders().getFirst(name);
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value.trim());
    }

    static void requireAdminIfConfigured(HttpExchange exchange, Config config) {
        if (config.adminToken.isEmpty()) {
            return;
        }
        String actual = exchange.getRequestHeaders().getFirst("X-Coordinator-Admin-Token");
        if (!config.adminToken.get().equals(actual)) {
            throw new HttpError(403, "invalid coordinator admin token");
        }
    }

    static Map<String, Object> errorBody(String message) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("error", message);
        return body;
    }

    static final class HttpError extends RuntimeException {
        final int status;

        HttpError(int status, String message) {
            super(message);
            this.status = status;
        }
    }
}
