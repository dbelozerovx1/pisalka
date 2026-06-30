package com.arrowflight.coordinator;

import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

final class CoordinatorMetrics {
    private final long startedAtUnixSeconds = Instant.now().getEpochSecond();
    private final ConcurrentHashMap<RequestLabels, LongAdder> requests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ErrorLabels, LongAdder> errors = new ConcurrentHashMap<>();

    void recordSuccess(String method, String action) {
        requests.computeIfAbsent(new RequestLabels(method, actionLabel(action), "success"), ignored -> new LongAdder())
                .increment();
    }

    void recordError(CoordinatorErrorFormatter.ErrorEnvelope envelope) {
        String method = envelope.context().method();
        String action = actionLabel(envelope.context().action().orElse(null));
        requests.computeIfAbsent(new RequestLabels(method, action, "error"), ignored -> new LongAdder())
                .increment();
        errors.computeIfAbsent(
                new ErrorLabels(method, action, Integer.toString(envelope.flightStatus()), envelope.code()),
                ignored -> new LongAdder()
        ).increment();
    }

    String renderPrometheus() {
        StringBuilder out = new StringBuilder(8192);
        long uptime = Instant.now().getEpochSecond() - startedAtUnixSeconds;
        metricHelp(out, "coordinator_up", "Coordinator process is running.");
        metricType(out, "coordinator_up", "gauge");
        metric(out, "coordinator_up", 1);
        metricHelp(out, "coordinator_uptime_seconds", "Coordinator process uptime in seconds.");
        metricType(out, "coordinator_uptime_seconds", "gauge");
        metric(out, "coordinator_uptime_seconds", uptime);

        metricHelp(out, "coordinator_flight_requests_total", "Coordinator Flight requests by method/action/result.");
        metricType(out, "coordinator_flight_requests_total", "counter");
        requests.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(RequestLabels::key)))
                .forEach(entry -> {
                    RequestLabels labels = entry.getKey();
                    labeledMetric(
                            out,
                            "coordinator_flight_requests_total",
                            labels(
                                    "method", labels.method(),
                                    "action", labels.action(),
                                    "result", labels.result()
                            ),
                            entry.getValue().sum()
                    );
                });

        metricHelp(out, "coordinator_flight_errors_total", "Coordinator Flight errors by method/action/status/code.");
        metricType(out, "coordinator_flight_errors_total", "counter");
        errors.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(ErrorLabels::key)))
                .forEach(entry -> {
                    ErrorLabels labels = entry.getKey();
                    labeledMetric(
                            out,
                            "coordinator_flight_errors_total",
                            labels(
                                    "method", labels.method(),
                                    "action", labels.action(),
                                    "status", labels.status(),
                                    "code", labels.code()
                            ),
                            entry.getValue().sum()
                    );
                });

        return out.toString();
    }

    private static String actionLabel(String action) {
        return action == null || action.isBlank() ? "none" : action;
    }

    private static void metricHelp(StringBuilder out, String name, String help) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
    }

    private static void metricType(StringBuilder out, String name, String type) {
        out.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private static void metric(StringBuilder out, String name, long value) {
        out.append(name).append(' ').append(value).append('\n');
    }

    private static LinkedHashMap<String, String> labels(String... entries) {
        if (entries.length % 2 != 0) {
            throw new IllegalArgumentException("metric label entries must be key/value pairs");
        }
        LinkedHashMap<String, String> labels = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            labels.put(entries[index], entries[index + 1]);
        }
        return labels;
    }

    private static void labeledMetric(
            StringBuilder out,
            String name,
            Map<String, String> labels,
            long value
    ) {
        out.append(name).append('{');
        boolean first = true;
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append(entry.getKey())
                    .append("=\"")
                    .append(escapeLabel(entry.getValue()))
                    .append('"');
        }
        out.append("} ").append(value).append('\n');
    }

    private static String escapeLabel(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    record RequestLabels(String method, String action, String result) {
        String key() {
            return method + '\0' + action + '\0' + result;
        }
    }

    record ErrorLabels(String method, String action, String status, String code) {
        String key() {
            return method + '\0' + action + '\0' + status + '\0' + code;
        }
    }
}
