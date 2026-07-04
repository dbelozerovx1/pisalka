package com.arrowflight.coordinator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

final class WorkerEndpointRewrite {
    static final WorkerEndpointRewrite NONE = new WorkerEndpointRewrite(Optional.empty());

    private static final Pattern DNS_LABEL = Pattern.compile("[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?");

    private final Optional<String> baseHostname;

    private WorkerEndpointRewrite(Optional<String> baseHostname) {
        this.baseHostname = baseHostname;
    }

    static WorkerEndpointRewrite fromBaseHostname(Optional<String> raw) {
        Optional<String> normalized = raw.map(String::trim).filter(value -> !value.isBlank());
        if (normalized.isEmpty()) {
            return NONE;
        }
        String base = normalized.orElseThrow();
        validateBaseHostname(base);
        return new WorkerEndpointRewrite(Optional.of(base));
    }

    boolean enabled() {
        return baseHostname.isPresent();
    }

    String rewrite(String workerId, String flightUri) {
        if (baseHostname.isEmpty()) {
            return flightUri;
        }
        validateWorkerId(workerId);
        String host = workerId + "." + baseHostname.orElseThrow();
        return WorkerFlightUri.parse(flightUri).withHost(host);
    }

    Map<String, Object> rewriteEndpoint(Map<String, Object> endpoint) {
        if (baseHostname.isEmpty()) {
            return endpoint;
        }
        String workerId = Json.requiredString(endpoint, "workerId");
        String flightUri = Json.requiredString(endpoint, "flightUri");
        LinkedHashMap<String, Object> rewritten = new LinkedHashMap<>(endpoint);
        rewritten.put("flightUri", rewrite(workerId, flightUri));

        Object selectedWorker = rewritten.get("selectedWorker");
        if (selectedWorker instanceof Map<?, ?> rawWorker) {
            @SuppressWarnings("unchecked")
            Map<String, Object> workerMap = (Map<String, Object>) rawWorker;
            LinkedHashMap<String, Object> rewrittenWorker = new LinkedHashMap<>(workerMap);
            String selectedWorkerId = Json.requiredString(rewrittenWorker, "workerId");
            String selectedFlightUri = Json.requiredString(rewrittenWorker, "flightUri");
            rewrittenWorker.put("flightUri", rewrite(selectedWorkerId, selectedFlightUri));
            rewritten.put("selectedWorker", rewrittenWorker);
        }
        return rewritten;
    }

    private static void validateBaseHostname(String value) {
        if (value.length() > 253 || value.contains("://") || value.contains("/") || value.contains(":")) {
            throw new CoordinatorException(
                    400,
                    BaseHostnameMiddleware.HEADER_NAME + " must be a DNS base hostname without scheme, path, or port"
            );
        }
        for (String label : value.split("\\.", -1)) {
            if (!DNS_LABEL.matcher(label).matches()) {
                throw new CoordinatorException(
                        400,
                        BaseHostnameMiddleware.HEADER_NAME + " must be a valid DNS base hostname"
                );
            }
        }
    }

    private static void validateWorkerId(String workerId) {
        if (!DNS_LABEL.matcher(workerId).matches()) {
            throw new CoordinatorException(
                    500,
                    "worker id " + workerId + " cannot be used with "
                            + BaseHostnameMiddleware.HEADER_NAME
                            + "; configure DNS-safe worker ids"
            );
        }
    }
}
