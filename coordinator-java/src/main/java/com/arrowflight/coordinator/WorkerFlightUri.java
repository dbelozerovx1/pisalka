package com.arrowflight.coordinator;

import java.net.URI;

final class WorkerFlightUri {
    private static final String DEFAULT_SCHEME = "grpc+tcp";

    private final String raw;
    private final URI parsed;
    private final boolean explicitScheme;

    private WorkerFlightUri(String raw, URI parsed, boolean explicitScheme) {
        this.raw = raw;
        this.parsed = parsed;
        this.explicitScheme = explicitScheme;
    }

    static WorkerFlightUri parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new CoordinatorException(500, "worker Flight URI is empty");
        }
        boolean hasScheme = raw.contains("://");
        try {
            return new WorkerFlightUri(raw, URI.create(hasScheme ? raw : DEFAULT_SCHEME + "://" + raw), hasScheme);
        } catch (IllegalArgumentException error) {
            throw new CoordinatorException(500, "worker Flight URI is invalid: " + raw, error);
        }
    }

    String scheme() {
        String scheme = parsed.getScheme();
        if (scheme == null || scheme.isBlank()) {
            return explicitScheme ? "" : DEFAULT_SCHEME;
        }
        return scheme;
    }

    String host() {
        String host = parsed.getHost();
        if (host == null || host.isBlank()) {
            throw new CoordinatorException(500, "worker Flight URI has no host: " + raw);
        }
        return host;
    }

    int port() {
        int port = parsed.getPort();
        if (port <= 0) {
            throw new CoordinatorException(500, "worker Flight URI has no explicit port: " + raw);
        }
        return port;
    }

    String address() {
        return hostForAddress(host()) + ":" + port();
    }

    String withHost(String host) {
        return scheme() + "://" + hostForAddress(host) + ":" + port();
    }

    private static String hostForAddress(String host) {
        if (host.indexOf(':') >= 0 && !(host.startsWith("[") && host.endsWith("]"))) {
            return "[" + host + "]";
        }
        return host;
    }
}
