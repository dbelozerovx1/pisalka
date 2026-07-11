package com.arrowflight.coordinator;

import java.util.Locale;

enum UploadFlavor {
    SMALL("small", 1),
    MEDIUM("medium", 4),
    LARGE("large", 8);

    private final String value;
    private final int streamCap;

    UploadFlavor(String value, int streamCap) {
        this.value = value;
        this.streamCap = streamCap;
    }

    static UploadFlavor fromRequest(Object raw) {
        String value = raw == null ? "small" : String.valueOf(raw).trim().toLowerCase(Locale.ROOT);
        return switch (value) {
            case "", "small" -> SMALL;
            case "medium" -> MEDIUM;
            case "large" -> LARGE;
            default -> throw new CoordinatorException(
                    400,
                    "unsupported upload flavor " + value + "; use small, medium, or large"
            );
        };
    }

    int targetStreams(int clusterUtilizationPerMille) {
        if (this == SMALL || clusterUtilizationPerMille >= 900) {
            return 1;
        }
        if (clusterUtilizationPerMille >= 750) {
            return this == MEDIUM ? 2 : 4;
        }
        if (clusterUtilizationPerMille >= 500) {
            return this == MEDIUM ? 3 : 6;
        }
        return streamCap;
    }

    String value() {
        return value;
    }
}
