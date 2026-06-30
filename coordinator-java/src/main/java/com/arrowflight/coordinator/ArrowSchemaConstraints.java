package com.arrowflight.coordinator;

import java.util.Map;

final class ArrowSchemaConstraints {
    private ArrowSchemaConstraints() {
    }

    static void requireMicrosecondTimestamp(Map<String, Object> type) {
        String unit = Json.string(type, "unit");
        if (!"microsecond".equals(unit)) {
            String rendered = unit == null || unit.isBlank() ? "<missing>" : unit;
            throw new IllegalArgumentException(
                    "unsupported Arrow Timestamp unit " + rendered
                            + "; only microsecond timestamps are supported for Iceberg v2 compatibility"
            );
        }
    }

    static void rejectDate64AsTimestamp() {
        throw new IllegalArgumentException(
                "unsupported Arrow Date64; it is millisecond-backed and would be planned as a timestamp. "
                        + "Use Date32 or Timestamp(Microsecond) for Iceberg v2 compatibility"
        );
    }
}
