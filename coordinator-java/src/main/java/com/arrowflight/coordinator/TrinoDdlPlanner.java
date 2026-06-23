package com.arrowflight.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class TrinoDdlPlanner {
    private TrinoDdlPlanner() {
    }

    static String createTableSql(String tableName, Map<String, Object> arrowSchema, String location) {
        List<Object> fields = Json.listValue(arrowSchema, "fields");
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Arrow schema does not contain fields");
        }

        ArrayList<String> columns = new ArrayList<>();
        for (Object value : fields) {
            if (!(value instanceof Map<?, ?> rawField)) {
                throw new IllegalArgumentException("Arrow schema field must be an object");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> field = (Map<String, Object>) rawField;
            String name = Json.requiredString(field, "name");
            Map<String, Object> type = Json.objectValue(field, "type");
            columns.add("    " + quoteIdentifier(name) + " " + trinoType(type));
        }

        return "CREATE TABLE " + normalizeTableName(tableName) + " (\n"
                + String.join(",\n", columns)
                + "\n) WITH (\n"
                + "    format = 'PARQUET',\n"
                + "    location = '" + escapeSqlString(location) + "'\n"
                + ")";
    }

    private static String trinoType(Map<String, Object> type) {
        String kind = Json.requiredString(type, "kind");
        return switch (kind) {
            case "Null" -> "varchar";
            case "Boolean" -> "boolean";
            case "Int8" -> "tinyint";
            case "Int16" -> "smallint";
            case "Int32" -> "integer";
            case "Int64" -> "bigint";
            case "UInt8" -> "smallint";
            case "UInt16" -> "integer";
            case "UInt32" -> "bigint";
            case "UInt64" -> "decimal(20,0)";
            case "Float16", "Float32" -> "real";
            case "Float64" -> "double";
            case "Utf8", "LargeUtf8", "Utf8View" -> "varchar";
            case "Binary", "LargeBinary", "BinaryView", "FixedSizeBinary" -> "varbinary";
            case "Date32" -> "date";
            case "Date64" -> "timestamp(3)";
            case "Timestamp" -> timestampType(type);
            case "Time32", "Time64" -> timeType(type);
            case "Duration", "Interval" -> "varchar";
            case "Decimal32", "Decimal64", "Decimal128", "Decimal256" -> decimalType(type);
            case "Dictionary" -> trinoType(Json.objectValue(type, "value"));
            case "List", "ListView", "LargeList", "LargeListView", "FixedSizeList" ->
                    "array(" + trinoType(Json.objectValue(Json.objectValue(type, "field"), "type")) + ")";
            case "Struct" -> rowType(type);
            case "Map" -> "map(varchar, varchar)";
            case "RunEndEncoded" -> trinoType(Json.objectValue(Json.objectValue(type, "values"), "type"));
            default -> throw new IllegalArgumentException("unsupported Arrow type for Trino DDL: " + kind);
        };
    }

    private static String timestampType(Map<String, Object> type) {
        String precision = precisionForUnit(Json.string(type, "unit"));
        Object timezone = type.get("timezone");
        return timezone == null ? "timestamp(" + precision + ")" : "timestamp(" + precision + ") with time zone";
    }

    private static String timeType(Map<String, Object> type) {
        return "time(" + precisionForUnit(Json.string(type, "unit")) + ")";
    }

    private static String precisionForUnit(String unit) {
        if (unit == null) {
            return "6";
        }
        return switch (unit) {
            case "second" -> "0";
            case "millisecond" -> "3";
            case "microsecond" -> "6";
            case "nanosecond" -> "9";
            default -> "6";
        };
    }

    private static String decimalType(Map<String, Object> type) {
        int precision = Json.intValue(type, "precision", 38);
        int scale = Math.max(0, Json.intValue(type, "scale", 0));
        return "decimal(" + Math.min(precision, 38) + "," + scale + ")";
    }

    private static String rowType(Map<String, Object> type) {
        List<Object> fields = Json.listValue(type, "fields");
        if (fields.isEmpty()) {
            return "row()";
        }
        ArrayList<String> columns = new ArrayList<>();
        for (Object value : fields) {
            if (!(value instanceof Map<?, ?> rawField)) {
                throw new IllegalArgumentException("struct field must be an object");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> field = (Map<String, Object>) rawField;
            columns.add(quoteIdentifier(Json.requiredString(field, "name"))
                    + " "
                    + trinoType(Json.objectValue(field, "type")));
        }
        return "row(" + String.join(", ", columns) + ")";
    }

    private static String normalizeTableName(String raw) {
        String value = raw == null ? "" : raw.trim();
        if (value.isBlank()) {
            throw new IllegalArgumentException("table name must not be empty");
        }
        String[] parts = value.split("\\.");
        ArrayList<String> out = new ArrayList<>();
        for (String part : parts) {
            if (part.isBlank()) {
                throw new IllegalArgumentException("table name contains an empty part");
            }
            out.add(quoteIdentifier(part));
        }
        return String.join(".", out);
    }

    private static String quoteIdentifier(String raw) {
        String value = raw == null ? "" : raw.trim();
        if (value.isBlank()) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.matches("[a-z_][a-z0-9_]*")) {
            return lower;
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String escapeSqlString(String raw) {
        return raw.replace("'", "''");
    }
}
