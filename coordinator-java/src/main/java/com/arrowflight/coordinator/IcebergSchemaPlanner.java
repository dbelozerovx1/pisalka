package com.arrowflight.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

final class IcebergSchemaPlanner {
    private IcebergSchemaPlanner() {
    }

    static Schema schema(Map<String, Object> arrowSchema) {
        List<Object> fields = Json.listValue(arrowSchema, "fields");
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Arrow schema does not contain fields");
        }
        FieldIds ids = new FieldIds();
        ArrayList<Types.NestedField> columns = new ArrayList<>(fields.size());
        for (Object value : fields) {
            columns.add(nestedField(asObject(value, "Arrow schema field"), ids));
        }
        return new Schema(columns);
    }

    private static Types.NestedField nestedField(Map<String, Object> field, FieldIds ids) {
        int id = ids.next();
        String name = Json.requiredString(field, "name");
        Type type = icebergType(Json.objectValue(field, "type"), ids);
        boolean nullable = Json.boolValue(field, "nullable", true);
        return nullable
                ? Types.NestedField.optional(id, name, type)
                : Types.NestedField.required(id, name, type);
    }

    private static Type icebergType(Map<String, Object> type, FieldIds ids) {
        String kind = Json.requiredString(type, "kind");
        return switch (kind) {
            case "Null" -> Types.StringType.get();
            case "Boolean" -> Types.BooleanType.get();
            case "Int8", "Int16", "Int32", "UInt8", "UInt16" -> Types.IntegerType.get();
            case "Int64", "UInt32" -> Types.LongType.get();
            case "UInt64" -> Types.DecimalType.of(20, 0);
            case "Float16", "Float32" -> Types.FloatType.get();
            case "Float64" -> Types.DoubleType.get();
            case "Utf8", "LargeUtf8", "Utf8View" -> Types.StringType.get();
            case "Binary", "LargeBinary", "BinaryView", "FixedSizeBinary" -> Types.BinaryType.get();
            case "Date32" -> Types.DateType.get();
            case "Date64" -> {
                ArrowSchemaConstraints.rejectDate64AsTimestamp();
                yield Types.TimestampType.withoutZone();
            }
            case "Time32", "Time64" -> Types.TimeType.get();
            case "Timestamp" -> timestampType(type);
            case "Decimal32", "Decimal64", "Decimal128", "Decimal256" -> decimalType(type);
            case "Dictionary" -> icebergType(Json.objectValue(type, "value"), ids);
            case "List", "ListView", "LargeList", "LargeListView", "FixedSizeList" -> listType(type, ids);
            case "Struct" -> structType(type, ids);
            case "Map" -> Types.MapType.ofOptional(
                    ids.next(),
                    ids.next(),
                    Types.StringType.get(),
                    Types.StringType.get()
            );
            case "RunEndEncoded" -> icebergType(Json.objectValue(Json.objectValue(type, "values"), "type"), ids);
            default -> throw new IllegalArgumentException("unsupported Arrow type for Iceberg table creation: " + kind);
        };
    }

    private static Type timestampType(Map<String, Object> type) {
        ArrowSchemaConstraints.requireMicrosecondTimestamp(type);
        return type.get("timezone") == null
                ? Types.TimestampType.withoutZone()
                : Types.TimestampType.withZone();
    }

    private static Type decimalType(Map<String, Object> type) {
        int precision = Math.min(Json.intValue(type, "precision", 38), 38);
        int scale = Math.max(0, Json.intValue(type, "scale", 0));
        return Types.DecimalType.of(precision, scale);
    }

    private static Type listType(Map<String, Object> type, FieldIds ids) {
        Map<String, Object> field = Json.objectValue(type, "field");
        Type elementType = icebergType(Json.objectValue(field, "type"), ids);
        int elementId = ids.next();
        return Json.boolValue(field, "nullable", true)
                ? Types.ListType.ofOptional(elementId, elementType)
                : Types.ListType.ofRequired(elementId, elementType);
    }

    private static Type structType(Map<String, Object> type, FieldIds ids) {
        List<Object> fields = Json.listValue(type, "fields");
        ArrayList<Types.NestedField> nestedFields = new ArrayList<>(fields.size());
        for (Object value : fields) {
            nestedFields.add(nestedField(asObject(value, "struct field"), ids));
        }
        return Types.StructType.of(nestedFields);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asObject(Object value, String description) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException(description + " must be an object");
    }

    private static final class FieldIds {
        private int next = 1;

        int next() {
            return next++;
        }
    }
}
