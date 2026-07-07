package com.arrowflight.coordinator;

final class SqlPlanner {
    private SqlPlanner() {
    }

    static String buildCtas(String targetTable, String sql, String location) {
        String normalizedTarget = validateTableName(targetTable);
        String sourceSql = normalizeSourceSql(sql);
        return "CREATE TABLE "
                + normalizedTarget
                + " WITH (location = '"
                + escapeSqlString(location)
                + "') AS "
                + sourceSql;
    }

    static String buildIcebergFilesQuery(String targetTable) {
        String[] parts = validateTableName(targetTable).split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("targetTable must be catalog.schema.table to query Iceberg files");
        }
        return "SELECT file_path, record_count, file_size_in_bytes FROM "
                + quoteIdentifier(parts[0])
                + "."
                + quoteIdentifier(parts[1])
                + "."
                + quoteIdentifier(parts[2] + "$files");
    }

    static String buildDropTable(String targetTable) {
        String[] parts = validateTableName(targetTable).split("\\.");
        StringBuilder out = new StringBuilder("DROP TABLE IF EXISTS ");
        for (int index = 0; index < parts.length; index++) {
            if (index > 0) {
                out.append(".");
            }
            out.append(quoteIdentifier(parts[index]));
        }
        return out.toString();
    }

    static String buildCreateSchema(String schemaName, String location) {
        return "CREATE SCHEMA IF NOT EXISTS "
                + quoteIdentifier(validateIdentifier(schemaName, "schemaName"))
                + " WITH (location = '"
                + escapeSqlString(location)
                + "')";
    }

    static String normalizeSourceSql(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql is required");
        }
        String trimmed = sql.trim();
        while (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }
        String lower = trimmed.toLowerCase(java.util.Locale.ROOT);
        if (!(lower.startsWith("select ") || lower.startsWith("with "))) {
            throw new IllegalArgumentException("sql must start with SELECT or WITH for CTAS");
        }
        if (trimmed.contains(";")) {
            throw new IllegalArgumentException("sql must contain a single statement");
        }
        return trimmed;
    }

    private static String quoteIdentifier(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String escapeSqlString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("location is required");
        }
        return value.replace("'", "''");
    }

    static String validateTableName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("targetTable is required");
        }
        String trimmed = tableName.trim();
        if (!trimmed.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*){0,2}")) {
            throw new IllegalArgumentException(
                    "targetTable must be an unquoted simple identifier such as catalog.schema.table"
            );
        }
        return trimmed;
    }

    static String validateIdentifier(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        String trimmed = value.trim();
        if (!trimmed.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(name + " must be an unquoted simple identifier");
        }
        return trimmed;
    }
}
