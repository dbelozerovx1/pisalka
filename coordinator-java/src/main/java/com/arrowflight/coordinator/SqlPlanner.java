package com.arrowflight.coordinator;

final class SqlPlanner {
    private SqlPlanner() {
    }

    static String buildCtas(String targetTable, String sql) {
        String normalizedTarget = validateTableName(targetTable);
        String sourceSql = normalizeSourceSql(sql);
        return "CREATE TABLE " + normalizedTarget + " AS " + sourceSql;
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
}
