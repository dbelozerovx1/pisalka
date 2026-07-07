package com.arrowflight.coordinator;

record ClientTable(String schema, String table) {
    String name() {
        return schema + "." + table;
    }

    String trinoName(String catalog) {
        return catalog + "." + schema + "." + table;
    }
}

record IcebergTableTarget(
        ClientTable table,
        String namespaceLocation,
        String tableLocation,
        boolean tableExists,
        java.util.Optional<String> existingTableLocation
) {
    String tableName() {
        return table.name();
    }
}
