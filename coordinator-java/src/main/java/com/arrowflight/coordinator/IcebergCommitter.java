package com.arrowflight.coordinator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.CheckCompatibility;
import org.apache.iceberg.types.Types;

final class IcebergCommitter {
    private final Config config;
    private final Configuration hadoopConf;
    private final HiveCatalog catalog;

    IcebergCommitter(Config config) {
        this.config = config;
        this.hadoopConf = hadoopConfiguration(config);
        this.catalog = new HiveCatalog();
        this.catalog.setConf(hadoopConf);
        this.catalog.initialize(config.icebergCatalogName, Map.of(
                CatalogProperties.URI, config.icebergCatalogUri,
                CatalogProperties.WAREHOUSE_LOCATION, config.icebergWarehouse,
                CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO"
        ));
    }

    CommitOutcome commit(String uploadId, String tableName, String mode, List<UploadFile> files) {
        CommitMode commitMode = CommitMode.from(mode);
        if (files.isEmpty()) {
            throw new CoordinatorException(409, "upload has no files to commit");
        }

        Table table = catalog.loadTable(tableIdentifier(tableName));
        ensureNameMapping(table);
        ArrayList<DataFile> dataFiles = new ArrayList<>(files.size());
        ArrayList<String> metricWarnings = new ArrayList<>();
        long recordCount = 0;
        long parquetBytes = 0;
        long metricsCollected = 0;

        for (UploadFile file : files) {
            BuiltDataFile builtFile = dataFile(table, file);
            dataFiles.add(builtFile.dataFile());
            recordCount += file.rows();
            parquetBytes += file.parquetObjectBytes();
            if (builtFile.footerMetricsCollected()) {
                metricsCollected++;
            } else {
                builtFile.metricWarning().ifPresent(metricWarnings::add);
            }
        }

        switch (commitMode) {
            case APPEND -> {
                AppendFiles append = table.newAppend();
                setSnapshotProperties(append, uploadId, commitMode);
                for (DataFile dataFile : dataFiles) {
                    append.appendFile(dataFile);
                }
                append.commit();
            }
            case OVERWRITE -> {
                OverwriteFiles overwrite = table.newOverwrite()
                        .overwriteByRowFilter(Expressions.alwaysTrue());
                setSnapshotProperties(overwrite, uploadId, commitMode);
                for (DataFile dataFile : dataFiles) {
                    overwrite.addFile(dataFile);
                }
                overwrite.commit();
            }
        }

        table.refresh();
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            throw new IllegalStateException("Iceberg commit completed but table has no current snapshot");
        }

        LinkedHashMap<String, Object> summary = new LinkedHashMap<>();
        summary.put("mode", commitMode.value);
        summary.put("fileCount", dataFiles.size());
        summary.put("recordCount", recordCount);
        summary.put("parquetObjectBytes", parquetBytes);
        summary.put("metricsCollectedFiles", metricsCollected);
        summary.put("metricsSkippedFiles", dataFiles.size() - metricsCollected);
        if (!metricWarnings.isEmpty()) {
            summary.put("metricsWarnings", metricWarnings.stream().limit(8).toList());
        }
        summary.put("snapshotId", snapshot.snapshotId());
        if (snapshot.operation() != null) {
            summary.put("snapshotOperation", snapshot.operation());
        }
        if (snapshot.summary() != null) {
            summary.put("snapshotSummary", new LinkedHashMap<>(snapshot.summary()));
        }

        return new CommitOutcome(commitMode.value, snapshot.snapshotId(), recordCount, parquetBytes, summary);
    }

    boolean tableExists(String tableName) {
        return catalog.tableExists(tableIdentifier(tableName));
    }

    Optional<CommitOutcome> committedUpload(String tableName, String uploadId, List<UploadFile> files) {
        TableIdentifier identifier = tableIdentifier(tableName);
        if (!catalog.tableExists(identifier)) {
            return Optional.empty();
        }
        Table table = catalog.loadTable(identifier);
        table.refresh();

        Snapshot matchedSnapshot = null;
        for (Snapshot snapshot : table.snapshots()) {
            Map<String, String> snapshotSummary = snapshot.summary();
            if (snapshotSummary != null && uploadId.equals(snapshotSummary.get("coordinator.upload-id"))) {
                matchedSnapshot = snapshot;
            }
        }
        if (matchedSnapshot == null) {
            return Optional.empty();
        }

        long recordCount = 0;
        long parquetBytes = 0;
        for (UploadFile file : files) {
            recordCount += file.rows();
            parquetBytes += file.parquetObjectBytes();
        }

        Map<String, String> snapshotSummary = matchedSnapshot.summary();
        String mode = Optional.ofNullable(snapshotSummary)
                .map(summary -> summary.get("coordinator.commit-mode"))
                .filter(value -> !value.isBlank())
                .orElse("unknown");
        LinkedHashMap<String, Object> summary = new LinkedHashMap<>();
        summary.put("mode", mode);
        summary.put("fileCount", files.size());
        summary.put("recordCount", recordCount);
        summary.put("parquetObjectBytes", parquetBytes);
        summary.put("snapshotId", matchedSnapshot.snapshotId());
        summary.put("recoveredFromIcebergSnapshot", true);
        if (matchedSnapshot.operation() != null) {
            summary.put("snapshotOperation", matchedSnapshot.operation());
        }
        if (snapshotSummary != null) {
            summary.put("snapshotSummary", new LinkedHashMap<>(snapshotSummary));
        }
        return Optional.of(new CommitOutcome(mode, matchedSnapshot.snapshotId(), recordCount, parquetBytes, summary));
    }

    Map<String, Object> validateAppendSchema(String tableName, Map<String, Object> arrowSchema) {
        TableIdentifier identifier = tableIdentifier(tableName);
        if (!catalog.tableExists(identifier)) {
            throw new CoordinatorException(
                    409,
                    "append commit requires an existing Iceberg table when worker files are written directly "
                            + "under table data location; use overwrite to recreate the table"
            );
        }

        Table table = catalog.loadTable(identifier);
        Schema uploadSchema = IcebergSchemaPlanner.schema(arrowSchema);
        List<String> compatibilityErrors = checkCompatibilityErrors(table.schema(), uploadSchema);
        if (!compatibilityErrors.isEmpty()) {
            throw new CoordinatorException(
                    409,
                    "uploaded schema is not compatible with existing Iceberg table " + tableName
                            + ": " + String.join("; ", compatibilityErrors.stream().limit(8).toList())
            );
        }

        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("tableName", tableName);
        body.put("tableSchemaId", table.schema().schemaId());
        body.put("tableLocation", table.location());
        body.put("compatible", true);
        return body;
    }

    boolean createTableIfMissing(String tableName, Map<String, Object> arrowSchema, String location) {
        TableIdentifier identifier = tableIdentifier(tableName);
        if (catalog.tableExists(identifier)) {
            return false;
        }
        Schema schema = IcebergSchemaPlanner.schema(arrowSchema);
        LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        properties.put(
                TableProperties.DEFAULT_NAME_MAPPING,
                NameMappingParser.toJson(MappingUtil.create(schema))
        );
        try {
            catalog.createTable(identifier, schema, PartitionSpec.unpartitioned(), location, properties);
            return true;
        } catch (AlreadyExistsException ignored) {
            return false;
        }
    }

    private BuiltDataFile dataFile(Table table, UploadFile file) {
        String location = config.objectUriForPrefix(file.filePath());
        InputFile inputFile = HadoopInputFile.fromPath(new Path(location), hadoopConf);
        DataFiles.Builder builder = DataFiles.builder(table.spec())
                .withPath(location)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(file.parquetObjectBytes())
                .withRecordCount(file.rows());
        try {
            Metrics metrics = ParquetUtil.fileMetrics(inputFile, MetricsConfig.forTable(table));
            return new BuiltDataFile(builder.withMetrics(metrics).build(), true, Optional.empty());
        } catch (RuntimeException error) {
            Metrics fallbackMetrics = new Metrics(file.rows(), null, null, null, null);
            String warning = file.filePath() + ": " + error.getMessage();
            return new BuiltDataFile(builder.withMetrics(fallbackMetrics).build(), false, Optional.of(warning));
        }
    }

    private static void ensureNameMapping(Table table) {
        if (table.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
            return;
        }
        table.updateProperties()
                .set(
                        TableProperties.DEFAULT_NAME_MAPPING,
                        NameMappingParser.toJson(MappingUtil.create(table.schema()))
                )
                .commit();
        table.refresh();
    }

    private static void setSnapshotProperties(org.apache.iceberg.SnapshotUpdate<?> update, String uploadId, CommitMode mode) {
        update.set("coordinator.upload-id", uploadId);
        update.set("coordinator.commit-mode", mode.value);
    }

    private static List<String> checkCompatibilityErrors(Schema tableSchema, Schema uploadSchema) {
        ArrayList<String> errors = new ArrayList<>(CheckCompatibility.writeCompatibilityErrors(
                tableSchema,
                uploadSchema,
                true
        ));
        errors.addAll(topLevelSchemaContractErrors(tableSchema, uploadSchema));
        return errors;
    }

    private static List<String> topLevelSchemaContractErrors(Schema tableSchema, Schema uploadSchema) {
        List<Types.NestedField> tableColumns = tableSchema.columns();
        List<Types.NestedField> uploadColumns = uploadSchema.columns();
        ArrayList<String> errors = new ArrayList<>();
        if (tableColumns.size() != uploadColumns.size()) {
            errors.add("table has " + tableColumns.size() + " columns but upload has " + uploadColumns.size());
        }

        int shared = Math.min(tableColumns.size(), uploadColumns.size());
        for (int index = 0; index < shared; index++) {
            Types.NestedField tableField = tableColumns.get(index);
            Types.NestedField uploadField = uploadColumns.get(index);
            if (!tableField.name().equals(uploadField.name())) {
                errors.add(
                        "column " + (index + 1) + " is " + uploadField.name()
                                + " but existing table column is " + tableField.name()
                );
            }
            if (tableField.isOptional() != uploadField.isOptional()) {
                errors.add(
                        "column " + uploadField.name() + " nullability differs; table is "
                                + (tableField.isOptional() ? "nullable" : "required")
                                + " but upload is " + (uploadField.isOptional() ? "nullable" : "required")
                );
            }
        }
        return errors;
    }

    private TableIdentifier tableIdentifier(String tableName) {
        String[] parts = SqlPlanner.validateTableName(tableName).split("\\.");
        String table;
        String[] namespace;
        if (parts.length == 1) {
            table = parts[0];
            namespace = new String[]{config.ctasSchema};
        } else if (parts.length == 2) {
            table = parts[1];
            namespace = new String[]{parts[0]};
        } else if (parts.length == 3) {
            String catalogName = parts[0];
            if (!catalogName.equalsIgnoreCase(config.icebergCatalogName)
                    && !catalogName.equalsIgnoreCase(config.trinoCatalog)) {
                throw new CoordinatorException(
                        400,
                        "table catalog " + catalogName + " does not match coordinator Iceberg catalog "
                                + config.icebergCatalogName
                );
            }
            table = parts[2];
            namespace = new String[]{parts[1]};
        } else {
            throw new CoordinatorException(400, "tableName must be table, schema.table, or catalog.schema.table");
        }
        return TableIdentifier.of(Namespace.of(namespace), table);
    }

    static Configuration hadoopConfiguration(Config config) {
        Configuration configuration = new Configuration();
        configuration.set("hive.metastore.uris", config.icebergCatalogUri);
        configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.s3a.path.style.access", Boolean.toString(config.s3PathStyleAccess));
        configuration.set("fs.s3a.endpoint.region", config.s3Region);
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        config.s3Endpoint.ifPresent(endpoint -> {
            configuration.set("fs.s3a.endpoint", endpoint);
            if (endpoint.startsWith("http://")) {
                configuration.set("fs.s3a.connection.ssl.enabled", "false");
            }
        });
        if (config.s3AllowHttp) {
            configuration.set("fs.s3a.connection.ssl.enabled", "false");
        }
        config.s3AccessKey.ifPresent(value -> configuration.set("fs.s3a.access.key", value));
        config.s3SecretKey.ifPresent(value -> configuration.set("fs.s3a.secret.key", value));
        return configuration;
    }

    enum CommitMode {
        APPEND("append"),
        OVERWRITE("overwrite");

        private final String value;

        CommitMode(String value) {
            this.value = value;
        }

        static CommitMode from(String raw) {
            String value = Optional.ofNullable(raw)
                    .filter(text -> !text.isBlank())
                    .orElse("append")
                    .trim()
                    .toLowerCase(Locale.ROOT);
            return switch (value) {
                case "append" -> APPEND;
                case "overwrite", "replace" -> OVERWRITE;
                default -> throw new CoordinatorException(400, "commit mode must be append or overwrite");
            };
        }
    }
}

record CommitOutcome(
        String mode,
        long snapshotId,
        long recordCount,
        long parquetObjectBytes,
        Map<String, Object> summary
) {
}

record BuiltDataFile(
        DataFile dataFile,
        boolean footerMetricsCollected,
        Optional<String> metricWarning
) {
}
