package com.arrowflight.coordinator;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

final class Config {
    final InetSocketAddress listenAddress;
    final boolean coordinatorMetricsEnabled;
    final InetSocketAddress coordinatorMetricsAddress;
    final URI trinoUri;
    final String trinoCatalog;
    final String trinoSchema;
    final String trinoSource;
    final String ctasCatalog;
    final String ctasSchema;
    final String ctasTablePrefix;
    final String ctasLocationPrefix;
    final String defaultSchemaLocationPrefix;
    final String icebergCatalogName;
    final String icebergCatalogUri;
    final String icebergWarehouse;
    final boolean icebergHiveLockEnabled;
    final Optional<String> s3Endpoint;
    final String s3Region;
    final Optional<String> s3AccessKey;
    final Optional<String> s3SecretKey;
    final boolean s3PathStyleAccess;
    final boolean s3AllowHttp;
    final Optional<String> capabilitySecret;
    final Optional<String> adminToken;
    final Optional<String> metadataDatabaseUrl;
    final boolean metadataMigrationsEnabled;
    final boolean metadataMigrationsBaselineOnMigrate;
    final long capabilityTtlMs;
    final long putCapabilityTtlMs;
    final long getCapabilityTtlMs;
    final long uploadSessionTtlMs;
    final long queryRegistryTtlMs;
    final long queryRegistryCleanupIntervalMs;
    final String objectStoreUriPrefix;
    final int defaultUploadStreams;
    final long defaultTargetFileSizeBytes;
    final long defaultMaxStreamBytes;
    final int defaultMaxUploadStreams;
    final long defaultPutMaxRecordBatchBytes;
    final int defaultGetMaxBatchRows;
    final long defaultGetMaxRecordBatchBytes;
    final int flightMaxMessageSize;
    final Duration trinoRequestTimeout;
    final boolean workerClientEndpointsRequired;
    final long workerClientEndpointTtlMs;
    final long workerSelectionGraceMs;
    final boolean k8sWorkerDiscoveryEnabled;
    final String k8sNamespace;
    final String k8sWorkerServiceSelector;
    final String k8sWorkerIdLabel;
    final String k8sWorkerFlightPortName;
    final String workerClientUriScheme;
    final long k8sInformerResyncMs;
    final long k8sInformerInitialSyncTimeoutMs;

    private Config(
            InetSocketAddress listenAddress,
            boolean coordinatorMetricsEnabled,
            InetSocketAddress coordinatorMetricsAddress,
            URI trinoUri,
            String trinoCatalog,
            String trinoSchema,
            String trinoSource,
            String ctasCatalog,
            String ctasSchema,
            String ctasTablePrefix,
            String ctasLocationPrefix,
            String defaultSchemaLocationPrefix,
            String icebergCatalogName,
            String icebergCatalogUri,
            String icebergWarehouse,
            boolean icebergHiveLockEnabled,
            Optional<String> s3Endpoint,
            String s3Region,
            Optional<String> s3AccessKey,
            Optional<String> s3SecretKey,
            boolean s3PathStyleAccess,
            boolean s3AllowHttp,
            Optional<String> capabilitySecret,
            Optional<String> adminToken,
            Optional<String> metadataDatabaseUrl,
            boolean metadataMigrationsEnabled,
            boolean metadataMigrationsBaselineOnMigrate,
            long capabilityTtlMs,
            long putCapabilityTtlMs,
            long getCapabilityTtlMs,
            long uploadSessionTtlMs,
            long queryRegistryTtlMs,
            long queryRegistryCleanupIntervalMs,
            String objectStoreUriPrefix,
            int defaultUploadStreams,
            long defaultTargetFileSizeBytes,
            long defaultMaxStreamBytes,
            int defaultMaxUploadStreams,
            long defaultPutMaxRecordBatchBytes,
            int defaultGetMaxBatchRows,
            long defaultGetMaxRecordBatchBytes,
            int flightMaxMessageSize,
            Duration trinoRequestTimeout,
            boolean workerClientEndpointsRequired,
            long workerClientEndpointTtlMs,
            long workerSelectionGraceMs,
            boolean k8sWorkerDiscoveryEnabled,
            String k8sNamespace,
            String k8sWorkerServiceSelector,
            String k8sWorkerIdLabel,
            String k8sWorkerFlightPortName,
            String workerClientUriScheme,
            long k8sInformerResyncMs,
            long k8sInformerInitialSyncTimeoutMs
    ) {
        this.listenAddress = listenAddress;
        this.coordinatorMetricsEnabled = coordinatorMetricsEnabled;
        this.coordinatorMetricsAddress = coordinatorMetricsAddress;
        this.trinoUri = trinoUri;
        this.trinoCatalog = trinoCatalog;
        this.trinoSchema = trinoSchema;
        this.trinoSource = trinoSource;
        this.ctasCatalog = ctasCatalog;
        this.ctasSchema = ctasSchema;
        this.ctasTablePrefix = ctasTablePrefix;
        this.ctasLocationPrefix = normalizeObjectStoreUriPrefix(ctasLocationPrefix);
        this.defaultSchemaLocationPrefix = normalizeObjectStoreUriPrefix(defaultSchemaLocationPrefix);
        this.icebergCatalogName = icebergCatalogName;
        this.icebergCatalogUri = icebergCatalogUri;
        this.icebergWarehouse = normalizeObjectStoreUriPrefix(icebergWarehouse);
        this.icebergHiveLockEnabled = icebergHiveLockEnabled;
        this.s3Endpoint = s3Endpoint;
        this.s3Region = s3Region;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.s3PathStyleAccess = s3PathStyleAccess;
        this.s3AllowHttp = s3AllowHttp;
        this.capabilitySecret = capabilitySecret;
        this.adminToken = adminToken;
        this.metadataDatabaseUrl = metadataDatabaseUrl;
        this.metadataMigrationsEnabled = metadataMigrationsEnabled;
        this.metadataMigrationsBaselineOnMigrate = metadataMigrationsBaselineOnMigrate;
        this.capabilityTtlMs = capabilityTtlMs;
        this.putCapabilityTtlMs = putCapabilityTtlMs;
        this.getCapabilityTtlMs = getCapabilityTtlMs;
        this.uploadSessionTtlMs = uploadSessionTtlMs;
        this.queryRegistryTtlMs = queryRegistryTtlMs;
        this.queryRegistryCleanupIntervalMs = queryRegistryCleanupIntervalMs;
        this.objectStoreUriPrefix = normalizeObjectStoreUriPrefix(objectStoreUriPrefix);
        this.defaultUploadStreams = defaultUploadStreams;
        this.defaultTargetFileSizeBytes = defaultTargetFileSizeBytes;
        this.defaultMaxStreamBytes = defaultMaxStreamBytes;
        this.defaultMaxUploadStreams = defaultMaxUploadStreams;
        this.defaultPutMaxRecordBatchBytes = defaultPutMaxRecordBatchBytes;
        this.defaultGetMaxBatchRows = defaultGetMaxBatchRows;
        this.defaultGetMaxRecordBatchBytes = defaultGetMaxRecordBatchBytes;
        this.flightMaxMessageSize = flightMaxMessageSize;
        this.trinoRequestTimeout = trinoRequestTimeout;
        this.workerClientEndpointsRequired = workerClientEndpointsRequired;
        this.workerClientEndpointTtlMs = workerClientEndpointTtlMs;
        this.workerSelectionGraceMs = workerSelectionGraceMs;
        this.k8sWorkerDiscoveryEnabled = k8sWorkerDiscoveryEnabled;
        this.k8sNamespace = k8sNamespace;
        this.k8sWorkerServiceSelector = k8sWorkerServiceSelector;
        this.k8sWorkerIdLabel = k8sWorkerIdLabel;
        this.k8sWorkerFlightPortName = k8sWorkerFlightPortName;
        this.workerClientUriScheme = workerClientUriScheme;
        this.k8sInformerResyncMs = k8sInformerResyncMs;
        this.k8sInformerInitialSyncTimeoutMs = k8sInformerInitialSyncTimeoutMs;
    }

    static Config fromEnv() {
        long capabilityTtlMs = envLong("COORDINATOR_CAPABILITY_TTL_MS", 15 * 60 * 1000L);
        boolean k8sWorkerDiscoveryEnabled = envBool("COORDINATOR_K8S_WORKER_DISCOVERY_ENABLED", false);
        String objectStoreUriPrefix = env("COORDINATOR_OBJECT_STORE_URI_PREFIX", "s3://arrow-flight");
        String trinoCatalog = env("TRINO_CATALOG", "iceberg");
        String trinoSchema = env("TRINO_SCHEMA", "arrow");
        String icebergWarehouse = env("ICEBERG_WAREHOUSE", objectStoreUriPrefix + "/iceberg");
        String ctasSchema = env("CTAS_TEMP_SCHEMA", env("CTAS_DEFAULT_SCHEMA", trinoSchema));
        return new Config(
                parseListenAddress(env("COORDINATOR_ADDR", "0.0.0.0:8088"), "COORDINATOR_ADDR"),
                envBool("COORDINATOR_METRICS_ENABLED", true),
                parseListenAddress(env("COORDINATOR_METRICS_ADDR", "0.0.0.0:9091"), "COORDINATOR_METRICS_ADDR"),
                URI.create(env("TRINO_URI", "http://host.docker.internal:8080")),
                trinoCatalog,
                trinoSchema,
                env("TRINO_SOURCE", "arrow-flight-coordinator"),
                trinoCatalog,
                ctasSchema,
                env("CTAS_TABLE_PREFIX", "ctas_tmp"),
                env("CTAS_TEMP_LOCATION_PREFIX", objectStoreUriPrefix + "/coordinator/ctas"),
                env("COORDINATOR_DEFAULT_SCHEMA_LOCATION_PREFIX", icebergWarehouse),
                env("ICEBERG_CATALOG_NAME", trinoCatalog),
                env("ICEBERG_CATALOG_URI", "thrift://host.docker.internal:9083"),
                icebergWarehouse,
                envBool("ICEBERG_HIVE_LOCK_ENABLED", false),
                envOptional("S3_ENDPOINT"),
                env("AWS_REGION", env("S3_REGION", "us-east-1")),
                envOptional("AWS_ACCESS_KEY_ID"),
                envOptional("AWS_SECRET_ACCESS_KEY"),
                envBool("S3_PATH_STYLE_ACCESS", true),
                envBool("AWS_ALLOW_HTTP", false),
                envOptional("COORDINATOR_CAPABILITY_SECRET").or(() -> envOptional("WORKER_CAPABILITY_SECRET")),
                envOptional("COORDINATOR_ADMIN_TOKEN"),
                envOptional("COORDINATOR_METADATA_DATABASE_URL").or(() -> envOptional("METADATA_DATABASE_URL")),
                envBool("COORDINATOR_METADATA_MIGRATIONS_ENABLED", true),
                envBool("COORDINATOR_METADATA_MIGRATIONS_BASELINE_ON_MIGRATE", true),
                capabilityTtlMs,
                envLong("COORDINATOR_PUT_CAPABILITY_TTL_MS", capabilityTtlMs),
                envLong("COORDINATOR_GET_CAPABILITY_TTL_MS", 5 * 60 * 1000L),
                envLong("COORDINATOR_UPLOAD_SESSION_TTL_MS", 60 * 60 * 1000L),
                envLong("COORDINATOR_QUERY_REGISTRY_TTL_MS", 60 * 60 * 1000L),
                envLong("COORDINATOR_QUERY_REGISTRY_CLEANUP_INTERVAL_MS", 5 * 60 * 1000L),
                objectStoreUriPrefix,
                envInt("COORDINATOR_DEFAULT_UPLOAD_STREAMS", 1),
                envLong("COORDINATOR_DEFAULT_TARGET_FILE_SIZE_BYTES", 512L * 1024 * 1024),
                envLong("COORDINATOR_DEFAULT_MAX_STREAM_BYTES", 10L * 1024 * 1024 * 1024),
                envInt("COORDINATOR_DEFAULT_MAX_UPLOAD_STREAMS", 4),
                envLong("COORDINATOR_DEFAULT_PUT_MAX_RECORD_BATCH_BYTES", 256L * 1024 * 1024),
                envInt("COORDINATOR_DEFAULT_GET_MAX_BATCH_ROWS", 65_536),
                envLong("COORDINATOR_DEFAULT_GET_MAX_RECORD_BATCH_BYTES", 128L * 1024 * 1024),
                envInt("FLIGHT_MAX_MESSAGE_SIZE", 256 * 1024 * 1024),
                Duration.ofMillis(envLong("TRINO_REQUEST_TIMEOUT_MS", 30_000)),
                envBool("COORDINATOR_WORKER_CLIENT_ENDPOINTS_REQUIRED", k8sWorkerDiscoveryEnabled),
                envLong("COORDINATOR_WORKER_CLIENT_ENDPOINT_TTL_MS", 2 * 60 * 1000L),
                workerSelectionGraceMs(),
                k8sWorkerDiscoveryEnabled,
                env("COORDINATOR_K8S_NAMESPACE", env("POD_NAMESPACE", "default")),
                env("COORDINATOR_K8S_WORKER_SERVICE_SELECTOR", "role=flight-worker-client-endpoint"),
                env("COORDINATOR_K8S_WORKER_ID_LABEL", "worker-id"),
                env("COORDINATOR_K8S_WORKER_FLIGHT_PORT_NAME", "flight"),
                normalizeUriScheme(env("COORDINATOR_WORKER_CLIENT_URI_SCHEME", "grpc+tcp")),
                envLong("COORDINATOR_K8S_INFORMER_RESYNC_MS", 30_000L),
                envLong("COORDINATOR_K8S_INFORMER_INITIAL_SYNC_TIMEOUT_MS", 30_000L)
        );
    }

    String generatedCtasTable(String queryId) {
        String suffix = queryId.replace("-", "_").replace(".", "_").replace(":", "_").toLowerCase(Locale.ROOT);
        return ctasSchema + "." + ctasTablePrefix + "_" + suffix;
    }

    String objectUriForPrefix(String prefix) {
        return objectStoreUriPrefix + "/" + normalizePrefix(prefix);
    }

    String ctasLocation(String queryId) {
        return ctasLocationPrefix + "/" + normalizePrefix(queryId);
    }

    String defaultSchemaLocation(String schemaName) {
        return defaultSchemaLocationPrefix + "/" + SqlPlanner.validateIdentifier(schemaName, "schemaName");
    }

    private static InetSocketAddress parseListenAddress(String raw, String envName) {
        int split = raw.lastIndexOf(':');
        if (split <= 0 || split == raw.length() - 1) {
            throw new IllegalArgumentException(envName + " must look like 0.0.0.0:8088");
        }
        return new InetSocketAddress(raw.substring(0, split), Integer.parseInt(raw.substring(split + 1)));
    }

    static String normalizePrefix(String raw) {
        String[] parts = raw.replace('\\', '/').split("/");
        StringBuilder out = new StringBuilder();
        for (String part : parts) {
            if (part.isBlank() || part.equals(".") || part.equals("..")) {
                continue;
            }
            if (!out.isEmpty()) {
                out.append('/');
            }
            out.append(part);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("prefix must not be empty");
        }
        return out.toString();
    }

    static String normalizePath(String raw) {
        String[] parts = raw.replace('\\', '/').split("/");
        StringBuilder out = new StringBuilder();
        for (String part : parts) {
            if (part.isBlank() || part.equals(".") || part.equals("..")) {
                continue;
            }
            if (!out.isEmpty()) {
                out.append('/');
            }
            out.append(part);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("path must not be empty");
        }
        String path = out.toString();
        return path.endsWith(".parquet") ? path : path + ".parquet";
    }

    private static String normalizeObjectStoreUriPrefix(String raw) {
        String value = raw == null ? "" : raw.trim();
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException("COORDINATOR_OBJECT_STORE_URI_PREFIX must not be empty");
        }
        return value;
    }

    private static String normalizeUriScheme(String raw) {
        String value = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
        return switch (value) {
            case "grpc+tcp", "grpc+tls", "http", "https" -> value;
            default -> throw new IllegalArgumentException(
                    "COORDINATOR_WORKER_CLIENT_URI_SCHEME must be grpc+tcp, grpc+tls, http, or https"
            );
        };
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static Optional<String> envOptional(String key) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value.trim());
    }

    private static long envLong(String key, long defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value.trim());
    }

    private static long workerSelectionGraceMs() {
        return Math.max(0L, envLong(
                "COORDINATOR_WORKER_SELECTION_GRACE_MS",
                envLong("COORDINATOR_WORKER_PICKUP_GRACE_MS", 0L)
        ));
    }

    private static int envInt(String key, int defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value.trim());
    }

    private static boolean envBool(String key, boolean defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "1", "true", "yes", "on" -> true;
            case "0", "false", "no", "off" -> false;
            default -> throw new IllegalArgumentException(key + " must be a boolean value");
        };
    }
}
