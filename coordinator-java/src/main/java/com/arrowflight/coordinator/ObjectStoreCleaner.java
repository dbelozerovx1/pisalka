package com.arrowflight.coordinator;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

final class ObjectStoreCleaner {
    private final Config config;
    private final Configuration hadoopConf;

    ObjectStoreCleaner(Config config) {
        this.config = config;
        this.hadoopConf = IcebergCommitter.hadoopConfiguration(config);
    }

    CleanupResult deleteUploadObjects(UploadSnapshot snapshot, String bucket) {
        LinkedHashSet<String> keys = new LinkedHashSet<>();
        LinkedHashSet<String> descriptors = new LinkedHashSet<>();
        for (UploadStreamState stream : snapshot.streams()) {
            String descriptor = Config.normalizePath(stream.descriptorPath());
            keys.add(descriptor);
            descriptors.add(descriptor);
        }
        for (UploadFile file : snapshot.files()) {
            keys.add(Config.normalizePath(file.filePath()));
        }
        return deleteObjects(bucket, snapshot.session().stagingPrefix(), keys, descriptors);
    }

    CleanupResult deleteCtasStaging(String queryId) {
        return deleteUriPrefix(config.ctasLocation(queryId));
    }

    CleanupResult deleteLocationPrefix(String rawUri) {
        return deleteUriPrefix(rawUri);
    }

    private CleanupResult deleteUriPrefix(String rawUri) {
        String uri = rawUri == null ? "" : rawUri.trim();
        if (uri.isBlank()) {
            return new CleanupResult("", "", false, false, 1, 0, Optional.of("uri prefix must not be empty"));
        }
        return deleteUriPrefix(uri, uri);
    }

    private CleanupResult deleteUriPrefix(String prefix, String uri) {
        try {
            Path path = new Path(uri);
            FileSystem fs = path.getFileSystem(hadoopConf);
            boolean existed = fs.exists(path);
            boolean deleted = fs.delete(path, true);
            return new CleanupResult(prefix, uri, existed, deleted, 1, deleted ? 1 : 0, Optional.empty());
        } catch (Exception error) {
            return new CleanupResult(prefix, uri, false, false, 1, 0, Optional.of(error.getMessage()));
        }
    }

    private CleanupResult deleteObjects(
            String bucket,
            String rawPrefix,
            Set<String> knownKeys,
            Set<String> descriptorKeys
    ) {
        String prefix = Config.normalizePrefix(rawPrefix);
        String uri = config.objectUriForBucket(bucket, prefix);
        boolean existed = false;
        int deletedObjects = 0;
        ArrayList<String> errors = new ArrayList<>();
        LinkedHashSet<String> keys = new LinkedHashSet<>(knownKeys);
        for (String descriptor : descriptorKeys) {
            discoverDescriptorObjects(bucket, descriptor, keys, errors);
        }
        for (String key : keys) {
            String objectUri = config.objectUriForBucket(bucket, key);
            try {
                Path path = new Path(objectUri);
                FileSystem fs = path.getFileSystem(hadoopConf);
                boolean objectExisted = fs.exists(path);
                existed = existed || objectExisted;
                if (objectExisted && fs.delete(path, false)) {
                    deletedObjects++;
                }
            } catch (Exception error) {
                errors.add(key + ": " + error.getMessage());
            }
        }
        return new CleanupResult(
                prefix,
                uri,
                existed,
                errors.isEmpty(),
                keys.size(),
                deletedObjects,
                errors.isEmpty() ? Optional.empty() : Optional.of(String.join("; ", errors.stream().limit(8).toList()))
        );
    }

    private void discoverDescriptorObjects(
            String bucket,
            String descriptor,
            Set<String> keys,
            ArrayList<String> errors
    ) {
        int slash = descriptor.lastIndexOf('/');
        String parent = slash < 0 ? "" : descriptor.substring(0, slash);
        String fileName = slash < 0 ? descriptor : descriptor.substring(slash + 1);
        String stem = fileName.endsWith(".parquet")
                ? fileName.substring(0, fileName.length() - ".parquet".length())
                : fileName;
        if (parent.isBlank() || stem.isBlank()) {
            return;
        }
        String globUri = config.objectUriForBucket(bucket, parent) + "/" + stem + "*.parquet";
        try {
            Path glob = new Path(globUri);
            FileSystem fs = glob.getFileSystem(hadoopConf);
            FileStatus[] matches = fs.globStatus(glob);
            if (matches == null) {
                return;
            }
            for (FileStatus match : matches) {
                if (!match.isFile()) {
                    continue;
                }
                String candidate = match.getPath().getName();
                if (candidate.equals(fileName)
                        || (candidate.startsWith(stem + "-part-") && candidate.endsWith(".parquet"))) {
                    keys.add(parent + "/" + candidate);
                }
            }
        } catch (Exception error) {
            errors.add(descriptor + " discovery: " + error.getMessage());
        }
    }
}

record CleanupResult(
        String prefix,
        String uri,
        boolean existed,
        boolean deleted,
        int attemptedObjects,
        int deletedObjects,
        Optional<String> errorMessage
) {
    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("prefix", prefix);
        body.put("uri", uri);
        body.put("existed", existed);
        body.put("deleted", deleted);
        body.put("attemptedObjects", attemptedObjects);
        body.put("deletedObjects", deletedObjects);
        errorMessage.ifPresent(value -> body.put("errorMessage", value));
        return body;
    }

    boolean succeeded() {
        return errorMessage.isEmpty();
    }
}
