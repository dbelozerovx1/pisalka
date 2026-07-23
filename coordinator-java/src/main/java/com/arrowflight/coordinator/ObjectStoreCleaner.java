package com.arrowflight.coordinator;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

final class ObjectStoreCleaner implements AutoCloseable {
    private static final int DELETE_BATCH_SIZE = 1_000;

    private final Config config;
    private final S3Client s3;

    ObjectStoreCleaner(Config config) {
        this.config = config;
        this.s3 = s3Client(config);
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
            return new CleanupResult("", "", false, false, 0, 0, Optional.of("uri prefix must not be empty"));
        }
        int attemptedObjects = 0;
        int deletedObjects = 0;
        ArrayList<String> errors = new ArrayList<>();
        try {
            S3Location location = S3Location.parse(uri);
            String objectPrefix = directoryPrefix(location.key());
            String continuationToken = null;
            do {
                ListObjectsV2Response page = s3.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(location.bucket())
                        .prefix(objectPrefix)
                        .continuationToken(continuationToken)
                        .build());
                List<String> keys = page.contents().stream()
                        .map(software.amazon.awssdk.services.s3.model.S3Object::key)
                        .toList();
                attemptedObjects += keys.size();
                DeleteBatchResult deleted = deleteKeys(location.bucket(), keys);
                deletedObjects += deleted.deletedObjects();
                errors.addAll(deleted.errors());
                if (!deleted.errors().isEmpty()) {
                    break;
                }
                continuationToken = page.nextContinuationToken();
            } while (continuationToken != null);
        } catch (Exception error) {
            errors.add(errorMessage(error));
        }
        boolean existed = attemptedObjects > 0;
        return new CleanupResult(
                uri,
                uri,
                existed,
                existed && errors.isEmpty() && deletedObjects == attemptedObjects,
                attemptedObjects,
                deletedObjects,
                errors.isEmpty() ? Optional.empty() : Optional.of(String.join("; ", errors.stream().limit(8).toList()))
        );
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
        DeleteBatchResult deleted = deleteKeys(bucket, keys.stream().toList());
        deletedObjects += deleted.deletedObjects();
        errors.addAll(deleted.errors());
        existed = deletedObjects > 0;
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
        try {
            String continuationToken = null;
            String objectPrefix = parent + "/" + stem;
            do {
                ListObjectsV2Response page = s3.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(objectPrefix)
                        .continuationToken(continuationToken)
                        .build());
                for (software.amazon.awssdk.services.s3.model.S3Object object : page.contents()) {
                    String key = object.key();
                    int candidateSlash = key.lastIndexOf('/');
                    String candidate = candidateSlash < 0 ? key : key.substring(candidateSlash + 1);
                    if (candidate.equals(fileName)
                            || (candidate.startsWith(stem + "-part-") && candidate.endsWith(".parquet"))) {
                        keys.add(key);
                    }
                }
                continuationToken = page.nextContinuationToken();
            } while (continuationToken != null);
        } catch (Exception error) {
            errors.add(descriptor + " discovery: " + errorMessage(error));
        }
    }

    private DeleteBatchResult deleteKeys(String bucket, List<String> keys) {
        int deletedObjects = 0;
        ArrayList<String> errors = new ArrayList<>();
        for (int start = 0; start < keys.size(); start += DELETE_BATCH_SIZE) {
            int end = Math.min(start + DELETE_BATCH_SIZE, keys.size());
            List<ObjectIdentifier> objects = keys.subList(start, end).stream()
                    .map(key -> ObjectIdentifier.builder().key(key).build())
                    .toList();
            try {
                DeleteObjectsResponse response = s3.deleteObjects(DeleteObjectsRequest.builder()
                        .bucket(bucket)
                        .delete(Delete.builder().objects(objects).quiet(false).build())
                        .build());
                deletedObjects += response.deleted().size();
                response.errors().forEach(error -> errors.add(
                        error.key() + ": " + error.code() + ": " + error.message()
                ));
            } catch (Exception error) {
                errors.add("delete batch starting with " + keys.get(start) + ": " + errorMessage(error));
            }
        }
        return new DeleteBatchResult(deletedObjects, errors);
    }

    private static S3Client s3Client(Config config) {
        if (config.s3AccessKey.isPresent() != config.s3SecretKey.isPresent()) {
            throw new IllegalArgumentException(
                    "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together"
            );
        }

        S3ClientBuilder builder = S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.of(config.s3Region))
                .forcePathStyle(config.s3PathStyleAccess);
        if (config.s3AccessKey.isPresent()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    config.s3AccessKey.orElseThrow(),
                    config.s3SecretKey.orElseThrow()
            )));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }
        config.s3Endpoint.ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
        return builder.build();
    }

    private static String directoryPrefix(String key) {
        String normalized = Config.normalizePrefix(key);
        return normalized.endsWith("/") ? normalized : normalized + "/";
    }

    private static String errorMessage(Throwable error) {
        String message = error.getMessage();
        return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
    }

    @Override
    public void close() {
        s3.close();
    }

    private record DeleteBatchResult(int deletedObjects, List<String> errors) {
    }

    private record S3Location(String bucket, String key) {
        private static S3Location parse(String rawUri) {
            URI uri = URI.create(rawUri);
            String scheme = Optional.ofNullable(uri.getScheme()).orElse("").toLowerCase();
            if (!scheme.equals("s3") && !scheme.equals("s3a")) {
                throw new IllegalArgumentException("object-store URI must use s3:// or s3a://");
            }
            String bucket = Optional.ofNullable(uri.getAuthority()).orElse("").trim();
            if (bucket.isBlank()) {
                throw new IllegalArgumentException("object-store URI must include a bucket");
            }
            String key = Optional.ofNullable(uri.getPath()).orElse("");
            while (key.startsWith("/")) {
                key = key.substring(1);
            }
            if (key.isBlank()) {
                throw new IllegalArgumentException("refusing to delete an entire bucket");
            }
            return new S3Location(bucket, key);
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
