package com.arrowflight.coordinator;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class UploadCleanupService implements AutoCloseable {
    private final ScheduledExecutorService executor;

    private UploadCleanupService(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    static UploadCleanupService start(Config config, CoordinatorService coordinator) {
        if (!coordinator.metadataEnabled() || config.uploadCleanupIntervalMs <= 0) {
            return new UploadCleanupService(null);
        }
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "expired-upload-cleaner");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(() -> {
            try {
                coordinator.cleanupExpiredUploads();
            } catch (RuntimeException error) {
                CoordinatorLog.error("expired_upload_cleanup_cycle_failed", Map.of(), error);
            }
        }, config.uploadCleanupIntervalMs, config.uploadCleanupIntervalMs, TimeUnit.MILLISECONDS);
        CoordinatorLog.info("expired_upload_cleanup_started", Map.of(
                "intervalMs", config.uploadCleanupIntervalMs
        ));
        return new UploadCleanupService(executor);
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
