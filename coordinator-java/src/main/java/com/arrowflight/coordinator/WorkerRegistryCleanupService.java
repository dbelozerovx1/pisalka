package com.arrowflight.coordinator;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class WorkerRegistryCleanupService implements AutoCloseable {
    private final ScheduledExecutorService executor;

    private WorkerRegistryCleanupService(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    static WorkerRegistryCleanupService start(Config config, CoordinatorMetadataStore metadataStore) {
        if (!metadataStore.enabled() || config.workerRegistryCleanupIntervalMs <= 0) {
            return new WorkerRegistryCleanupService(null);
        }

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "worker-registry-cleaner");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(() -> {
            try {
                WorkerRegistryCleanupResult result = metadataStore.cleanupStaleWorkers(
                        config.workerRegistryRetentionMs
                );
                if (result.markedStale() > 0 || result.deleted() > 0) {
                    CoordinatorLog.info("worker_registry_cleanup_completed", Map.of(
                            "markedStale", result.markedStale(),
                            "deleted", result.deleted(),
                            "retentionMs", config.workerRegistryRetentionMs
                    ));
                }
            } catch (RuntimeException error) {
                CoordinatorLog.error("worker_registry_cleanup_failed", Map.of(
                        "retentionMs", config.workerRegistryRetentionMs
                ), error);
            }
        }, config.workerRegistryCleanupIntervalMs, config.workerRegistryCleanupIntervalMs, TimeUnit.MILLISECONDS);
        CoordinatorLog.info("worker_registry_cleanup_started", Map.of(
                "intervalMs", config.workerRegistryCleanupIntervalMs,
                "retentionMs", config.workerRegistryRetentionMs
        ));
        return new WorkerRegistryCleanupService(executor);
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
