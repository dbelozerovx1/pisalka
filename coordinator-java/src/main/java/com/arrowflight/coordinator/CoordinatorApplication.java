package com.arrowflight.coordinator;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.Map;

public final class CoordinatorApplication {
    private CoordinatorApplication() {
    }

    public static void main(String[] args) throws Exception {
        CoordinatorLog.installStdStreamWrapper();
        Config config = Config.fromEnv();
        CoordinatorMigrations.migrate(config);
        CoordinatorMetrics metrics = new CoordinatorMetrics();
        CoordinatorService coordinator = new CoordinatorService(config);
        CoordinatorFlightProducer producer = new CoordinatorFlightProducer(coordinator, metrics);
        Location location = Location.forGrpcInsecure(
                config.listenAddress.getHostString(),
                config.listenAddress.getPort()
        );

        try (WorkerEndpointDiscovery ignoredDiscovery = WorkerEndpointDiscovery.start(config, coordinator.metadataStore());
             UploadCleanupService ignoredCleanup = UploadCleanupService.start(config, coordinator);
             WorkerRegistryCleanupService ignoredWorkerCleanup = WorkerRegistryCleanupService.start(
                     config,
                     coordinator.metadataStore()
             );
             CoordinatorMetricsServer ignored = CoordinatorMetricsServer.start(config, metrics, coordinator.metadataStore());
             BufferAllocator allocator = new RootAllocator();
             FlightServer server = FlightServer.builder(allocator, location, producer)
                     .middleware(BaseHostnameMiddleware.KEY, BaseHostnameMiddleware.factory())
                     .maxInboundMessageSize(config.flightMaxMessageSize)
                     .build()
                     .start()) {
            CoordinatorLog.info("coordinator_started", Map.of(
                    "location", server.getLocation().toString(),
                    "trinoUri", config.trinoUri.toString(),
                    "metadataDb", coordinator.metadataEnabled()
            ));
            server.awaitTermination();
        }
    }
}
