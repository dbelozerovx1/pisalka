package com.arrowflight.coordinator;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public final class CoordinatorApplication {
    private CoordinatorApplication() {
    }

    public static void main(String[] args) throws Exception {
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
             CoordinatorMetricsServer ignored = CoordinatorMetricsServer.start(config, metrics, coordinator.metadataStore());
             BufferAllocator allocator = new RootAllocator();
             FlightServer server = FlightServer.builder(allocator, location, producer)
                     .middleware(BaseHostnameMiddleware.KEY, BaseHostnameMiddleware.factory())
                     .maxInboundMessageSize(config.flightMaxMessageSize)
                     .build()
                     .start()) {
            System.out.printf(
                    "coordinator flight service listening on %s, trino=%s, metadata_db=%s%n",
                    server.getLocation(),
                    config.trinoUri,
                    coordinator.metadataEnabled()
            );
            server.awaitTermination();
        }
    }
}
