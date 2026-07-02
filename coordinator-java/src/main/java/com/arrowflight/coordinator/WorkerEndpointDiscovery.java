package com.arrowflight.coordinator;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

interface WorkerEndpointDiscovery extends AutoCloseable {
    static WorkerEndpointDiscovery start(Config config, CoordinatorMetadataStore metadataStore) {
        if (!config.k8sWorkerDiscoveryEnabled) {
            return () -> {
            };
        }
        if (!metadataStore.enabled()) {
            throw new IllegalStateException(
                    "COORDINATOR_K8S_WORKER_DISCOVERY_ENABLED=true requires COORDINATOR_METADATA_DATABASE_URL"
            );
        }
        return new KubernetesWorkerEndpointDiscovery(config, metadataStore).start();
    }

    @Override
    void close();
}

final class KubernetesWorkerEndpointDiscovery implements WorkerEndpointDiscovery {
    private final Config config;
    private final CoordinatorMetadataStore metadataStore;
    private final Map<String, String> lastFlightUris = new ConcurrentHashMap<>();
    private KubernetesClient client;
    private SharedIndexInformer<Service> informer;

    KubernetesWorkerEndpointDiscovery(Config config, CoordinatorMetadataStore metadataStore) {
        this.config = config;
        this.metadataStore = metadataStore;
    }

    KubernetesWorkerEndpointDiscovery start() {
        client = new KubernetesClientBuilder().build();
        System.out.printf(
                "starting Kubernetes worker endpoint discovery namespace=%s selector=%s%n",
                config.k8sNamespace,
                config.k8sWorkerServiceSelector
        );
        informer = client.services()
                .inNamespace(config.k8sNamespace)
                .withLabelSelector(config.k8sWorkerServiceSelector)
                .inform(new ResourceEventHandler<>() {
                    @Override
                    public void onAdd(Service service) {
                        safeUpsert(service);
                    }

                    @Override
                    public void onUpdate(Service oldService, Service newService) {
                        safeUpsert(newService);
                    }

                    @Override
                    public void onDelete(Service service, boolean deletedFinalStateUnknown) {
                        safeRemove(service);
                    }
                }, config.k8sInformerResyncMs);
        waitForInitialSync();
        return this;
    }

    private void waitForInitialSync() {
        long deadline = System.currentTimeMillis() + config.k8sInformerInitialSyncTimeoutMs;
        while (!informer.hasSynced() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        System.out.printf(
                "Kubernetes worker endpoint discovery synced=%s%n",
                informer.hasSynced()
        );
    }

    private void safeUpsert(Service service) {
        try {
            upsert(service);
        } catch (RuntimeException error) {
            System.err.printf(
                    "failed to update worker client endpoint from service=%s error=%s%n",
                    serviceName(service),
                    error.getMessage()
            );
        }
    }

    private void safeRemove(Service service) {
        try {
            remove(service);
        } catch (RuntimeException error) {
            System.err.printf(
                    "failed to remove worker client endpoint from service=%s error=%s%n",
                    serviceName(service),
                    error.getMessage()
            );
        }
    }

    private void upsert(Service service) {
        Optional<String> workerId = workerId(service);
        if (workerId.isEmpty()) {
            return;
        }

        Optional<String> endpoint = endpointUri(service);
        if (endpoint.isEmpty()) {
            metadataStore.deleteWorkerClientEndpoint(workerId.get());
            if (lastFlightUris.remove(workerId.get()) != null) {
                System.out.printf("worker client endpoint unavailable worker_id=%s%n", workerId.get());
            }
            return;
        }

        WorkerClientEndpoint clientEndpoint = new WorkerClientEndpoint(
                workerId.get(),
                endpoint.get(),
                "kubernetes-service",
                Instant.now().plusMillis(config.workerClientEndpointTtlMs),
                Optional.empty()
        );
        metadataStore.upsertWorkerClientEndpoint(clientEndpoint);
        String previous = lastFlightUris.put(workerId.get(), endpoint.get());
        if (!endpoint.get().equals(previous)) {
            System.out.printf("worker client endpoint updated worker_id=%s flight_uri=%s%n", workerId.get(), endpoint.get());
        }
    }

    private void remove(Service service) {
        workerId(service).ifPresent(workerId -> {
            metadataStore.deleteWorkerClientEndpoint(workerId);
            if (lastFlightUris.remove(workerId) != null) {
                System.out.printf("worker client endpoint removed worker_id=%s%n", workerId);
            }
        });
    }

    private Optional<String> workerId(Service service) {
        Map<String, String> labels = service.getMetadata() == null ? null : service.getMetadata().getLabels();
        if (labels == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(labels.get(config.k8sWorkerIdLabel))
                .map(String::trim)
                .filter(value -> !value.isBlank());
    }

    private Optional<String> endpointUri(Service service) {
        Optional<String> host = loadBalancerHost(service);
        Optional<Integer> port = flightPort(service);
        if (host.isEmpty() || port.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(config.workerClientUriScheme + "://" + bracketIpv6(host.get()) + ":" + port.get());
    }

    private Optional<String> loadBalancerHost(Service service) {
        if (service.getStatus() == null
                || service.getStatus().getLoadBalancer() == null
                || service.getStatus().getLoadBalancer().getIngress() == null) {
            return Optional.empty();
        }
        for (LoadBalancerIngress ingress : service.getStatus().getLoadBalancer().getIngress()) {
            String ip = trimToNull(ingress.getIp());
            if (ip != null) {
                return Optional.of(ip);
            }
            String hostname = trimToNull(ingress.getHostname());
            if (hostname != null) {
                return Optional.of(hostname);
            }
        }
        return Optional.empty();
    }

    private Optional<Integer> flightPort(Service service) {
        if (service.getSpec() == null || service.getSpec().getPorts() == null) {
            return Optional.empty();
        }
        List<ServicePort> ports = service.getSpec().getPorts();
        for (ServicePort port : ports) {
            if (config.k8sWorkerFlightPortName.equals(port.getName())) {
                return Optional.ofNullable(port.getPort());
            }
        }
        if (ports.size() == 1) {
            return Optional.ofNullable(ports.getFirst().getPort());
        }
        return Optional.empty();
    }

    private static String trimToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static String bracketIpv6(String host) {
        if (host.contains(":") && !host.startsWith("[") && !host.endsWith("]")) {
            return "[" + host + "]";
        }
        return host;
    }

    private static String serviceName(Service service) {
        if (service == null || service.getMetadata() == null) {
            return "<unknown>";
        }
        return service.getMetadata().getName();
    }

    @Override
    public void close() {
        if (informer != null) {
            informer.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
