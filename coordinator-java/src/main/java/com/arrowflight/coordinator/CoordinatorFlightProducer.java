package com.arrowflight.coordinator;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class CoordinatorFlightProducer implements FlightProducer {
    private static final Schema EMPTY_SCHEMA = new Schema(List.of());

    private final CoordinatorService coordinator;
    private final CoordinatorMetrics metrics;

    CoordinatorFlightProducer(CoordinatorService coordinator, CoordinatorMetrics metrics) {
        this.coordinator = coordinator;
        this.metrics = metrics;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        listener.error(CallStatus.UNIMPLEMENTED
                .withDescription("coordinator only plans Flight endpoints; redeem DoGet tickets on worker locations")
                .toRuntimeException());
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        Map<String, Object> request = Map.of();
        try {
            request = ensureRequestId(requestWithHeaders(context, descriptorRequest(descriptor)));
            FlightPlan plan = withRequestId(coordinator.startFlight(request, endpointRewrite(context)), request);
            FlightInfo response = flightInfo(plan, false);
            CoordinatorRequestLog.success("GetFlightInfo", null, request, Map.of(
                    "queryId", plan.queryId(),
                    "status", plan.status(),
                    "requestId", request.get("requestId"),
                    "endpointCount", plan.endpoints().size()
            ));
            metrics.recordSuccess("GetFlightInfo", null);
            return response;
        } catch (RuntimeException error) {
            throw CoordinatorErrorFormatter.toFlight(
                    error,
                    CoordinatorErrorFormatter.ErrorContext.method("GetFlightInfo", request),
                    metrics
            );
        }
    }

    @Override
    public PollInfo pollFlightInfo(CallContext context, FlightDescriptor descriptor) {
        Map<String, Object> request = Map.of();
        try {
            request = ensureRequestId(requestWithHeaders(context, descriptorRequest(descriptor)));
            PollResult result = coordinator.pollFlight(request, endpointRewrite(context));
            FlightPlan plan = withRequestId(result.plan(), request);
            FlightDescriptor nextDescriptor = result.complete()
                    ? null
                    : FlightDescriptor.command(jsonBytes(Map.of("type", "poll", "queryId", plan.queryId())));
            PollInfo response = new PollInfo(
                    flightInfo(plan, result.complete()),
                    nextDescriptor,
                    result.progress().orElse(null),
                    result.complete() ? null : result.expiresAt()
            );
            CoordinatorRequestLog.success("PollFlightInfo", null, request, Map.of(
                    "queryId", plan.queryId(),
                    "status", plan.status(),
                    "requestId", request.get("requestId"),
                    "endpointCount", plan.endpoints().size()
            ));
            metrics.recordSuccess("PollFlightInfo", null);
            return response;
        } catch (RuntimeException error) {
            throw CoordinatorErrorFormatter.toFlight(
                    error,
                    CoordinatorErrorFormatter.ErrorContext.method("PollFlightInfo", request),
                    metrics
            );
        }
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> listener) {
        throw CallStatus.UNIMPLEMENTED
                .withDescription("coordinator does not accept DoPut; use coordinator.create-upload action and worker tickets")
                .toRuntimeException();
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        Map<String, Object> request = Map.of();
        try {
            request = ensureRequestId(requestWithHeaders(context, actionBody(action)));
            WorkerEndpointRewrite endpointRewrite = endpointRewrite(context);
            Map<String, Object> response = switch (action.getType()) {
                case "coordinator.config" -> coordinator.configJson();
                case "coordinator.create-schema", "coordinator.create_schema" -> coordinator.createSchema(request);
                case "coordinator.create-upload" -> coordinator.createUpload(request, endpointRewrite);
                case "coordinator.commit-upload", "coordinator.do-commit" -> coordinator.commitUpload(request);
                case "coordinator.abort-upload" -> coordinator.abortUpload(request);
                case "coordinator.drop-temp", "coordinator.drop_temp" -> coordinator.dropTemp(request);
                case "coordinator.put-ticket" -> coordinator.putTicket(request, endpointRewrite);
                case "coordinator.get-ticket" -> coordinator.getTicket(request, endpointRewrite);
                default -> throw new CoordinatorException(400, "unknown coordinator action: " + action.getType());
            };
            response = withRequestContext(response, request);
            listener.onNext(new Result(jsonBytes(response)));
            listener.onCompleted();
            CoordinatorRequestLog.success("DoAction", action.getType(), request, response);
            metrics.recordSuccess("DoAction", action.getType());
        } catch (RuntimeException error) {
            listener.onError(CoordinatorErrorFormatter.toFlight(
                    error,
                    CoordinatorErrorFormatter.ErrorContext.action(action.getType(), request),
                    metrics
            ));
        }
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onNext(new ActionType("coordinator.config", "Return non-secret coordinator configuration"));
        listener.onNext(new ActionType("coordinator.create-schema", "Create an Iceberg schema through Trino"));
        listener.onNext(new ActionType("coordinator.create-upload", "Create a durable upload session and signed DoPut tickets"));
        listener.onNext(new ActionType("coordinator.commit-upload", "Commit uploaded files to Iceberg with append or overwrite"));
        listener.onNext(new ActionType("coordinator.do-commit", "Alias for coordinator.commit-upload"));
        listener.onNext(new ActionType("coordinator.abort-upload", "Mark an upload session aborted"));
        listener.onNext(new ActionType("coordinator.drop-temp", "Drop a coordinator-created CTAS temp table by query id"));
        listener.onNext(new ActionType("coordinator.put-ticket", "Low-level signed DoPut ticket for internal/dev use"));
        listener.onNext(new ActionType("coordinator.get-ticket", "Low-level signed DoGet ticket for internal/dev use"));
        listener.onCompleted();
    }

    private FlightInfo flightInfo(FlightPlan plan, boolean finalResult) {
        FlightDescriptor descriptor = finalResult
                ? FlightDescriptor.command(jsonBytes(Map.of("type", "complete", "queryId", plan.queryId())))
                : FlightDescriptor.command(jsonBytes(Map.of("type", "poll", "queryId", plan.queryId())));
        return FlightInfo.builder(EMPTY_SCHEMA, descriptor, endpoints(plan))
                .setBytes(plan.totalBytes())
                .setRecords(plan.totalRecords())
                .setOrdered(false)
                .setOption(IpcOption.DEFAULT)
                .setAppMetadata(jsonBytes(plan.metadata()))
                .build();
    }

    private List<FlightEndpoint> endpoints(FlightPlan plan) {
        ArrayList<FlightEndpoint> endpoints = new ArrayList<>();
        for (Map<String, Object> endpoint : plan.endpoints()) {
            String ticket = Json.requiredString(endpoint, "ticket");
            String flightUri = Json.requiredString(endpoint, "flightUri");
            try {
                endpoints.add(FlightEndpoint
                        .builder(new Ticket(ticket.getBytes(StandardCharsets.UTF_8)), new Location(flightUri))
                        .setAppMetadata(jsonBytes(endpoint))
                        .build());
            } catch (Exception error) {
                throw new CoordinatorException(500, "failed to build Flight endpoint for " + flightUri, error);
            }
        }
        return endpoints;
    }

    private Map<String, Object> descriptorRequest(FlightDescriptor descriptor) {
        if (descriptor.isCommand()) {
            String raw = new String(descriptor.getCommand(), StandardCharsets.UTF_8);
            return Json.parseObject(raw);
        }
        List<String> path = descriptor.getPath();
        if (path.size() >= 2 && path.getFirst().equalsIgnoreCase("read")) {
            return Map.of("type", "read", "path", String.join("/", path.subList(1, path.size())));
        }
        if (path.size() >= 2 && path.getFirst().equalsIgnoreCase("poll")) {
            return Map.of("type", "poll", "queryId", path.get(1));
        }
        throw new CoordinatorException(400, "FlightDescriptor must be a JSON command or path read/<path>");
    }

    private Map<String, Object> actionBody(Action action) {
        byte[] body = action.getBody();
        if (body.length == 0) {
            return Map.of();
        }
        return Json.parseObject(new String(body, StandardCharsets.UTF_8));
    }

    private byte[] jsonBytes(Object value) {
        return Json.stringify(value).getBytes(StandardCharsets.UTF_8);
    }

    private WorkerEndpointRewrite endpointRewrite(CallContext context) {
        BaseHostnameMiddleware middleware = context.getMiddleware(BaseHostnameMiddleware.KEY);
        if (middleware == null) {
            return WorkerEndpointRewrite.NONE;
        }
        return WorkerEndpointRewrite.fromBaseHostname(middleware.baseHostname());
    }

    private Map<String, Object> requestWithHeaders(CallContext context, Map<String, Object> request) {
        BaseHostnameMiddleware middleware = context.getMiddleware(BaseHostnameMiddleware.KEY);
        if (middleware == null || (middleware.authorization().isEmpty()
                && middleware.requestId().isEmpty()
                && middleware.trinoUser().isEmpty())) {
            return request;
        }
        LinkedHashMap<String, Object> merged = new LinkedHashMap<>(request);
        middleware.authorization().ifPresent(value -> merged.put("authorization", value));
        middleware.requestId().ifPresent(value -> merged.put("requestId", value));
        middleware.trinoUser().ifPresent(value -> merged.put("user", value));
        return merged;
    }

    private Map<String, Object> ensureRequestId(Map<String, Object> request) {
        Object existing = request.get("requestId");
        if (existing != null && !String.valueOf(existing).isBlank()) {
            return request;
        }
        LinkedHashMap<String, Object> merged = new LinkedHashMap<>(request);
        merged.put("requestId", "coord-req-" + UUID.randomUUID().toString().replace("-", ""));
        return merged;
    }

    private Map<String, Object> withRequestContext(Map<String, Object> response, Map<String, Object> request) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>(response);
        copyResponseId(body, request, "requestId");
        copyResponseId(body, request, "operationId");
        copyResponseId(body, request, "uploadId");
        copyResponseId(body, request, "queryId");
        copyResponseId(body, request, "attemptId");
        copyResponseId(body, request, "streamId");
        copyResponseId(body, request, "tableName");
        return body;
    }

    private void copyResponseId(LinkedHashMap<String, Object> response, Map<String, Object> request, String key) {
        if (response.containsKey(key)) {
            return;
        }
        Object value = request.get(key);
        if (value != null && !String.valueOf(value).isBlank()) {
            response.put(key, value);
        }
    }

    private FlightPlan withRequestId(FlightPlan plan, Map<String, Object> request) {
        LinkedHashMap<String, Object> metadata = new LinkedHashMap<>(plan.metadata());
        copyResponseId(metadata, request, "requestId");
        return new FlightPlan(
                plan.queryId(),
                plan.status(),
                metadata,
                plan.endpoints(),
                plan.totalRecords(),
                plan.totalBytes(),
                plan.expiresAt()
        );
    }
}
