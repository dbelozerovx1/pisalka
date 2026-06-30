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
import java.util.List;
import java.util.Map;

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
            request = descriptorRequest(descriptor);
            FlightInfo response = flightInfo(coordinator.startFlight(request), false);
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
            request = descriptorRequest(descriptor);
            PollResult result = coordinator.pollFlight(request);
            FlightDescriptor nextDescriptor = result.complete()
                    ? null
                    : FlightDescriptor.command(jsonBytes(Map.of("type", "poll", "queryId", result.plan().queryId())));
            PollInfo response = new PollInfo(
                    flightInfo(result.plan(), result.complete()),
                    nextDescriptor,
                    result.progress().orElse(null),
                    result.complete() ? null : result.expiresAt()
            );
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
            request = actionBody(action);
            Map<String, Object> response = switch (action.getType()) {
                case "coordinator.config" -> coordinator.configJson();
                case "coordinator.create-upload" -> coordinator.createUpload(request);
                case "coordinator.commit-upload", "coordinator.do-commit" -> coordinator.commitUpload(request);
                case "coordinator.abort-upload" -> coordinator.abortUpload(request);
                case "coordinator.drop-temp", "coordinator.drop_temp" -> coordinator.dropTemp(request);
                case "coordinator.put-ticket" -> coordinator.putTicket(request);
                case "coordinator.get-ticket" -> coordinator.getTicket(request);
                default -> throw new CoordinatorException(400, "unknown coordinator action: " + action.getType());
            };
            listener.onNext(new Result(jsonBytes(response)));
            listener.onCompleted();
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
}
