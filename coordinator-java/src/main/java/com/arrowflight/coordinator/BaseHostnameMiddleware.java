package com.arrowflight.coordinator;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

import java.util.Optional;

final class BaseHostnameMiddleware implements FlightServerMiddleware {
    static final String HEADER_NAME = "x-base-hostname";
    static final FlightServerMiddleware.Key<BaseHostnameMiddleware> KEY =
            FlightServerMiddleware.Key.of("base-hostname");

    private final Optional<String> baseHostname;

    private BaseHostnameMiddleware(Optional<String> baseHostname) {
        this.baseHostname = baseHostname;
    }

    static FlightServerMiddleware.Factory<BaseHostnameMiddleware> factory() {
        return BaseHostnameMiddleware::fromHeaders;
    }

    Optional<String> baseHostname() {
        return baseHostname;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    }

    @Override
    public void onCallCompleted(org.apache.arrow.flight.CallStatus status) {
    }

    @Override
    public void onCallErrored(Throwable err) {
    }

    private static BaseHostnameMiddleware fromHeaders(
            CallInfo callInfo,
            CallHeaders incomingHeaders,
            RequestContext context
    ) {
        return new BaseHostnameMiddleware(Optional.ofNullable(incomingHeaders.get(HEADER_NAME))
                .map(String::trim)
                .filter(value -> !value.isBlank()));
    }
}
