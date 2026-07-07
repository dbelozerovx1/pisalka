package com.arrowflight.coordinator;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

import java.util.Optional;

final class BaseHostnameMiddleware implements FlightServerMiddleware {
    static final String HEADER_NAME = "x-base-hostname";
    static final String AUTHORIZATION_HEADER = "authorization";
    static final String REQUEST_ID_HEADER = "x-request-id";
    static final String TRINO_USER_HEADER = "x-trino-user";
    static final FlightServerMiddleware.Key<BaseHostnameMiddleware> KEY =
            FlightServerMiddleware.Key.of("base-hostname");

    private final Optional<String> baseHostname;
    private final Optional<String> authorization;
    private final Optional<String> requestId;
    private final Optional<String> trinoUser;

    private BaseHostnameMiddleware(
            Optional<String> baseHostname,
            Optional<String> authorization,
            Optional<String> requestId,
            Optional<String> trinoUser
    ) {
        this.baseHostname = baseHostname;
        this.authorization = authorization;
        this.requestId = requestId;
        this.trinoUser = trinoUser;
    }

    static FlightServerMiddleware.Factory<BaseHostnameMiddleware> factory() {
        return BaseHostnameMiddleware::fromHeaders;
    }

    Optional<String> baseHostname() {
        return baseHostname;
    }

    Optional<String> authorization() {
        return authorization;
    }

    Optional<String> requestId() {
        return requestId;
    }

    Optional<String> trinoUser() {
        return trinoUser;
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
        return new BaseHostnameMiddleware(
                trimHeader(incomingHeaders, HEADER_NAME),
                trimHeader(incomingHeaders, AUTHORIZATION_HEADER).map(BaseHostnameMiddleware::bearerAuthorization),
                trimHeader(incomingHeaders, REQUEST_ID_HEADER),
                trimHeader(incomingHeaders, TRINO_USER_HEADER)
        );
    }

    private static Optional<String> trimHeader(CallHeaders headers, String name) {
        return Optional.ofNullable(headers.get(name))
                .or(() -> Optional.ofNullable(headers.get(name.toUpperCase(java.util.Locale.ROOT))))
                .map(String::trim)
                .filter(value -> !value.isBlank());
    }

    private static String bearerAuthorization(String raw) {
        String value = raw.trim();
        if (value.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
            value = value.substring("Bearer".length()).trim();
        }
        return "Bearer " + value;
    }
}
