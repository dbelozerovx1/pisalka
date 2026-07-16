package com.arrowflight.coordinator;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorErrorFormatterTest {
    @Test
    void errorEnvelopeKeepsRequestAndOperationCorrelation() {
        CoordinatorErrorFormatter.ErrorEnvelope envelope = CoordinatorErrorFormatter.envelope(
                new CoordinatorException(500, "database unavailable"),
                CoordinatorErrorFormatter.ErrorContext.action(
                        "coordinator.commit-upload",
                        Map.of(
                                "requestId", "coord-req-1",
                                "operationId", "operation-1",
                                "uploadId", "upload-1"
                        ),
                        37L
                )
        );

        assertTrue(envelope.errorId().startsWith("coord-err-"));
        assertEquals("coord-req-1", envelope.context().ids().get("requestId"));
        assertEquals("operation-1", envelope.context().ids().get("operationId"));
        assertEquals("upload-1", envelope.context().ids().get("uploadId"));
        assertEquals(37L, envelope.context().elapsedMs());
        assertTrue(envelope.userDescription().contains("requestId=coord-req-1"));
        assertTrue(envelope.userDescription().contains("operationId=operation-1"));
        assertTrue(envelope.userDescription().contains("uploadId=upload-1"));
    }
}
