package com.arrowflight.coordinator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class UploadFlavorTest {
    @Test
    void defaultsToSmallAndRejectsUnknownValues() {
        assertEquals(UploadFlavor.SMALL, UploadFlavor.fromRequest(null));
        assertEquals(UploadFlavor.MEDIUM, UploadFlavor.fromRequest("MEDIUM"));
        assertThrows(CoordinatorException.class, () -> UploadFlavor.fromRequest("unlimited"));
    }

    @Test
    void reducesBurstWidthAsClusterUtilizationRises() {
        assertEquals(1, UploadFlavor.SMALL.targetStreams(0));
        assertEquals(4, UploadFlavor.MEDIUM.targetStreams(499));
        assertEquals(3, UploadFlavor.MEDIUM.targetStreams(500));
        assertEquals(2, UploadFlavor.MEDIUM.targetStreams(750));
        assertEquals(1, UploadFlavor.MEDIUM.targetStreams(900));
        assertEquals(8, UploadFlavor.LARGE.targetStreams(499));
        assertEquals(6, UploadFlavor.LARGE.targetStreams(500));
        assertEquals(4, UploadFlavor.LARGE.targetStreams(750));
        assertEquals(1, UploadFlavor.LARGE.targetStreams(900));
    }
}
