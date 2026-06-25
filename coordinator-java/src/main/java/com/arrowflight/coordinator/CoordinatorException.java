package com.arrowflight.coordinator;

final class CoordinatorException extends RuntimeException {
    final int status;

    CoordinatorException(int status, String message) {
        super(message);
        this.status = status;
    }

    CoordinatorException(int status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }
}
