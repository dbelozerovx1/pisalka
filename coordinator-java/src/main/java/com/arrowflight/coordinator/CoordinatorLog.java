package com.arrowflight.coordinator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

final class CoordinatorLog {
    private static final LogIdentity IDENTITY = LogIdentity.fromEnv("coordinator");
    private static final PrintStream ORIGINAL_OUT = System.out;
    private static final PrintStream ORIGINAL_ERR = System.err;

    private CoordinatorLog() {
    }

    static void installStdStreamWrapper() {
        System.setOut(new PrintStream(
                new StructuredLineOutputStream(false),
                true,
                StandardCharsets.UTF_8
        ));
        System.setErr(new PrintStream(
                new StructuredLineOutputStream(true),
                true,
                StandardCharsets.UTF_8
        ));
    }

    static void info(String event, Map<String, Object> fields) {
        write(ORIGINAL_OUT, "INFO", event, fields, null);
    }

    static void warn(String event, Map<String, Object> fields) {
        write(ORIGINAL_OUT, "WARN", event, fields, null);
    }

    static void error(String event, Map<String, Object> fields, Throwable error) {
        write(ORIGINAL_ERR, "ERROR", event, fields, error);
    }

    private static void write(
            PrintStream stream,
            String level,
            String event,
            Map<String, Object> fields,
            Throwable error
    ) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        IDENTITY.putInto(body);
        body.put("timestamp", Instant.now().toString());
        body.put("level", level);
        body.put("event", event);
        body.putAll(fields);
        if (error != null) {
            body.put("exceptionType", error.getClass().getName());
            body.put("exceptionMessage", Optional.ofNullable(error.getMessage()).orElse(""));
            body.put("stackTrace", stackTrace(error));
        }
        stream.println(Json.stringify(body));
    }

    private static String stackTrace(Throwable error) {
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    private static void stdLine(boolean error, String line) {
        if (line == null || line.isBlank()) {
            return;
        }
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        fields.put("message", line);
        write(
                error ? ORIGINAL_ERR : ORIGINAL_OUT,
                error ? "ERROR" : "INFO",
                error ? "jvm_stderr" : "jvm_stdout",
                fields,
                null
        );
    }

    private static final class StructuredLineOutputStream extends OutputStream {
        private final boolean error;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);

        private StructuredLineOutputStream(boolean error) {
            this.error = error;
        }

        @Override
        public synchronized void write(int value) {
            if (value == '\n') {
                flushLine();
                return;
            }
            if (value != '\r') {
                buffer.write(value);
            }
        }

        @Override
        public synchronized void write(byte[] bytes, int offset, int length) {
            for (int index = 0; index < length; index++) {
                write(bytes[offset + index]);
            }
        }

        @Override
        public synchronized void flush() throws IOException {
            flushLine();
        }

        private void flushLine() {
            if (buffer.size() == 0) {
                return;
            }
            String line = buffer.toString(StandardCharsets.UTF_8);
            buffer.reset();
            stdLine(error, line);
        }
    }

    private record LogIdentity(String env, String group, String system, String namespace) {
        static LogIdentity fromEnv(String defaultSystem) {
            return new LogIdentity(
                    env("LOG_ENV", env("APP_ENV", env("ENV", "local"))),
                    env("LOG_GROUP", env("GROUP", "arrow-flight")),
                    env("LOG_SYSTEM", env("SYSTEM", defaultSystem)),
                    env("LOG_NAMESPACE", env("POD_NAMESPACE", env("NAMESPACE", "local")))
            );
        }

        void putInto(LinkedHashMap<String, Object> body) {
            body.put("env", env);
            body.put("group", group);
            body.put("system", system);
            body.put("namespace", namespace);
        }

        private static String env(String key, String defaultValue) {
            String value = System.getenv(key);
            return value == null || value.isBlank() ? defaultValue : value.trim();
        }
    }
}
