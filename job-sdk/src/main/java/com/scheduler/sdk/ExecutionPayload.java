package com.scheduler.sdk;

import java.io.*;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * The bridge that passes context from the worker JVM to the job process.
 * Carries the worker agent URL (so the job can POST status updates back),
 * the job ID, and user-supplied parameters.
 *
 * <pre>
 * Worker JVM                                       Job process (child JVM)
 * ──────────                                       ───────────────────────
 * WorkerAgent.executeJob()                         _Harness.main(args)
 *   │                                                │
 *   ├─ new ExecutionPayload(url, jobId, params)      │
 *   ├─ payload.encode() → base64 string              │
 *   ├─ WorkerAgent passes it as args[0]  ─────────►  ExecutionPayload.decode(args[0])
 *   │                                                ├─ .workerAgentUrl() → JobReporter
 *   │                                                ├─ .jobId()
 *   │                                                └─ .param("region", String.class)
 * </pre>
 *
 * <p>Both sides share this class via {@code job-sdk}, so we use Java serialization directly.
 */
public record ExecutionPayload(String workerAgentUrl, String jobId, Map<String, String> params) implements Serializable {

    public static final String FIELD_WORKER_AGENT_URL = "workerAgentUrl";
    public static final String FIELD_JOB_ID = "jobId";

    public ExecutionPayload {
        Objects.requireNonNull(workerAgentUrl, FIELD_WORKER_AGENT_URL);
        Objects.requireNonNull(jobId, FIELD_JOB_ID);
        params = Map.copyOf(params);
    }

    /** Encodes this payload as a base64 string suitable for passing as a CLI argument. */
    public String encode() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(this);
            }
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode payload", e);
        }
    }

    /** Decodes a base64-encoded payload from {@code args[0]}. */
    public static ExecutionPayload decode(String base64) {
        try {
            byte[] bytes = Base64.getDecoder().decode(base64);
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return (ExecutionPayload) ois.readObject();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decode payload", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("ExecutionPayload class not found on classpath", e);
        }
    }

    /** Returns a typed parameter value with coercion for String, int, long, double, boolean. */
    @SuppressWarnings("unchecked")
    public <T> T param(String name, Class<T> type) {
        String value = params.get(name);
        if (value == null) {
            return null;
        }
        Object result;
        if (type == String.class) {
            result = value;
        } else if (type == int.class || type == Integer.class) {
            result = Integer.parseInt(value);
        } else if (type == long.class || type == Long.class) {
            result = Long.parseLong(value);
        } else if (type == double.class || type == Double.class) {
            result = Double.parseDouble(value);
        } else if (type == boolean.class || type == Boolean.class) {
            result = Boolean.parseBoolean(value);
        } else {
            throw new IllegalArgumentException("Unsupported param type: " + type.getName());
        }
        return (T) result;
    }
}
