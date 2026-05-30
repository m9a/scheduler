package com.scheduler.worker;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * Worker's representation of the payload sent to job containers.
 * Produces base64(JSON) with fields: workerAgentUrl, jobId, params.
 *
 * <p>Both Java and Python job runtimes decode the same base64(JSON) format,
 * so the worker does not need to know which SDK the job uses.
 */
record ExecutionPayload(String workerAgentUrl, String jobId, Map<String, String> params) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    ExecutionPayload {
        Objects.requireNonNull(workerAgentUrl, "workerAgentUrl");
        Objects.requireNonNull(jobId, "jobId");
        params = Map.copyOf(params);
    }

    /** Encodes as base64(JSON) suitable for env vars or CLI arguments. */
    String encode() {
        Map<String, Object> map = Map.of(
                "workerAgentUrl", workerAgentUrl,
                "jobId", jobId,
                "params", params
        );
        try {
            byte[] json = MAPPER.writeValueAsBytes(map);
            return Base64.getEncoder().encodeToString(json);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode payload as JSON", e);
        }
    }
}
