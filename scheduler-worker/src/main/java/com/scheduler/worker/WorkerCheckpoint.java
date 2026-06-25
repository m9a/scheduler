package com.scheduler.worker;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * The worker's local state file (worker_checkpoint.yaml) — distinct from the
 * user-authored worker_config.yaml. It holds the stable {@code workerId} so the
 * worker keeps one identity across restarts, which lets the coordinator
 * reconcile its jobs after either side restarts (see task #14).
 *
 * <p>One worker agent runs per host, so the id is just a generated UUID — there
 * is no host/machine-id derivation. The file is created on first boot when
 * absent; the path comes from {@code worker_config.yaml} (required, no default).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkerCheckpoint {

    private static final Logger log = LoggerFactory.getLogger(WorkerCheckpoint.class);
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private String workerId;

    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }

    /**
     * Returns the persisted workerId, generating and writing one when the file is
     * absent or has no id. Throws if the file can't be read or written — identity
     * is required, so we fail fast rather than run with an unstable id.
     */
    static String resolveOrCreate(Path path) {
        try {
            if (Files.isRegularFile(path)) {
                WorkerCheckpoint checkpoint = YAML.readValue(path.toFile(), WorkerCheckpoint.class);
                if (checkpoint.workerId != null && !checkpoint.workerId.isBlank()) {
                    log.info("Loaded workerId={} from checkpoint {}", checkpoint.workerId, path);
                    return checkpoint.workerId;
                }
            }
            WorkerCheckpoint checkpoint = new WorkerCheckpoint();
            checkpoint.workerId = UUID.randomUUID().toString();
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            YAML.writeValue(path.toFile(), checkpoint);
            log.info("Generated workerId={} and wrote checkpoint {}", checkpoint.workerId, path);
            return checkpoint.workerId;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to resolve worker checkpoint at " + path, e);
        }
    }
}
