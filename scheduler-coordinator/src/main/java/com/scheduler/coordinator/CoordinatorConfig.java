package com.scheduler.coordinator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Coordinator's view of control-plane.yaml — the single control-plane config,
 * shared with scripts/control-plane.sh (which reads the mlflow/metrics stack
 * toggles; the coordinator only reads its own and the minio sections, ignoring
 * the rest). Loaded by {@link Coordinator#main} via {@code --config <path>};
 * field defaults apply when the file or a key is absent.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoordinatorConfig {

    private CoordinatorSettings coordinator = new CoordinatorSettings();
    private Minio minio = new Minio();

    public static CoordinatorConfig load(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(path.toFile(), CoordinatorConfig.class);
    }

    public CoordinatorSettings getCoordinator() { return coordinator; }
    public void setCoordinator(CoordinatorSettings coordinator) { this.coordinator = coordinator; }

    public Minio getMinio() { return minio; }
    public void setMinio(Minio minio) { this.minio = minio; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CoordinatorSettings {
        private int port = 9090;
        private int heartbeatTimeoutSeconds = 15;
        private int heartbeatScanIntervalSeconds = 5;

        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public int getHeartbeatTimeoutSeconds() { return heartbeatTimeoutSeconds; }
        public void setHeartbeatTimeoutSeconds(int v) { this.heartbeatTimeoutSeconds = v; }
        public int getHeartbeatScanIntervalSeconds() { return heartbeatScanIntervalSeconds; }
        public void setHeartbeatScanIntervalSeconds(int v) { this.heartbeatScanIntervalSeconds = v; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Minio {
        private String endpoint = "http://localhost:9000";
        private String accessKey = "minioadmin";
        private String secretKey = "minioadmin";
        private String bucket = "scheduler";

        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }
        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }
    }
}
