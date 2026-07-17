package com.scheduler.worker;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkerConfig {

    private Coordinator coordinator = new Coordinator();
    private String hostname;
    private int port;
    // Port for the Prometheus /metrics endpoint; 0 disables it. The default matches
    // the control plane's scrape config (metrics/prometheus.yml).
    private int metricsPort = 9092;
    // Path to this worker's checkpoint file (worker_checkpoint.yaml) — holds the
    // stable workerId. Required: no default, the worker fails fast if it's absent.
    private String checkpointPath;
    // Path to the worker's durable status store (SQLite) — mirrors the status it forwards
    // so a coordinator that missed updates during an outage re-learns state on reconnect.
    private String statusDbPath;
    // How long a terminal-unacked status row survives before the retention sweep drops it
    // (guards against a permanently-dead coordinator growing the store forever).
    private int statusRetentionDays = 7;
    private Resources resources = new Resources();
    private Docker docker = new Docker();
    private Minio minio = new Minio();
    private Mlflow mlflow = new Mlflow();
    private Liveness liveness = new Liveness();

    public static WorkerConfig load(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(path.toFile(), WorkerConfig.class);
    }

    /** Fails fast at startup if a required setting is missing or non-positive. */
    public void validate() {
        require(coordinator.host != null && !coordinator.host.isBlank(), "coordinator.host");
        require(coordinator.heartbeatIntervalSeconds > 0, "coordinator.heartbeatIntervalSeconds");
        require(coordinator.pollIntervalSeconds > 0, "coordinator.pollIntervalSeconds");
        require(hostname != null && !hostname.isBlank(), "hostname");
        require(checkpointPath != null && !checkpointPath.isBlank(), "checkpointPath");
        require(statusDbPath != null && !statusDbPath.isBlank(), "statusDbPath");
        require(statusRetentionDays > 0, "statusRetentionDays");
        require(metricsPort > 0, "metricsPort");
        require(docker.imagePullTimeoutMinutes > 0, "docker.imagePullTimeoutMinutes");
        require(resources.memory > 0, "resources.memory");
        require(resources.cpu > 0, "resources.cpu");
        require(minio.endpoint != null && !minio.endpoint.isBlank(), "minio.endpoint");
        require(minio.bucket != null && !minio.bucket.isBlank(), "minio.bucket");
    }

    private static void require(boolean ok, String field) {
        if (!ok) {
            throw new IllegalArgumentException("worker config: missing or invalid '" + field + "'");
        }
    }

    public Coordinator getCoordinator() { return coordinator; }
    public void setCoordinator(Coordinator coordinator) { this.coordinator = coordinator; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public int getMetricsPort() { return metricsPort; }
    public void setMetricsPort(int metricsPort) { this.metricsPort = metricsPort; }

    public String getCheckpointPath() { return checkpointPath; }
    public void setCheckpointPath(String checkpointPath) { this.checkpointPath = checkpointPath; }

    public String getStatusDbPath() { return statusDbPath; }
    public void setStatusDbPath(String statusDbPath) { this.statusDbPath = statusDbPath; }

    public int getStatusRetentionDays() { return statusRetentionDays; }
    public void setStatusRetentionDays(int v) { this.statusRetentionDays = v; }

    public Resources getResources() { return resources; }
    public void setResources(Resources resources) { this.resources = resources; }

    public Docker getDocker() { return docker; }
    public void setDocker(Docker docker) { this.docker = docker; }

    public Minio getMinio() { return minio; }
    public void setMinio(Minio minio) { this.minio = minio; }

    public Mlflow getMlflow() { return mlflow; }
    public void setMlflow(Mlflow mlflow) { this.mlflow = mlflow; }

    public Liveness getLiveness() { return liveness; }
    public void setLiveness(Liveness liveness) { this.liveness = liveness; }

    /**
     * Stall detection: the container must show activity within {@code startupTimeoutSeconds}
     * of launch, then keep pinging — {@code maxMissedPings} consecutive missed
     * {@code pingIntervalSeconds} windows mark it unresponsive and (when {@code autoKill})
     * the worker gracefully terminates it. The interval should match the SDK's ping rate.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Liveness {
        private int startupTimeoutSeconds = 30;
        private int pingIntervalSeconds = 15;
        private int maxMissedPings = 3;
        private boolean autoKill = true;
        // SIGTERM → SIGKILL grace for the graceful stop, giving @OnShutdown time to run.
        private int shutdownGraceSeconds = 10;

        public int getStartupTimeoutSeconds() { return startupTimeoutSeconds; }
        public void setStartupTimeoutSeconds(int v) { this.startupTimeoutSeconds = v; }
        public int getPingIntervalSeconds() { return pingIntervalSeconds; }
        public void setPingIntervalSeconds(int v) { this.pingIntervalSeconds = v; }
        public int getMaxMissedPings() { return maxMissedPings; }
        public void setMaxMissedPings(int v) { this.maxMissedPings = v; }
        public boolean isAutoKill() { return autoKill; }
        public void setAutoKill(boolean v) { this.autoKill = v; }
        public int getShutdownGraceSeconds() { return shutdownGraceSeconds; }
        public void setShutdownGraceSeconds(int v) { this.shutdownGraceSeconds = v; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Coordinator {
        private String host;
        private int port;
        // Heartbeat send period; keep below the coordinator's heartbeatTimeoutSeconds.
        private int heartbeatIntervalSeconds = 5;
        // How often the worker polls the coordinator for a job to claim.
        private int pollIntervalSeconds = 5;
        // Retry period after the command streams drop: re-register + resubscribe.
        // Generous — the heartbeat grace after a coordinator restart is minutes.
        private int reconnectRetrySeconds = 60;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public int getHeartbeatIntervalSeconds() { return heartbeatIntervalSeconds; }
        public void setHeartbeatIntervalSeconds(int v) { this.heartbeatIntervalSeconds = v; }
        public int getPollIntervalSeconds() { return pollIntervalSeconds; }
        public void setPollIntervalSeconds(int v) { this.pollIntervalSeconds = v; }
        public int getReconnectRetrySeconds() { return reconnectRetrySeconds; }
        public void setReconnectRetrySeconds(int v) { this.reconnectRetrySeconds = v; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Resources {
        private int memory;
        private int cpu;
        private boolean gpu;
        private List<String> capabilities;

        public int getMemory() { return memory; }
        public void setMemory(int memory) { this.memory = memory; }
        public int getCpu() { return cpu; }
        public void setCpu(int cpu) { this.cpu = cpu; }
        public boolean isGpu() { return gpu; }
        public void setGpu(boolean gpu) { this.gpu = gpu; }
        public List<String> getCapabilities() { return capabilities; }
        public void setCapabilities(List<String> capabilities) { this.capabilities = capabilities; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Docker {
        private String network;
        // Bound for `docker run -d`, which includes pulling the image. On expiry
        // the job fails (PROCESS_START_FAILED) — the container never started.
        private int imagePullTimeoutMinutes = 10;

        public String getNetwork() { return network; }
        public void setNetwork(String network) { this.network = network; }
        public int getImagePullTimeoutMinutes() { return imagePullTimeoutMinutes; }
        public void setImagePullTimeoutMinutes(int v) { this.imagePullTimeoutMinutes = v; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Minio {
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private String bucket;

        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }
        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Mlflow {
        private String trackingUri;

        public String getTrackingUri() { return trackingUri; }
        public void setTrackingUri(String trackingUri) { this.trackingUri = trackingUri; }
    }
}
