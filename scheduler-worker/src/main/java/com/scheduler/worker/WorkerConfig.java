package com.scheduler.worker;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * YAML-backed configuration for the worker process.
 * Loaded from a file passed via {@code --config <path>} on the command line.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkerConfig {

    private Coordinator coordinator = new Coordinator();
    private Worker worker = new Worker();
    private Docker docker = new Docker();
    private Minio minio = new Minio();
    private Mlflow mlflow = new Mlflow();

    public static WorkerConfig load(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(path.toFile(), WorkerConfig.class);
    }

    public static WorkerConfig defaults() {
        return new WorkerConfig();
    }

    public Coordinator getCoordinator() { return coordinator; }
    public void setCoordinator(Coordinator coordinator) { this.coordinator = coordinator; }

    public Worker getWorker() { return worker; }
    public void setWorker(Worker worker) { this.worker = worker; }

    public Docker getDocker() { return docker; }
    public void setDocker(Docker docker) { this.docker = docker; }

    public Minio getMinio() { return minio; }
    public void setMinio(Minio minio) { this.minio = minio; }

    public Mlflow getMlflow() { return mlflow; }
    public void setMlflow(Mlflow mlflow) { this.mlflow = mlflow; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Coordinator {
        private String host = "localhost";
        private int port = 9090;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Worker {
        private String hostname = "localhost";
        private int capacity = 1;
        private int memoryMb = 0;
        private int cpuCores = 0;
        private Set<String> capabilities = Set.of();

        public String getHostname() { return hostname; }
        public void setHostname(String hostname) { this.hostname = hostname; }
        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }
        public int getMemoryMb() { return memoryMb; }
        public void setMemoryMb(int memoryMb) { this.memoryMb = memoryMb; }
        public int getCpuCores() { return cpuCores; }
        public void setCpuCores(int cpuCores) { this.cpuCores = cpuCores; }
        public Set<String> getCapabilities() { return capabilities; }
        public void setCapabilities(Set<String> capabilities) { this.capabilities = capabilities; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Docker {
        private String network;

        public String getNetwork() { return network; }
        public void setNetwork(String network) { this.network = network; }
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Mlflow {
        private String trackingUri;

        public String getTrackingUri() { return trackingUri; }
        public void setTrackingUri(String trackingUri) { this.trackingUri = trackingUri; }
    }
}
