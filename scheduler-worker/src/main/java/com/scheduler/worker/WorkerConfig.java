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
    private Resources resources = new Resources();
    private Docker docker = new Docker();
    private Minio minio = new Minio();
    private Mlflow mlflow = new Mlflow();

    public static WorkerConfig load(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(path.toFile(), WorkerConfig.class);
    }

    public Coordinator getCoordinator() { return coordinator; }
    public void setCoordinator(Coordinator coordinator) { this.coordinator = coordinator; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public Resources getResources() { return resources; }
    public void setResources(Resources resources) { this.resources = resources; }

    public Docker getDocker() { return docker; }
    public void setDocker(Docker docker) { this.docker = docker; }

    public Minio getMinio() { return minio; }
    public void setMinio(Minio minio) { this.minio = minio; }

    public Mlflow getMlflow() { return mlflow; }
    public void setMlflow(Mlflow mlflow) { this.mlflow = mlflow; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Coordinator {
        private String host;
        private int port;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
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

        public String getNetwork() { return network; }
        public void setNetwork(String network) { this.network = network; }
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
