package com.scheduler.coordinator;

import com.scheduler.coordinator.client.ClientHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.core.ObjectStore;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;

public class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    /**
     * Config path comes from the {@code CONTROL_PLANE_CONFIG} env var — the single
     * source of truth for all settings, including the port. The process refuses to
     * start if the var is unset/empty or the file fails to parse. No CLI arguments.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        String configPath = System.getenv("CONTROL_PLANE_CONFIG");
        if (configPath == null || configPath.isBlank()) {
            System.err.println("CONTROL_PLANE_CONFIG must point to control-plane.yaml");
            System.exit(1);
            return;
        }
        CoordinatorConfig config;
        try {
            config = CoordinatorConfig.load(java.nio.file.Path.of(configPath));
        } catch (Exception e) {
            System.err.println("Failed to load CONTROL_PLANE_CONFIG=" + configPath + ": " + e.getMessage());
            System.exit(1);
            return;
        }
        log.info("Loaded config from CONTROL_PLANE_CONFIG={}", configPath);
        int port = config.getCoordinator().getPort();
        Duration heartbeatTimeout = Duration.ofSeconds(config.getCoordinator().getHeartbeatTimeoutSeconds());
        Duration heartbeatScanInterval = Duration.ofSeconds(config.getCoordinator().getHeartbeatScanIntervalSeconds());

        ObjectStore objectStore = createObjectStore(config.getMinio());
        JobManager jobManager = new JobManager();
        ClientHandler clientHandler = new ClientHandler(jobManager, objectStore);
        WorkerHandler workerHandler = new WorkerHandler(jobManager);
        workerHandler.startHeartbeatMonitor(heartbeatTimeout, heartbeatScanInterval);

        // Prometheus scrapes gRPC port + 1 (e.g. 9090 → 9091/metrics).
        CoordinatorMetrics.init(jobManager, workerHandler);
        CoordinatorMetrics metrics = new CoordinatorMetrics();
        metrics.startServer(port + 1);

        Server server = ServerBuilder.forPort(port)
                .addService(clientHandler)
                .addService(workerHandler)
                .build()
                .start();

        log.info("Coordinator started on port {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down coordinator");
            workerHandler.shutdownHeartbeatMonitor();
            metrics.stop();
            server.shutdown();
        }));

        server.awaitTermination();
    }

    private static ObjectStore createObjectStore(CoordinatorConfig.Minio minio) {
        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(minio.getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minio.getAccessKey(), minio.getSecretKey())))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();

        return new ObjectStore(s3, minio.getBucket());
    }
}
