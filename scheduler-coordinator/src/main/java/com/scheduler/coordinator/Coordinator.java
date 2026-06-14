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
     * Usage: {@code coordinator [--config <control-plane.yaml>] [port]}.
     * Settings come from control-plane.yaml (defaults apply without one); a bare
     * port argument overrides the configured port (used by scheduler-cli).
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        CoordinatorConfig config = new CoordinatorConfig();
        Integer portOverride = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--config") && i + 1 < args.length) {
                config = CoordinatorConfig.load(java.nio.file.Path.of(args[++i]));
            } else {
                portOverride = Integer.parseInt(args[i]);
            }
        }
        int port = portOverride != null ? portOverride : config.getCoordinator().getPort();
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
