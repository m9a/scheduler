package com.scheduler.coordinator;

import com.scheduler.coordinator.client.UserRequestHandler;
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
    private static final int DEFAULT_PORT = 9090;
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration HEARTBEAT_SCAN_INTERVAL = Duration.ofSeconds(5);

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        ObjectStore objectStore = createObjectStore();
        JobManagerImpl jobManager = new JobManagerImpl();
        UserRequestHandler clientHandler = new UserRequestHandler(jobManager, objectStore);
        WorkerHandler workerHandler = new WorkerHandler(jobManager);
        workerHandler.startHeartbeatMonitor(HEARTBEAT_TIMEOUT, HEARTBEAT_SCAN_INTERVAL);

        Server server = ServerBuilder.forPort(port)
                .addService(clientHandler)
                .addService(workerHandler)
                .build()
                .start();

        log.info("Coordinator started on port {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down coordinator");
            workerHandler.shutdownHeartbeatMonitor();
            server.shutdown();
        }));

        server.awaitTermination();
    }

    private static ObjectStore createObjectStore() {
        String endpoint = System.getProperty("minio.endpoint", "http://localhost:9000");
        String accessKey = System.getProperty("minio.accessKey", "minioadmin");
        String secretKey = System.getProperty("minio.secretKey", "minioadmin");
        String bucket = System.getProperty("minio.bucket", "scheduler");

        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();

        return new ObjectStore(s3, bucket);
    }
}
