package com.scheduler.coordinator;

import com.scheduler.core.api.JobManager;
import com.scheduler.coordinator.client.UserClientHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);
    private static final int DEFAULT_PORT = 9090;

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        JobManager jobManager = new JobManagerImpl();
        UserClientHandler clientHandler = new UserClientHandler(jobManager);
        WorkerHandler workerHandler = new WorkerHandler(jobManager);

        Server server = ServerBuilder.forPort(port)
                .addService(clientHandler)
                .addService(workerHandler)
                .build()
                .start();

        log.info("Coordinator started on port {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down coordinator");
            server.shutdown();
        }));

        server.awaitTermination();
    }
}
