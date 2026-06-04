package com.scheduler.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scheduler.coordinator.JobManagerImpl;
import com.scheduler.coordinator.client.UserRequestHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.core.ObjectStore;
import com.scheduler.client.SchedulerClient;
import com.scheduler.proto.v1.*;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end integration test. Brings up infrastructure via docker-compose
 * (MinIO, PostgreSQL, MLflow) and a local Docker registry, builds real images
 * from scheduler-sdk, then runs the full client → coordinator → worker →
 * container pipeline.
 *
 * <pre>
 * @BeforeAll — infrastructure:
 *   docker-compose.test.yml:
 *     ├── MinIO      (:9000, :9001)  S3-compatible object store
 *     ├── PostgreSQL  (:5432)        MLflow backend store
 *     └── MLflow      (:5000)        experiment tracking
 *   registry:2       (:5050)         local Docker registry on scheduler-net
 *   Coordinator      (gRPC, ephemeral port)
 *   WorkerAgent      (background thread, connects to coordinator)
 *
 *   docker build + push from scheduler-sdk:
 *     |-- sample-job               (Java)
 *     |-- sample-py-job            (Python)
 *     |-- sample-pytorch-job       (PyTorch training)
 *     |-- sample-py-training-job   (Lightning + MLflow metrics)
 *     '-- sample-inference-job     (HTTP inference server)
 *
 * Test flow:
 *   ClientStub --SubmitJob--&gt; Coordinator --PullJob--&gt; WorkerAgent
 *                                                          |
 *     docker run --name job-{id} --network scheduler-net ...
 *                                                          |
 *     Container --POST /task-status--&gt; WorkerAgent --gRPC--&gt; Coordinator
 *                                                          |
 *   ClientStub &lt;--GetJobStatus--- Coordinator              |
 *                                                          |
 *     WorkerAgent uploads /workspace/output --&gt; MinIO (object store)
 *
 * Test ordering:
 *   1. testJavaDockerJob        — batch job, 3 tasks
 *   2. testPythonDockerJob      — batch job, 3 tasks
 *   3. testPytorchDockerJob     — trains LSTM, uploads model.pt
 *   4. testInferenceDockerJob   — loads model.pt, serves HTTP, verifies predictions
 *   5. testLightningTrainingJob — Lightning MNIST, verifies MLflow metrics
 *   6. testTaskFailureJava      — Java job where second task throws
 *   7. testTaskFailurePython    — Python job where second task raises
 *   8. testWorkerHeartbeatLost  — coordinator detects dead worker, fails job
 * </pre>
 *
 * Skips transparently when Docker is unavailable — no POM config needed.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(IntegrationTest.class);
    private static final String REGISTRY_PREFIX = "localhost:5050";

    private static final Path SDK_REPO_PATH = Path.of("../../scheduler-sdk");
    private static final Path COMPOSE_FILE = Path.of("../../scheduler/docker-compose.test.yml");

    private static final String MINIO_URL = "http://localhost:9000";
    private static final String MLFLOW_URL = "http://localhost:5000";

    private static String registryContainerName;
    private static ObjectStore objectStore;

    private static Server coordinatorServer;
    private static SchedulerClient client;
    private static WorkerAgent workerAgent;
    private static Thread workerThread;
    private static String trainingJobId;
    private static final List<String> jobContainerNames = new ArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        boolean dockerReady = dockerAvailable();
        if (!dockerReady) {
            log.error("Docker is not available — skipping all integration tests. "
                    + "Start Docker Desktop and re-run.");
        }
        assumeTrue(dockerReady, "Docker not available — skipping Docker integration tests");

        log.info("Using scheduler-sdk at {}", SDK_REPO_PATH);

        assertSdkPrerequisites();
        startComposeStack();
        startRegistry();
        createSchedulerBucket();
        buildAndPushImages();
        startCoordinatorAndWorker();
    }

    @AfterAll
    static void tearDown() {
        removeJobContainers();
        stopWorker();
        shutdownCoordinator();
        removeRegistry();
        stopComposeStack();
        removeTestImages();
    }

    @Test
    @Order(1)
    void testJavaDockerJob() throws Exception {
        Job submitted = client.submitJob("docker-java-job",
                REGISTRY_PREFIX + "/sample-job:latest",
                Map.of("region", "us", "batchSize", "100"));

        String jobId = submitted.getId();
        assertFalse(jobId.isEmpty(), "Expected a job ID");
        assertEquals(JobStatus.JOB_STATUS_QUEUED, submitted.getStatus());

        Job completed = client.waitForCompletion(jobId, Duration.ofSeconds(60));
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, completed.getStatus());
        assertTrue(completed.getStartedAtMillis() > 0, "Expected startedAt to be set");
        assertTrue(completed.getCompletedAtMillis() > 0, "Expected completedAt to be set");
    }

    @Test
    @Order(2)
    void testPythonDockerJob() throws Exception {
        Job submitted = client.submitJob("docker-python-job",
                REGISTRY_PREFIX + "/sample-py-job:latest",
                Map.of("region", "eu", "batch_size", "500"));

        String jobId = submitted.getId();
        assertFalse(jobId.isEmpty(), "Expected a job ID");
        assertEquals(JobStatus.JOB_STATUS_QUEUED, submitted.getStatus());

        Job completed = client.waitForCompletion(jobId, Duration.ofSeconds(60));
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, completed.getStatus());
    }

    @Test
    @Order(3)
    void testPytorchDockerJob() throws Exception {
        // Generate sine wave CSV data
        StringBuilder csv = new StringBuilder("index,value\n");
        for (int i = 0; i < 200; i++) {
            csv.append(i).append(",").append(Math.sin(i * 0.1)).append("\n");
        }

        SubmitJobResponse submitResponse = client.submitJob(SubmitJobRequest.newBuilder()
                .setName("docker-pytorch-job")
                .setArtifactUri(REGISTRY_PREFIX + "/sample-pytorch-job:latest")
                .putAllParams(Map.of("epochs", "5", "hidden_size", "16"))
                .addInputFiles(com.scheduler.proto.v1.InputFile.newBuilder()
                        .setName("sine_data.csv")
                        .setContent(ByteString.copyFromUtf8(csv.toString()))
                        .build())
                .build());

        String jobId = submitResponse.getJob().getId();
        trainingJobId = jobId;
        assertFalse(jobId.isEmpty(), "Expected a job ID");
        assertEquals(JobStatus.JOB_STATUS_QUEUED, submitResponse.getJob().getStatus());

        // PyTorch install + training takes longer than the simple jobs
        Job completed = client.waitForCompletion(jobId, Duration.ofSeconds(120));
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, completed.getStatus());

        // Verify task states: both completed with real names from the @task decorators
        assertEquals(2, completed.getTasksCount());
        for (Task task : completed.getTasksList()) {
            assertEquals(TaskStatus.TASK_STATUS_COMPLETED, task.getStatus());
            assertFalse(task.getName().startsWith("task-"),
                    "Expected real task name, got " + task.getName());
        }

        // Verify model.pt exists in output files
        List<OutputFile> files = client.listJobFiles(jobId);
        boolean hasModel = files.stream()
                .anyMatch(f -> f.getName().endsWith("model.pt"));
        assertTrue(hasModel, "Expected model.pt in output files, got: " + files);
    }

    @Test
    @Order(4)
    void testInferenceDockerJob() throws Exception {
        assertNotNull(trainingJobId, "pytorchDockerJob must run first to produce trainingJobId");

        SubmitJobResponse submitResponse = client.submitJob(SubmitJobRequest.newBuilder()
                .setName("docker-inference-job")
                .setArtifactUri(REGISTRY_PREFIX + "/sample-inference-job:latest")
                .putAllParams(Map.of(
                        "hidden_size", "16",
                        "port", "8080",
                        "containerPort", "8080"))
                .addInputFiles(com.scheduler.proto.v1.InputFile.newBuilder()
                        .setName("model.pt")
                        .setUri("jobs/" + trainingJobId + "/output/model.pt")
                        .build())
                .build());

        String jobId = submitResponse.getJob().getId();
        jobContainerNames.add("job-" + jobId);
        assertFalse(jobId.isEmpty(), "Expected a job ID");

        // Wait for the server container to start
        pollUntilRunning(jobId, 120, TimeUnit.SECONDS);

        int hostPort = parseHostPort("job-" + jobId, 8080);
        log.info("Inference server mapped to host port {}", hostPort);

        pollUntilHealthy(hostPort, 30, TimeUnit.SECONDS);

        // Send a prediction request with a sine wave sequence
        double[] sequence = new double[10];
        for (int i = 0; i < 10; i++) {
            sequence[i] = Math.sin(i * 0.1);
        }

        ObjectMapper mapper = new ObjectMapper();
        String predictBody = mapper.writeValueAsString(Map.of("sequence", sequence));

        HttpURLConnection predictConn = (HttpURLConnection) URI.create(
                "http://localhost:" + hostPort + "/predict").toURL().openConnection();
        predictConn.setRequestMethod("POST");
        predictConn.setRequestProperty("Content-Type", "application/json");
        predictConn.setDoOutput(true);
        try (OutputStream os = predictConn.getOutputStream()) {
            os.write(predictBody.getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(200, predictConn.getResponseCode(), "Expected 200 from /predict");
        String predictResponse = new String(predictConn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        log.info("Prediction response: {}", predictResponse);
        assertTrue(predictResponse.contains("\"prediction\""),
                "Expected prediction field in response: " + predictResponse);

        // Trigger clean shutdown
        HttpURLConnection shutdownConn = (HttpURLConnection) URI.create(
                "http://localhost:" + hostPort + "/shutdown").toURL().openConnection();
        shutdownConn.setRequestMethod("POST");
        shutdownConn.setRequestProperty("Content-Length", "0");
        shutdownConn.setDoOutput(true);
        assertEquals(200, shutdownConn.getResponseCode(), "Expected 200 from /shutdown");

        // The server exits, task completes, job finishes normally
        Job completed = client.waitForCompletion(jobId, Duration.ofSeconds(60));
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, completed.getStatus());

        // Verify predictions.jsonl was uploaded to the object store
        List<OutputFile> files = client.listJobFiles(jobId);
        boolean hasPredictions = files.stream()
                .anyMatch(f -> f.getName().endsWith("predictions.jsonl"));
        assertTrue(hasPredictions, "Expected predictions.jsonl in output files, got: " + files);
    }

    /**
     * Submits a Lightning training job (MNIST, 2 epochs) with three tasks:
     * train → test → export. Verifies all tasks complete, that the exported
     * model.pt is uploaded to object storage, and that training metrics
     * (train_loss, val_loss, val_acc, test_loss, test_acc) are logged in MLflow.
     */
    @Test
    @Order(5)
    @SuppressWarnings("unchecked")
    void testLightningTrainingJob() throws Exception {
        Job submitted = client.submitJob("docker-lightning-job",
                REGISTRY_PREFIX + "/sample-py-training-job:latest",
                Map.of("epochs", "2", "batch_size", "128"));

        String jobId = submitted.getId();
        assertFalse(jobId.isEmpty(), "Expected a job ID");
        assertEquals(JobStatus.JOB_STATUS_QUEUED, submitted.getStatus());

        // Lightning + MNIST download + training — needs a generous timeout
        Job completed = client.waitForCompletion(jobId, Duration.ofSeconds(300));
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, completed.getStatus(),
                "Lightning training job should complete successfully");

        // All three tasks should have completed: train, test, export
        assertEquals(3, completed.getTasksCount(), "Expected 3 tasks");
        assertEquals("train", completed.getTasks(0).getName());
        assertEquals(TaskStatus.TASK_STATUS_COMPLETED, completed.getTasks(0).getStatus());
        assertEquals("test", completed.getTasks(1).getName());
        assertEquals(TaskStatus.TASK_STATUS_COMPLETED, completed.getTasks(1).getStatus());
        assertEquals("export", completed.getTasks(2).getName());
        assertEquals(TaskStatus.TASK_STATUS_COMPLETED, completed.getTasks(2).getStatus());

        // Verify model.pt was exported and uploaded
        List<OutputFile> files = client.listJobFiles(jobId);
        boolean hasModel = files.stream()
                .anyMatch(f -> f.getName().endsWith("model.pt"));
        assertTrue(hasModel, "Expected model.pt in output files, got: " + files);

        // Verify MLflow experiment exists
        ObjectMapper mapper = new ObjectMapper();
        HttpURLConnection expConn = (HttpURLConnection) URI.create(
                MLFLOW_URL + "/api/2.0/mlflow/experiments/get-by-name?experiment_name=mnist-lightning"
        ).toURL().openConnection();
        expConn.setConnectTimeout(5000);
        expConn.setReadTimeout(5000);
        assertEquals(200, expConn.getResponseCode(), "MLflow experiment 'mnist-lightning' should exist");
        String expBody = new String(expConn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, Object> expJson = mapper.readValue(expBody, Map.class);
        Map<String, Object> experiment = (Map<String, Object>) expJson.get("experiment");
        String experimentId = (String) experiment.get("experiment_id");
        assertNotNull(experimentId, "Expected experiment_id");

        // Search for runs in this experiment
        HttpURLConnection runConn = (HttpURLConnection) URI.create(
                MLFLOW_URL + "/api/2.0/mlflow/runs/search"
        ).toURL().openConnection();
        runConn.setRequestMethod("POST");
        runConn.setRequestProperty("Content-Type", "application/json");
        runConn.setDoOutput(true);
        String searchBody = mapper.writeValueAsString(Map.of("experiment_ids", List.of(experimentId)));
        try (OutputStream os = runConn.getOutputStream()) {
            os.write(searchBody.getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(200, runConn.getResponseCode(), "MLflow run search should succeed");
        String runBody = new String(runConn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, Object> runJson = mapper.readValue(runBody, Map.class);
        List<Map<String, Object>> runs = (List<Map<String, Object>>) runJson.get("runs");
        assertFalse(runs.isEmpty(), "Expected at least one MLflow run");

        // Verify training metrics were logged
        Map<String, Object> firstRun = runs.get(0);
        Map<String, Object> data = (Map<String, Object>) firstRun.get("data");
        List<Map<String, Object>> metrics = (List<Map<String, Object>>) data.get("metrics");
        assertNotNull(metrics, "Expected metrics in run data");

        List<String> metricKeys = new ArrayList<>();
        for (Map<String, Object> m : metrics) {
            metricKeys.add((String) m.get("key"));
        }
        for (String expected : List.of("train_loss", "val_loss", "val_acc", "test_loss", "test_acc")) {
            assertTrue(metricKeys.contains(expected),
                    "Expected metric '" + expected + "' in MLflow run, got: " + metricKeys);
        }
    }

    @Test
    @Order(6)
    void testTaskFailureJava() throws Exception {
        Job submitted = client.submitJob("docker-java-failing-job",
                REGISTRY_PREFIX + "/sample-failing-job:latest", Map.of());

        String jobId = submitted.getId();
        assertFalse(jobId.isEmpty(), "Expected a job ID");

        Job finalJob = client.waitForCompletion(jobId, Duration.ofSeconds(60));
        assertEquals(JobStatus.JOB_STATUS_FAILED, finalJob.getStatus());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_EXITED, finalJob.getFailureReason());
        assertFalse(finalJob.getErrorMessage().isEmpty(), "Expected error_message for backward compat");

        // First task (validate) should have completed
        assertTrue(finalJob.getTasksCount() >= 2, "Expected at least 2 tasks reported");
        assertEquals(TaskStatus.TASK_STATUS_COMPLETED, finalJob.getTasks(0).getStatus());
        assertEquals("validate", finalJob.getTasks(0).getName());

        // Second task (process) should have failed
        assertEquals(TaskStatus.TASK_STATUS_FAILED, finalJob.getTasks(1).getStatus());
        assertEquals("process", finalJob.getTasks(1).getName());
    }

    @Test
    @Order(7)
    void testTaskFailurePython() throws Exception {
        Job submitted = client.submitJob("docker-python-failing-job",
                REGISTRY_PREFIX + "/sample-py-failing-job:latest", Map.of());

        String jobId = submitted.getId();
        assertFalse(jobId.isEmpty(), "Expected a job ID");

        Job finalJob = client.waitForCompletion(jobId, Duration.ofSeconds(60));
        assertEquals(JobStatus.JOB_STATUS_FAILED, finalJob.getStatus());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_EXITED, finalJob.getFailureReason());
        assertFalse(finalJob.getErrorMessage().isEmpty(), "Expected error_message for backward compat");

        // First task (setup_data) should have completed
        assertTrue(finalJob.getTasksCount() >= 2, "Expected at least 2 tasks reported");
        assertEquals(TaskStatus.TASK_STATUS_COMPLETED, finalJob.getTasks(0).getStatus());
        assertEquals("setup_data", finalJob.getTasks(0).getName());

        // Second task (process) should have failed
        assertEquals(TaskStatus.TASK_STATUS_FAILED, finalJob.getTasks(1).getStatus());
        assertEquals("process", finalJob.getTasks(1).getName());
    }

    /**
     * Verifies the full heartbeat-loss path end-to-end: worker dies → coordinator
     * detects via heartbeat monitor → job marked FAILED with HEARTBEAT_LOST →
     * client sees the failure via GetJobStatus.
     *
     * <p>Uses its own coordinator with heartbeat monitoring enabled and a
     * TestableWorkerAgent that blocks spawn, since the shared coordinator
     * does not have heartbeat monitoring.
     */
    @Test
    @Order(8)
    void testWorkerHeartbeatLost() throws Exception {
        // Stand up a coordinator with aggressive heartbeat settings (2s timeout, 500ms scan)
        JobManagerImpl hbJobManager = new JobManagerImpl();
        WorkerHandler hbWorkerHandler = new WorkerHandler(hbJobManager);
        hbWorkerHandler.startHeartbeatMonitor(Duration.ofSeconds(2), Duration.ofMillis(500));

        Server hbServer = ServerBuilder.forPort(0)
                .addService(new UserRequestHandler(hbJobManager, null))
                .addService(hbWorkerHandler)
                .build()
                .start();

        SchedulerClient hbClient = SchedulerClient.builder()
                .host("localhost")
                .port(hbServer.getPort())
                .deadline(Duration.ofSeconds(30))
                .maxRetries(0)
                .build();

        // Worker that blocks the spawn so the job stays in-flight while we kill heartbeats
        TestableWorkerAgent hbWorker = new TestableWorkerAgent(testConfig(hbServer.getPort()));
        CountDownLatch spawnBlocked = new CountDownLatch(1);
        hbWorker.setSpawnBehavior((details, url) -> {
            spawnBlocked.await();
            return 0;
        });

        String jobId;
        try {
            Job submitted = hbClient.submitJob("heartbeat-loss-test", "test-image:latest", Map.of());
            jobId = submitted.getId();

            Thread hbWorkerThread = new Thread(hbWorker::run);
            hbWorkerThread.setDaemon(true);
            hbWorkerThread.start();

            // Wait for the job to be claimed (status becomes STARTING)
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                Job polled = hbClient.getJobStatus(jobId);
                if (polled.getStatus() != JobStatus.JOB_STATUS_QUEUED) {
                    break;
                }
                Thread.sleep(100);
            }

            // Kill the worker — heartbeats stop while spawn is still blocked
            hbWorker.close();

            // Wait for the heartbeat monitor to detect the dead worker and fail the job
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Job job = null;
            while (System.nanoTime() < deadline) {
                job = hbClient.getJobStatus(jobId);
                if (job.getStatus() == JobStatus.JOB_STATUS_FAILED) {
                    break;
                }
                Thread.sleep(100);
            }

            assertNotNull(job);
            assertEquals(JobStatus.JOB_STATUS_FAILED, job.getStatus());
            assertEquals(FailureReason.FAILURE_REASON_HEARTBEAT_LOST, job.getFailureReason());
            assertTrue(job.getErrorMessage().contains("heartbeat"),
                    "Expected error_message to mention heartbeat, got: " + job.getErrorMessage());
            assertTrue(job.getCompletedAtMillis() > 0, "Expected completedAt to be set");

            // Unblock spawn so the worker thread can finish
            spawnBlocked.countDown();
            hbWorkerThread.join(5000);
        } finally {
            hbWorkerHandler.shutdownHeartbeatMonitor();
            hbClient.close();
            hbServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @FunctionalInterface
    interface SpawnBehavior {
        int execute(JobDetails details, String workerAgentUrl) throws IOException, InterruptedException;
    }

    /**
     * WorkerAgent subclass that overrides spawnJobProcess to run test-controlled
     * logic instead of launching a real Docker container.
     */
    static class TestableWorkerAgent extends WorkerAgent {
        private volatile SpawnBehavior spawnBehavior;

        TestableWorkerAgent(WorkerConfig config) throws IOException {
            super(config, null, Duration.ofSeconds(10));
        }

        void setSpawnBehavior(SpawnBehavior behavior) {
            this.spawnBehavior = behavior;
        }

        @Override
        int spawnJobProcess(JobDetails details, Path inputDir, Path outputDir, Path logFile,
                            Map<String, String> params) throws IOException, InterruptedException {
            return spawnBehavior.execute(details, workerAgentUrl());
        }
    }

    // -- lifecycle helpers --

    private static boolean dockerAvailable() {
        try {
            Process process = new ProcessBuilder("docker", "info")
                    .redirectErrorStream(true)
                    .start();
            boolean finished = process.waitFor(10, TimeUnit.SECONDS);
            if (!finished) {
                log.warn("'docker info' timed out after 10s — Docker daemon may be unresponsive");
                process.destroyForcibly();
                return false;
            }
            process.getInputStream().readAllBytes();
            if (process.exitValue() != 0) {
                log.warn("'docker info' exited with code {} — Docker daemon not running", process.exitValue());
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("Docker availability check failed: {}", e.getMessage());
            return false;
        }
    }

    private static void assertSdkPrerequisites() {
        Path sampleJobJar = SDK_REPO_PATH.resolve("sample-job/target/sample-job-1.0-SNAPSHOT.jar");
        if (!Files.exists(sampleJobJar)) {
            log.error("sample-job JAR not found at {} — run 'mvn package -pl sample-job -am' in the SDK repo first",
                    sampleJobJar);
        }
        assumeTrue(Files.exists(sampleJobJar),
                "sample-job JAR not found at " + sampleJobJar + " — run 'mvn package -pl sample-job -am' in the SDK repo first");

        Path failingJobJar = SDK_REPO_PATH.resolve("sample-failing-job/target/sample-failing-job-1.0-SNAPSHOT.jar");
        if (!Files.exists(failingJobJar)) {
            log.error("sample-failing-job JAR not found at {} — run 'mvn package -pl sample-failing-job -am' in the SDK repo first",
                    failingJobJar);
        }
        assumeTrue(Files.exists(failingJobJar),
                "sample-failing-job JAR not found at " + failingJobJar + " — run 'mvn package -pl sample-failing-job -am' in the SDK repo first");
    }

    private static void startComposeStack() throws Exception {
        log.info("Starting docker-compose stack from {}", COMPOSE_FILE.toAbsolutePath());
        runCommand("docker", "compose", "-f", COMPOSE_FILE.toAbsolutePath().toString(), "up", "-d");

        boolean minioReady = pollUrl(MINIO_URL + "/minio/health/ready", 60);
        assertTrue(minioReady, "MinIO did not become ready");

        // MLflow installs psycopg2-binary + boto3 on first start
        boolean mlflowReady = pollUrl(MLFLOW_URL + "/health", 120);
        assertTrue(mlflowReady, "MLflow did not become ready");

        log.info("Docker-compose stack ready");
    }

    private static void stopComposeStack() {
        try {
            runCommand("docker", "compose", "-f", COMPOSE_FILE.toAbsolutePath().toString(), "down", "-v");
        } catch (Exception e) {
            log.warn("Failed to stop compose stack: {}", e.getMessage());
        }
    }

    private static void startRegistry() throws Exception {
        registryContainerName = "test-registry-" + ProcessHandle.current().pid();
        runCommand("docker", "run", "-d", "-p", "5050:5000",
                "--network", "scheduler-net",
                "--name", registryContainerName, "registry:2");

        boolean ready = pollUrl("http://localhost:5050/v2/", 30);
        assertTrue(ready, "Registry did not become ready");
        log.info("Registry ready at localhost:5050");
    }

    private static void createSchedulerBucket() {
        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(MINIO_URL))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("minioadmin", "minioadmin")))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();

        // minio-init in docker-compose may have already created the bucket
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket("scheduler").build());
            log.info("Created 'scheduler' bucket");
        } catch (Exception e) {
            log.info("Bucket 'scheduler' already exists: {}", e.getMessage());
        }

        objectStore = new ObjectStore(s3, "scheduler");
    }

    /**
     * Parses the host port that Docker mapped for a given container port
     * using {@code docker port <container> <containerPort>}.
     */
    private static int parseHostPort(String containerName, int containerPort) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("docker", "port", containerName, String.valueOf(containerPort));
        pb.redirectErrorStream(true);
        Process process = pb.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("docker port failed: " + output);
        }
        // Output format: "0.0.0.0:32768" or "[::]:32768" — take last line, split on ':'
        String lastLine = output.lines().reduce((a, b) -> b).orElse(output);
        String portStr = lastLine.substring(lastLine.lastIndexOf(':') + 1);
        return Integer.parseInt(portStr);
    }

    private static void buildAndPushImages() throws Exception {
        // Java image — build context is sample-job/ directory
        String javaImage = REGISTRY_PREFIX + "/sample-job:latest";
        runCommand("docker", "build", "-t", javaImage,
                "-f", SDK_REPO_PATH.resolve("sample-job/Dockerfile").toString(),
                SDK_REPO_PATH.resolve("sample-job").toString());
        runCommand("docker", "push", javaImage);

        // Python image — build context is SDK root (needs py-sdk/ and sample-py-job/)
        String pythonImage = REGISTRY_PREFIX + "/sample-py-job:latest";
        runCommand("docker", "build", "-t", pythonImage,
                "-f", SDK_REPO_PATH.resolve("sample-py-job/Dockerfile").toString(),
                SDK_REPO_PATH.toString());
        runCommand("docker", "push", pythonImage);

        // PyTorch image — build context is SDK root (needs py-sdk/ and sample-pytorch-job/)
        String pytorchImage = REGISTRY_PREFIX + "/sample-pytorch-job:latest";
        runCommand("docker", "build", "-t", pytorchImage,
                "-f", SDK_REPO_PATH.resolve("sample-pytorch-job/Dockerfile").toString(),
                SDK_REPO_PATH.toString());
        runCommand("docker", "push", pytorchImage);

        // Lightning training image — build context is SDK root (needs py-sdk/ and sample-py-training-job/)
        String lightningImage = REGISTRY_PREFIX + "/sample-py-training-job:latest";
        runCommand("docker", "build", "-t", lightningImage,
                "-f", SDK_REPO_PATH.resolve("sample-py-training-job/Dockerfile").toString(),
                SDK_REPO_PATH.toString());
        runCommand("docker", "push", lightningImage);

        // Inference server image — build context is SDK root (needs py-sdk/ and sample-inference-job/)
        String inferenceImage = REGISTRY_PREFIX + "/sample-inference-job:latest";
        runCommand("docker", "build", "-t", inferenceImage,
                "-f", SDK_REPO_PATH.resolve("sample-inference-job/Dockerfile").toString(),
                SDK_REPO_PATH.toString());
        runCommand("docker", "push", inferenceImage);

        // Java failing job — build context is sample-failing-job/ directory
        String javaFailingImage = REGISTRY_PREFIX + "/sample-failing-job:latest";
        runCommand("docker", "build", "-t", javaFailingImage,
                "-f", SDK_REPO_PATH.resolve("sample-failing-job/Dockerfile").toString(),
                SDK_REPO_PATH.resolve("sample-failing-job").toString());
        runCommand("docker", "push", javaFailingImage);

        // Python failing job — build context is SDK root (needs py-sdk/ and sample-py-failing-job/)
        String pyFailingImage = REGISTRY_PREFIX + "/sample-py-failing-job:latest";
        runCommand("docker", "build", "-t", pyFailingImage,
                "-f", SDK_REPO_PATH.resolve("sample-py-failing-job/Dockerfile").toString(),
                SDK_REPO_PATH.toString());
        runCommand("docker", "push", pyFailingImage);
    }

    private static void startCoordinatorAndWorker() throws Exception {
        JobManagerImpl jobManager = new JobManagerImpl();

        coordinatorServer = ServerBuilder.forPort(0)
                .addService(new UserRequestHandler(jobManager, objectStore))
                .addService(new WorkerHandler(jobManager))
                .build()
                .start();

        client = SchedulerClient.builder()
                .host("localhost")
                .port(coordinatorServer.getPort())
                .deadline(Duration.ofSeconds(30))
                .maxRetries(0)
                .build();

        // hostname = host.docker.internal so containers can POST status back to the host
        WorkerConfig workerConfig = testConfig(coordinatorServer.getPort());
        workerConfig.getWorker().setHostname("host.docker.internal");
        workerConfig.getDocker().setNetwork("scheduler-net");
        workerConfig.getMlflow().setTrackingUri("http://mlflow:5000");
        workerAgent = new WorkerAgent(workerConfig, objectStore, Duration.ofSeconds(120));
        workerThread = new Thread(workerAgent::run);
        workerThread.start();
    }

    private static void stopWorker() {
        if (workerAgent != null) {
            try {
                workerAgent.stop();
                workerThread.interrupt();
                workerThread.join(5000);
                workerAgent.close();
            } catch (Exception e) {
                log.warn("Error stopping worker: {}", e.getMessage());
            }
        }
    }

    private static void shutdownCoordinator() {
        try {
            if (client != null) {
                client.close();
            }
            if (coordinatorServer != null) {
                coordinatorServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.warn("Error shutting down coordinator: {}", e.getMessage());
        }
    }

    private static void removeJobContainers() {
        for (String name : jobContainerNames) {
            runQuietly("docker", "rm", "-f", "-v", name);
        }
    }

    private static void removeRegistry() {
        if (registryContainerName != null) {
            runQuietly("docker", "rm", "-f", "-v", registryContainerName);
        }
    }

    /**
     * Removes all Docker images built during the test run — both the tagged
     * sample images and the base images they pulled. Without this, each run
     * leaks several GB of images that accumulate and fill the disk.
     */
    private static void removeTestImages() {
        // Tagged images built by buildAndPushImages
        String[] testImages = {
                REGISTRY_PREFIX + "/sample-job:latest",
                REGISTRY_PREFIX + "/sample-py-job:latest",
                REGISTRY_PREFIX + "/sample-pytorch-job:latest",
                REGISTRY_PREFIX + "/sample-py-training-job:latest",
                REGISTRY_PREFIX + "/sample-inference-job:latest",
                REGISTRY_PREFIX + "/sample-failing-job:latest",
                REGISTRY_PREFIX + "/sample-py-failing-job:latest",
        };
        for (String image : testImages) {
            runQuietly("docker", "rmi", "-f", image);
        }

        // Infrastructure images (registry is standalone, the rest are from docker-compose)
        runQuietly("docker", "rmi", "-f", "registry:2");
        runQuietly("docker", "rmi", "-f", "minio/minio");
        runQuietly("docker", "rmi", "-f", "minio/mc");
        runQuietly("docker", "rmi", "-f", "postgres:16");
        runQuietly("docker", "rmi", "-f", "ghcr.io/mlflow/mlflow:v2.16.0");

        // Prune dangling images (intermediate build layers)
        runQuietly("docker", "image", "prune", "-f");
    }

    // -- config --

    private static WorkerConfig testConfig(int coordinatorPort) {
        WorkerConfig config = WorkerConfig.defaults();
        config.getCoordinator().setPort(coordinatorPort);
        return config;
    }

    // -- polling --

    private void pollUntilRunning(String jobId, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        JobStatus status = JobStatus.JOB_STATUS_UNSPECIFIED;

        while (System.nanoTime() < deadline) {
            Job job = client.getJobStatus(jobId);
            status = job.getStatus();

            if (status == JobStatus.JOB_STATUS_RUNNING) {
                return;
            }
            if (status == JobStatus.JOB_STATUS_COMPLETED || status == JobStatus.JOB_STATUS_FAILED
                    || status == JobStatus.JOB_STATUS_KILLED) {
                fail("Job reached terminal status before RUNNING: " + status);
            }
            Thread.sleep(500);
        }

        fail("Job did not reach RUNNING within timeout. Last status: " + status);
    }

    private static void pollUntilHealthy(int port, long timeout, TimeUnit unit) throws Exception {
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        while (System.nanoTime() < deadline) {
            try {
                HttpURLConnection conn = (HttpURLConnection) URI.create(
                        "http://localhost:" + port + "/health").toURL().openConnection();
                conn.setConnectTimeout(1000);
                conn.setReadTimeout(1000);
                if (conn.getResponseCode() == 200) {
                    log.info("Inference server healthy on port {}", port);
                    return;
                }
            } catch (IOException ignored) {
            }
            Thread.sleep(500);
        }

        fail("Inference server did not become healthy within timeout");
    }

    // -- URL polling helpers (used by startComposeStack / startRegistry) --

    private static boolean pollUrl(String url, int timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            try {
                if (httpGet(url) == 200) return true;
            } catch (Exception ignored) {
            }
            try { Thread.sleep(1000); } catch (InterruptedException e) { return false; }
        }
        return false;
    }

    private static int httpGet(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        conn.setRequestMethod("GET");
        return conn.getResponseCode();
    }

    // -- command helpers --

    private static void runCommand(String... command) throws Exception {
        log.info("Running: {}", String.join(" ", command));
        ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true);
        Process process = pb.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.info("  {}", line);
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Command failed (exit " + exitCode + "): " + String.join(" ", command));
        }
    }

    private static void runQuietly(String... command) {
        try {
            ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true);
            Process process = pb.start();
            process.getInputStream().readAllBytes();
            process.waitFor();
        } catch (Exception ignored) {
        }
    }
}
