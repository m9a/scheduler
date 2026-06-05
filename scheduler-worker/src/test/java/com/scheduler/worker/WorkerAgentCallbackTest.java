package com.scheduler.worker;

import com.scheduler.proto.v1.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class WorkerAgentCallbackTest {

    private WorkerAgent agent;
    private List<StatusUpdate> receivedUpdates;

    @BeforeEach
    void setUp() throws IOException {
        receivedUpdates = Collections.synchronizedList(new ArrayList<>());
        Path configPath = Path.of(getClass().getClassLoader().getResource("config.yaml").getPath());
        WorkerConfig config = WorkerConfig.load(configPath);
        agent = new WorkerAgent(config, null, java.time.Duration.ofSeconds(10));
        agent.onStatusUpdate(receivedUpdates::add);
    }

    @AfterEach
    void tearDown() throws Exception {
        agent.close();
    }

    @Test
    void receiveUpdate() throws Exception {
        byte[] proto = com.scheduler.proto.job.StatusUpdate.newBuilder()
                .setJobId("job-1")
                .setTaskIndex(0)
                .setTaskName("extract")
                .setTaskStatus(TaskStatus.TASK_STATUS_RUNNING)
                .build()
                .toByteArray();

        sendWebSocketBinary(agent.workerAgentUrl(), prefixed(WorkerAgent.TYPE_TAG_STATUS, proto));

        assertEquals(1, receivedUpdates.size());
        assertEquals("job-1", receivedUpdates.get(0).jobId());
        assertEquals("extract", receivedUpdates.get(0).taskName());
        assertEquals("RUNNING", receivedUpdates.get(0).taskStatus());
    }

    /** Prepends a one-byte type tag to a proto payload, matching the SDK wire format. */
    private static byte[] prefixed(byte typeTag, byte[] proto) {
        byte[] framed = new byte[proto.length + 1];
        framed[0] = typeTag;
        System.arraycopy(proto, 0, framed, 1, proto.length);
        return framed;
    }

    /**
     * Opens a WebSocket connection, sends a single binary message, then closes.
     * Blocks until the server has processed the message.
     */
    private static void sendWebSocketBinary(String wsUrl, byte[] data) throws Exception {
        CountDownLatch opened = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);

        WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(wsUrl), new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        opened.countDown();
                        WebSocket.Listener.super.onOpen(webSocket);
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                        closed.countDown();
                        return null;
                    }
                }).join();

        assertTrue(opened.await(5, TimeUnit.SECONDS), "WebSocket should open within 5s");
        ws.sendBinary(ByteBuffer.wrap(data), true).join();

        // Brief pause to let the server process the message before closing
        Thread.sleep(100);

        ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
        assertTrue(closed.await(5, TimeUnit.SECONDS), "WebSocket should close within 5s");
    }
}
