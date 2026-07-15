package com.scheduler.coordinator;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorConfigTest {

    private Path resource(String name) {
        return Path.of("src/test/resources", name);
    }

    @Test
    void load() throws Exception {
        CoordinatorConfig config = CoordinatorConfig.load(resource("control_plane_config.yaml"));

        CoordinatorConfig.CoordinatorSettings coordinator = config.getCoordinator();
        assertEquals(7070, coordinator.getPort());
        assertEquals(30, coordinator.getHeartbeatTimeoutSeconds());
        assertEquals(10, coordinator.getHeartbeatScanIntervalSeconds());

        CoordinatorConfig.Minio minio = config.getMinio();
        assertEquals("http://minio:9000", minio.getEndpoint());
        assertEquals("testkey", minio.getAccessKey());
        assertEquals("testsecret", minio.getSecretKey());
        assertEquals("test-bucket", minio.getBucket());
    }

    // mlflow/metrics sections in the file are not coordinator concerns; loading
    // must skip them rather than fail on unknown properties.
    @Test
    void ignoresUnknownSections() throws Exception {
        assertDoesNotThrow(() -> CoordinatorConfig.load(resource("control_plane_config.yaml")));
    }

    // No file / missing keys must fall back to the in-code defaults.
    @Test
    void defaults() {
        CoordinatorConfig config = new CoordinatorConfig();
        assertEquals(9090, config.getCoordinator().getPort());
        assertEquals(300, config.getCoordinator().getHeartbeatTimeoutSeconds());
        assertEquals(5, config.getCoordinator().getHeartbeatScanIntervalSeconds());
    }
}
