package com.scheduler.worker;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class JobLivenessMonitorTest {

    // Null coordinator → the monitor skips the liveness report and only exercises detection.

    @Test
    void killsOnStartupTimeout() throws InterruptedException {
        CountDownLatch killed = new CountDownLatch(1);
        JobLivenessMonitor.Config cfg = new JobLivenessMonitor.Config(200, 100, 2, true);
        JobLivenessMonitor m = new JobLivenessMonitor("j", null, cfg, killed::countDown);
        m.start();

        assertTrue(killed.await(3, TimeUnit.SECONDS), "a container that never pings must be killed");
        assertTrue(m.isUnresponsive());
        m.close();
    }

    @Test
    void killsAfterMissedPings() throws InterruptedException {
        CountDownLatch killed = new CountDownLatch(1);
        JobLivenessMonitor.Config cfg = new JobLivenessMonitor.Config(5000, 100, 2, true);
        JobLivenessMonitor m = new JobLivenessMonitor("j", null, cfg, killed::countDown);
        m.recordActivity();  // first ping arrived → steady state, then goes silent
        m.start();

        assertTrue(killed.await(3, TimeUnit.SECONDS), "silence past maxMissedPings must be killed");
        m.close();
    }

    @Test
    void healthyJobNotKilled() throws InterruptedException {
        CountDownLatch killed = new CountDownLatch(1);
        JobLivenessMonitor.Config cfg = new JobLivenessMonitor.Config(5000, 100, 3, true);
        JobLivenessMonitor m = new JobLivenessMonitor("j", null, cfg, killed::countDown);
        m.start();

        for (int i = 0; i < 8; i++) {  // keep pinging — stays under maxMissedPings * interval
            m.recordActivity();
            Thread.sleep(80);
        }
        assertFalse(m.isUnresponsive(), "a pinging job must not be flagged unresponsive");
        assertEquals(1, killed.getCount());
        m.close();
    }

    @Test
    void autoKillDisabled() throws InterruptedException {
        CountDownLatch killed = new CountDownLatch(1);
        JobLivenessMonitor.Config cfg = new JobLivenessMonitor.Config(100, 50, 2, false);
        JobLivenessMonitor m = new JobLivenessMonitor("j", null, cfg, killed::countDown);
        m.start();

        assertFalse(killed.await(400, TimeUnit.MILLISECONDS), "autoKill=false must never kill");
        assertFalse(m.isUnresponsive());
        m.close();
    }
}
