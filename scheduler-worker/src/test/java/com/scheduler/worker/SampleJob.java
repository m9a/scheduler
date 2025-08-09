package com.scheduler.worker;

import com.scheduler.annotation.Job;
import com.scheduler.annotation.Task;

/**
 * A minimal annotation-based job used by integration tests. The annotation
 * processor generates {@code SampleJob_Harness} (entry point) and
 * {@code SampleJob_Descriptor} (metadata) at compile time.
 *
 * <p>WorkerAgent spawns this as a child JVM process via the generated harness,
 * which reports task status back to WorkerAgent via HTTP.
 */
@Job(id = "sample-job")
public class SampleJob {

    @Task(name = "step-1", order = 1)
    public void step1() {
        System.out.println("Executing step-1");
    }

    @Task(name = "step-2", order = 2)
    public void step2() {
        System.out.println("Executing step-2");
    }
}
