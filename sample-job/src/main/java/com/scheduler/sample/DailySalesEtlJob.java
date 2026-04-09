package com.scheduler.sample;

import com.scheduler.annotation.AfterJob;
import com.scheduler.annotation.BeforeJob;
import com.scheduler.annotation.Context;
import com.scheduler.annotation.Job;
import com.scheduler.annotation.Param;
import com.scheduler.annotation.ResourceProfile;
import com.scheduler.annotation.Task;
import com.scheduler.sdk.JobContext;

/**
 * Sample annotated job demonstrating the annotation-based API.
 * Simulates a daily sales ETL pipeline: extract → transform → load.
 */
@Job(id = "daily-sales-etl",
     description = "Daily sales ETL pipeline",
     timeoutSeconds = 3600,
     maxRetries = 2,
     resource = @ResourceProfile(minMemoryMb = 512, cpuCores = 2))
public class DailySalesEtlJob {

    private final String region;
    private final int batchSize;

    public DailySalesEtlJob(@Param("region") String region,
                            @Param(value = "batchSize", defaultValue = "1000") int batchSize) {
        this.region = region;
        this.batchSize = batchSize;
    }

    @BeforeJob
    public void setup() {
        System.out.println("Setting up ETL for region=" + region + ", batchSize=" + batchSize);
    }

    @Task(name = "extract", order = 1, critical = true)
    public void extract(@Context JobContext ctx) {
        System.out.println("Extracting sales data for region=" + region);
        ctx.put("rowCount", 5000);
    }

    @Task(name = "transform", order = 2, dependsOn = "extract")
    public void transform(@Context JobContext ctx) {
        Integer rowCount = ctx.get("rowCount", Integer.class);
        System.out.println("Transforming " + rowCount + " rows in batches of " + batchSize);
    }

    @Task(name = "load", order = 3, dependsOn = "transform")
    public void load(@Context JobContext ctx) {
        Integer rowCount = ctx.get("rowCount", Integer.class);
        System.out.println("Loading " + rowCount + " rows into warehouse");
    }

    @AfterJob
    public void cleanup() {
        System.out.println("Cleaning up temporary files");
    }
}
