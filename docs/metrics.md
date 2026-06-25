# Infra metrics

Machine and scheduling metrics for the infra components. Jobs never instrument
themselves for these — the worker observes containers from outside, and the
coordinator measures scheduling. (Job-emitted telemetry — progress, loss, events —
is a separate path: it goes to the coordinator and shows on `GetJobStatus`;
training metric *history* goes to MLflow.)

## Enabling / disabling

The control-plane stacks are toggled in one place: **`control_plane_config.yaml`** at
the repo root.

```yaml
mlflow:
  enabled: true     # MLflow + PostgreSQL (experiment tracking)
metrics:
  enabled: true     # Prometheus + Grafana (this page)
```

MinIO (object store) is always on. Start/stop via the launcher, which derives
compose profiles and variables from the config (the compose file is the
template — no generated file to maintain):

```bash
scripts/control-plane.sh up      # logs the fully resolved compose to
                                 # .control-plane-resolved.yml for debugging
scripts/control-plane.sh status
scripts/control-plane.sh down
```

The coordinator reads the same file for its own settings, located via the
`CONTROL_PLANE_CONFIG` env var:
`CONTROL_PLANE_CONFIG=control_plane_config.yaml java -jar scheduler-coordinator.jar`.

| UI | URL |
|----|-----|
| Prometheus | http://localhost:9095 |
| Grafana (dashboard "Scheduler", anonymous admin) | http://localhost:3000 |

Endpoints scraped (see `metrics/prometheus.yml`):
- Coordinator: `:9091/metrics` (always served; gRPC port + 1)
- Worker: `:9092/metrics` (always served, fixed port; a failed bind logs a warning and the worker keeps running)

The endpoints are always on; enabling/disabling the `metrics` profile only
controls whether Prometheus/Grafana run to scrape and display them.

## Coordinator metrics (`:9091/metrics`)

| Metric | Type | Meaning |
|--------|------|---------|
| `scheduler_jobs_submitted_total` | counter | Jobs accepted by submit |
| `scheduler_jobs_finished_total{status}` | counter | Terminal outcomes (completed/failed/killed/cancelled); `rate()` = throughput |
| `scheduler_telemetry_reports_total` | counter | Job telemetry reports forwarded by workers |
| `scheduler_worker_heartbeat_losses_total` | counter | Workers evicted after heartbeat timeout |
| `scheduler_job_queue_wait_seconds` | histogram | Submit → claim latency (scheduling delay) |
| `scheduler_jobs{status}` | gauge | Jobs currently known, by status (scrape-time, never stale) |
| `scheduler_queue_depth` | gauge | Jobs waiting to be claimed |
| `scheduler_workers_registered` | gauge | Workers currently registered |

## Worker metrics (`:9092/metrics`)

| Metric | Type | Meaning |
|--------|------|---------|
| `scheduler_worker_jobs_running` | gauge | Jobs currently executing on this worker |
| `scheduler_worker_job_duration_seconds{outcome}` | histogram | Wall-clock job duration by outcome |
| `scheduler_job_container_cpu_percent{job_id,job_name}` | gauge | Job container CPU, from `docker stats` (10s sampling) |
| `scheduler_job_container_memory_used_bytes{job_id,job_name}` | gauge | Job container memory, from `docker stats` |
| `scheduler_gpu_utilization_percent{gpu}` | gauge | Host GPU utilization, from `nvidia-smi` (only when the worker is configured `resources.gpu: true`) |
| `scheduler_gpu_memory_used_bytes{gpu}` | gauge | Host GPU memory in use |

Per-job container series are removed when the job finishes, so finished jobs
don't linger as stale gauges.

## Planned

- `scheduler_job_stalled{job_id}` on the coordinator — RUNNING with no telemetry
  report past a threshold (pairs with the SDK liveness ping).
