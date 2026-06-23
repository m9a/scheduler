// Typed client for the coordinator's pull-only HTTP read API. The shapes here
// mirror the hand-mapped JSON in ReadApiServer (scheduler-coordinator) — keep
// them in sync with that serializer, which is the contract's source of truth.
// All calls go through /api, which Vite (dev) and Caddy (prod) proxy to the
// coordinator's HTTP port.

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

// State strings are the proto enum names emitted by the coordinator
// (e.g. "JOB_STATE_RUNNING", "TASK_STATE_COMPLETED").
export type JobSummary = {
  id: string;
  name: string;
  state: string;
  createdAt: string | null;
  startedAt: string | null;
  completedAt: string | null;
  taskCounts: Record<string, number>;
};

export type TaskStatus = {
  taskIndex: number;
  taskName: string;
  state: string;
  startedAt: string | null;
  completedAt: string | null;
  errorMessage: string | null;
  exitCode: number | null;
};

export type JobDetail = JobSummary & {
  failureReason: string | null;
  failureDetail: string | null;
  lastActivityMs: number | null;
  tasks: TaskStatus[];
};

export type Worker = {
  id: string;
  hostname: string;
  memoryMb: number;
  cpuCores: number;
  gpu: boolean;
  capabilities: string[];
  registeredAt: string | null;
  lastHeartbeat: string | null;
};

async function get<T>(path: string): Promise<T> {
  const resp = await fetch(`${BASE}${path}`);
  if (!resp.ok) {
    // Surface the coordinator's {"error": ...} body when present.
    let detail = resp.statusText;
    try {
      const body = await resp.json();
      if (body?.error) detail = body.error;
    } catch {
      // non-JSON error body — fall back to statusText
    }
    throw new Error(`${resp.status}: ${detail}`);
  }
  return resp.json() as Promise<T>;
}

export const api = {
  listJobs: () => get<JobSummary[]>("/jobs"),
  getJob: (id: string) => get<JobDetail>(`/jobs/${encodeURIComponent(id)}`),
  listWorkers: () => get<Worker[]>("/workers"),
};
