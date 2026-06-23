import { useState } from "react";
import { api, type JobSummary } from "../api.ts";
import { usePolling } from "../usePolling.ts";
import { fmtTime, shortState, stateClass } from "../format.ts";
import { JobDetail } from "./JobDetail.tsx";

const POLL_MS = 3000;

// Job list (left) + selected job detail (right). Both poll independently so the
// list keeps refreshing while a detail pane is open.
export function JobsView() {
  const { data: jobs, error, loading } = usePolling<JobSummary[]>(api.listJobs, POLL_MS);
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div className="split">
      <section className="list">
        {error && <p className="error">Failed to load jobs: {error}</p>}
        {loading && !jobs && <p className="muted">Loading…</p>}
        {jobs && jobs.length === 0 && <p className="muted">No jobs submitted.</p>}
        {jobs && jobs.length > 0 && (
          <table>
            <thead>
              <tr><th>Name</th><th>State</th><th>Tasks</th><th>Created</th></tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr
                  key={job.id}
                  className={selected === job.id ? "row selected" : "row"}
                  onClick={() => setSelected(job.id)}
                >
                  <td>{job.name}<div className="muted small">{job.id}</div></td>
                  <td><span className={`badge ${stateClass(job.state)}`}>{shortState(job.state)}</span></td>
                  <td>{taskSummary(job.taskCounts)}</td>
                  <td>{fmtTime(job.createdAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
      <section className="detail">
        {selected ? <JobDetail jobId={selected} onClose={() => setSelected(null)} /> : <p className="muted">Select a job.</p>}
      </section>
    </div>
  );
}

function taskSummary(counts: Record<string, number>): string {
  const entries = Object.entries(counts);
  if (entries.length === 0) return "—";
  return entries.map(([state, n]) => `${shortState(state)}:${n}`).join("  ");
}
