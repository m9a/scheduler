import { api, type JobDetail as JobDetailData } from "../api.ts";
import { usePolling } from "../usePolling.ts";
import { fmtAgo, fmtTime, shortState, stateClass } from "../format.ts";

const POLL_MS = 2000;

// Detail pane for one job: lifecycle, failure info, liveness, and per-task table.
// Polls faster than the list since this is the focused view.
export function JobDetail({ jobId, onClose }: { jobId: string; onClose: () => void }) {
  const { data: job, error } = usePolling<JobDetailData>(
    () => api.getJob(jobId),
    POLL_MS,
    [jobId],
  );

  if (error) return <p className="error">Failed to load job: {error}</p>;
  if (!job) return <p className="muted">Loading…</p>;

  return (
    <div className="job-detail">
      <div className="detail-head">
        <h2>{job.name} <span className={`badge ${stateClass(job.state)}`}>{shortState(job.state)}</span></h2>
        <button className="close" onClick={onClose}>✕</button>
      </div>
      <dl className="meta">
        <dt>ID</dt><dd>{job.id}</dd>
        <dt>Created</dt><dd>{fmtTime(job.createdAt)}</dd>
        <dt>Started</dt><dd>{fmtTime(job.startedAt)}</dd>
        <dt>Completed</dt><dd>{fmtTime(job.completedAt)}</dd>
        <dt>Last activity</dt>
        <dd>{job.lastActivityMs ? fmtAgo(new Date(job.lastActivityMs).toISOString()) : "—"}</dd>
        {job.failureReason && (<><dt>Failure</dt><dd className="state-bad">{shortState(job.failureReason)}{job.failureDetail ? `: ${job.failureDetail}` : ""}</dd></>)}
      </dl>

      <h3>Tasks</h3>
      {job.tasks.length === 0 ? (
        <p className="muted">No tasks reported yet.</p>
      ) : (
        <table>
          <thead>
            <tr><th>#</th><th>Name</th><th>State</th><th>Started</th><th>Completed</th><th>Exit</th></tr>
          </thead>
          <tbody>
            {job.tasks.map((t) => (
              <tr key={t.taskIndex}>
                <td>{t.taskIndex}</td>
                <td>{t.taskName}{t.errorMessage && <div className="error small">{t.errorMessage}</div>}</td>
                <td><span className={`badge ${stateClass(t.state)}`}>{shortState(t.state)}</span></td>
                <td>{fmtTime(t.startedAt)}</td>
                <td>{fmtTime(t.completedAt)}</td>
                <td>{t.exitCode ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
