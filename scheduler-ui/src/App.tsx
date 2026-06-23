import { useState } from "react";
import { JobsView } from "./components/JobsView.tsx";
import { WorkersView } from "./components/WorkersView.tsx";
import { MetricsView } from "./components/MetricsView.tsx";

type Tab = "jobs" | "workers" | "metrics";

// Top-level shell: a tab switcher over the three read-only views. State stays in
// the URL hash so a view (and a selected job) survives reload and is linkable.
export function App() {
  const [tab, setTab] = useState<Tab>(() => (location.hash.slice(1) as Tab) || "jobs");

  const select = (t: Tab) => {
    setTab(t);
    location.hash = t;
  };

  return (
    <div className="app">
      <header>
        <h1>Scheduler</h1>
        <nav>
          <button className={tab === "jobs" ? "active" : ""} onClick={() => select("jobs")}>Jobs</button>
          <button className={tab === "workers" ? "active" : ""} onClick={() => select("workers")}>Workers</button>
          <button className={tab === "metrics" ? "active" : ""} onClick={() => select("metrics")}>Metrics</button>
        </nav>
      </header>
      <main>
        {tab === "jobs" && <JobsView />}
        {tab === "workers" && <WorkersView />}
        {tab === "metrics" && <MetricsView />}
      </main>
    </div>
  );
}
