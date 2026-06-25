# scheduler-ui

Read-only monitoring UI for the scheduler control plane. A React + TypeScript
single-page app (built with Vite) that **polls** the coordinator's HTTP read API
and renders jobs, tasks, and worker health. Metrics are delegated to embedded
Grafana — this UI does not draw charts.

There is no write path: you cannot submit/kill/retry jobs from here (by design,
v1). Live follow stays on the gRPC client library, not this UI.

## How it talks to the backend

The **coordinator serves both the UI and the API** from one HTTP server (port
8080): static files at `/`, the read API at `/api/*`. Because files and data
share one origin, the browser needs no CORS, and there's no reverse proxy in the
request path.

```
prod:  browser ──HTTPS──► Caddy (TLS only) ──► coordinator :8080  (serves UI + /api)
dev:   browser ──────────► Vite dev server :5173 ──/api──► coordinator :8080
```

- **Prod:** the built `dist/` is handed to the coordinator (config
  `coordinator.uiDir`), which serves it. Caddy does **only TLS** — the same job
  it does for every other service, no special routing.
- **Dev:** Vite hosts the UI with hot reload and forwards `/api/*` to the
  coordinator (override target with `COORDINATOR_HTTP`). No prefix rewrite — the
  coordinator already serves under `/api`.

The JSON shapes in `src/api.ts` mirror `ReadApiServer` in
`scheduler-coordinator` — keep them in sync.

## Develop

Requires Node 18+ (not yet installed on this machine).

```bash
npm install
npm run dev      # dev server on http://localhost:5173, proxying /api → :8080
npm run build    # static build into dist/
```

## Build the bundle without local Node (Docker)

`Dockerfile` builds the bundle with Node only inside a build stage, then exports
just `dist/`. `build-dist.sh` writes it to the host:

```bash
./build-dist.sh         # → ./dist  (no Node needed locally)
```

## Wire dist/ into the coordinator

The coordinator serves whatever directory `coordinator.uiDir` points at (empty →
API-only). Two flows:

- **Local (CLI launches the coordinator as a host process):** build `dist/` as
  above, then set `uiDir` to its absolute path in your `control_plane_config.yaml`
  (the file at `CONTROL_PLANE_CONFIG`; the CLI preserves your edits):

  ```yaml
  coordinator:
    uiDir: /Users/you/src/scheduler/scheduler-ui/dist
  ```

- **Prod (coordinator runs as a container):** COPY the bundle into the
  coordinator image from this UI image and point `uiDir` at it:

  ```dockerfile
  COPY --from=scheduler-ui /dist /opt/scheduler/ui   # uiDir: /opt/scheduler/ui
  ```

Either way the coordinator serves the UI at `/` and the API at `/api/*` — same
origin, no proxy. Caddy only adds TLS in prod.

## Build-time config (optional)

- `VITE_API_BASE` — override the API base (default `/api`).
- `VITE_GRAFANA_URL` — Grafana dashboard URL to embed in the Metrics tab.
