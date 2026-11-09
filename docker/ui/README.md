# Docker — Conductor UI (legacy)

This Dockerfile builds the **legacy `ui/`** image (CRA dev server on port 5000). Freshworks uses it for **conductor-ui** Jenkins → ECR `conductor-ui` (read/write deployments).

**ui-next** (static nginx bundle) is built from **`docker/ui-next/Dockerfile`**.

## Build

From the repository root:

```bash
docker build -f docker/ui/Dockerfile -t conductor:ui .
```

## Run

- With localhost conductor server: `docker run -p 5000:5000 -d -t conductor:ui`
- With external conductor server: `docker run -p 5000:5000 -d -t -e "WF_SERVER=http://conductor-server:8080" conductor:ui`

The infra layer copies `setupProxy.js` for auth-proxy / tenant routing (see `ceapps-infra-templates` `ci/workflow-engine/docker/conductor-ui`).
