# Docker — Conductor UI (ui-next)

Builds **ui-next** and serves the static bundle with **nginx**. Use this image when running the modern UI separately from the Java server (Freshworks Jenkins job / ECR `conductor-ui-next`).

Legacy tenant UI (`ui/`, CRA dev server) lives in **`docker/ui/Dockerfile`**.

## Build

From the repository root:

```bash
docker build -f docker/ui-next/Dockerfile -t conductor:ui-next .
```

Tenant selection is **on by default** in this Dockerfile (Freshworks sandbox). To disable:

```bash
docker build -f docker/ui-next/Dockerfile \
  --build-arg REACT_APP_FEATURE_TENANT_SELECTION=false \
  -t conductor:ui-next .
```

Runtime fallback: `ui-next/public/context.js` sets `TENANT_SELECTION: true` for auth-proxy deployments.

## Run

- Local conductor server: `docker run -p 5000:5000 -d conductor:ui-next`
- External server: `docker run -p 5000:5000 -d -e WF_SERVER=http://conductor-server:8080 conductor:ui-next`
