#
# conductor:ui-next - Conductor UI (ui-next)
#
# Standalone UI image: builds ui-next and serves the static bundle with nginx.
# API requests are proxied to the conductor server; point the container at
# your server with:
#
#   docker run -e WF_SERVER=http://my-conductor:8080 -p 5000:5000 conductor:ui-next
#
# Build from the repo root:
#
#   docker build -t conductor:ui-next -f docker/ui-next/Dockerfile .
#
# Note: the Freshworks conductor:server image bundles legacy ui/ (tenant UI).
# Use this image to run ui-next separately (e.g. sandbox evaluation).
#

# ===========================================================================================================
# 0. Build stage
# ===========================================================================================================
FROM node:lts AS builder
LABEL maintainer="Freshworks <search-team@freshworks.com>"

COPY ui-next /conductor/ui-next
WORKDIR /conductor/ui-next

# Feature flags are baked at build time (see ui-next/src/utils/flags.ts, vite.config.ts).
# Default true for Freshworks sandbox; pass --build-arg REACT_APP_FEATURE_TENANT_SELECTION=false to disable.
ARG REACT_APP_FEATURE_TENANT_SELECTION=true
ENV REACT_APP_FEATURE_TENANT_SELECTION=${REACT_APP_FEATURE_TENANT_SELECTION}

RUN corepack enable && \
    pnpm install --frozen-lockfile && \
    NODE_OPTIONS=--max-old-space-size=4096 pnpm build

# ===========================================================================================================
# 1. Serve stage
# ===========================================================================================================
FROM nginx:stable-alpine
LABEL maintainer="Freshworks <search-team@freshworks.com>"

# Upstream conductor server for /api and /actuator; override at runtime.
ENV WF_SERVER=http://conductor-server:8080

COPY --from=builder /conductor/ui-next/dist /usr/share/nginx/html
# The official nginx image envsubst's templates into /etc/nginx/conf.d at start.
COPY docker/ui-next/nginx.conf.template /etc/nginx/templates/default.conf.template

EXPOSE 5000
