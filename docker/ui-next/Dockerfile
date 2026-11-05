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
#   docker build --pull -t conductor:ui-next -f docker/ui-next/Dockerfile .
#
# Note: the Freshworks conductor:server image bundles legacy ui/ (tenant UI).
# Use this image to run ui-next separately (e.g. sandbox evaluation).
#
# Security / scan scope: only the final stage (nginx) is shipped to ECR and scanned.
# Do not revert to nginx:stable-alpine (pulls curl, libxml2, libX11, etc.) or node:lts
# (tag drift; Node 24+ images may omit corepack and break the build).
#

# ===========================================================================================================
# 0. Build stage (discarded — not present in the runtime image)
# ===========================================================================================================
ARG NODE_IMAGE=node:22-bookworm
FROM ${NODE_IMAGE} AS builder
LABEL maintainer="Freshworks <search-team@freshworks.com>"

RUN corepack enable

COPY ui-next /conductor/ui-next
WORKDIR /conductor/ui-next

# Feature flags are baked at build time (see ui-next/src/utils/flags.ts, vite.config.ts).
# Default true for Freshworks sandbox; pass --build-arg REACT_APP_FEATURE_TENANT_SELECTION=false to disable.
ARG REACT_APP_FEATURE_TENANT_SELECTION=true
ENV REACT_APP_FEATURE_TENANT_SELECTION=${REACT_APP_FEATURE_TENANT_SELECTION}

RUN pnpm install --frozen-lockfile && \
    NODE_OPTIONS=--max-old-space-size=4096 pnpm build

# ===========================================================================================================
# ===========================================================================================================
# Slim: core nginx + envsubst only — not the full stable-alpine bundle (xslt, curl, image-filter, …).
FROM nginx:stable-alpine-slim
LABEL maintainer="Freshworks <search-team@freshworks.com>"

# Apply Alpine CVE fixes at build time; use docker build --pull for a fresh nginx base.
RUN apk update && apk upgrade --no-cache \
    && apk add --no-cache --upgrade \
        nginx \
        pcre2 \
        zlib \
        busybox \
        libcrypto3 \
        libssl3

# Upstream conductor server for /api and /actuator; override at runtime.
ENV WF_SERVER=http://conductor-server:8080

COPY --from=builder /conductor/ui-next/dist /usr/share/nginx/html
# The official nginx image envsubst's templates into /etc/nginx/conf.d at start.
COPY docker/ui-next/nginx.conf.template /etc/nginx/templates/default.conf.template

EXPOSE 5000
