#
# conductor:server - Conductor Server + UI
#
# Build args:
#   PREBUILT=true:  Skips both JAR and UI builds, uses pre-built artifacts (CI multi-arch).
#                   Expects docker/server/libs/conductor-server.jar and
#                   docker/server/libs/opentelemetry-javaagent.jar, plus a built ui/build.
#   SKIP_UI=true:   Skips only the UI build (E2E CI — tests serve UI from the host)
#   YARN_OPTS:      Extra options passed to the yarn install and build commands
#   Default:        Builds JAR and UI from source (docker-compose, local dev)
#
# The bundled UI is the Freshworks customized React app in ui/ (not upstream ui-next).
#

# ===========================================================================================================
# 0. Builder stage
# ===========================================================================================================
FROM azul/zulu-openjdk-debian:21 AS builder

ARG PREBUILT=false
ARG INDEXING_BACKEND=elasticsearch

LABEL maintainer="Orkes OSS <oss@orkes.io>"

COPY . /conductor
WORKDIR /conductor

RUN if [ "$PREBUILT" = "true" ]; then \
      rm -rf server/build/libs && mkdir -p server/build/libs && \
      cp docker/server/libs/conductor-server.jar server/build/libs/conductor-server-boot.jar && \
      if [ -f docker/server/libs/opentelemetry-javaagent.jar ]; then \
        cp docker/server/libs/opentelemetry-javaagent.jar server/build/libs/opentelemetry-javaagent.jar; \
      fi && \
      echo "Using pre-built server JAR"; \
    else \
      ./gradlew build -x test -x spotlessCheck -x shadowJar \
        -PindexingBackend=${INDEXING_BACKEND} \
        -Dorg.gradle.jvmargs=-Xmx2g \
        --no-daemon --no-parallel; \
    fi


# ===========================================================================================================
# 1. UI builder stage
# ===========================================================================================================
FROM node:lts AS ui-builder

ARG PREBUILT=false
ARG SKIP_UI=false
ARG YARN_OPTS

LABEL maintainer="Orkes OSS <oss@orkes.io>"

# Freshworks fork: build the customized React UI in ui/ (tenant selector, role gate, version
# dropdown, errors inspector) instead of the upstream ui-next. The tenant features have not been
# ported to ui-next yet. Switch this stage back to ui-next once they are.
COPY ui /conductor/ui
WORKDIR /conductor/ui

# Include monaco sources into the bundle (instead of using the CDN) and enable the errors inspector.
ENV REACT_APP_MONACO_EDITOR_USING_CDN=false
ENV REACT_APP_ENABLE_ERRORS_INSPECTOR=true

RUN if [ "$SKIP_UI" = "true" ]; then \
      mkdir -p build && echo "UI build skipped (SKIP_UI=true)"; \
    elif [ "$PREBUILT" = "false" ]; then \
      yarn ${YARN_OPTS} install && \
      cp -r node_modules/monaco-editor public/ && \
      NODE_OPTIONS=--max-old-space-size=4096 yarn ${YARN_OPTS} build; \
    else \
      echo "Using pre-built UI assets (ui/build)"; \
    fi

# ===========================================================================================================
# 2. Final stage
# ===========================================================================================================
FROM debian:stable-slim

LABEL maintainer="Orkes OSS <oss@orkes.io>"

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless nginx curl ca-certificates \
    && rm -f /etc/nginx/sites-enabled/default \
    && rm -rf /var/lib/apt/lists/*

# Make app folders
RUN mkdir -p /app/config /app/logs /app/libs

# Copy the compiled output to new image
COPY docker/server/bin /app
COPY docker/server/config /app/config
COPY --from=builder /conductor/server/build/libs/*boot*.jar /app/libs/conductor-server.jar
COPY --from=builder /conductor/server/build/libs/opentelemetry-javaagent.jar /app/libs/opentelemetry-javaagent.jar

# Copy compiled UI assets to nginx www directory
WORKDIR /usr/share/nginx/html
RUN rm -rf ./*
COPY --from=ui-builder /conductor/ui/build .
COPY docker/server/nginx/nginx.conf  /etc/nginx/conf.d/default.conf

# Copy the files for the server into the app folders
RUN chmod +x /app/startup.sh

HEALTHCHECK --interval=60s --timeout=30s --retries=10 CMD curl -I -XGET http://localhost:8080/health || exit 1

CMD [ "/app/startup.sh" ]
ENTRYPOINT [ "/bin/sh"]
