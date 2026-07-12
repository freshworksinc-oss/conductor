#
# conductor:migrator - Build the standalone app-level migrator (jar-only; no UI/nginx).
# Mirrors docker/server/Dockerfile. Produces the base image the ceapps wrapper builds FROM.
#
# ===========================================================================================================
# 0. Builder stage — build the migrator boot jar from source
# ===========================================================================================================
FROM azul/zulu-openjdk-debian:17 AS builder

LABEL maintainer="Conductor OSS"

# Copy the project onto the image and build the migrator boot jar.
COPY . /conductor
WORKDIR /conductor
RUN ./gradlew :conductor-migrator:bootJar -x test -x spotlessCheck
WORKDIR /conductor/migrator/build/libs
RUN ls -ltr

# ===========================================================================================================
# 1. Bin stage — slim runtime with just the JRE + the jar
# ===========================================================================================================
FROM alpine:3.19

LABEL maintainer="Conductor OSS"

RUN apk add --no-cache openjdk17 curl

# App folders
RUN mkdir -p /app/config /app/logs /app/libs

COPY docker/migrator/bin /app
COPY --from=builder /conductor/migrator/build/libs/conductor-migrator.jar /app/libs/conductor-migrator.jar

RUN chmod +x /app/startup.sh

# Liveness for docker/k8s (the migrator serves actuator health on 8080).
HEALTHCHECK --interval=30s --timeout=10s --retries=10 \
    CMD curl -sf http://localhost:8080/actuator/health/liveness || exit 1

WORKDIR /app/libs
CMD [ "/app/startup.sh" ]
ENTRYPOINT [ "/bin/sh" ]
