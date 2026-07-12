#!/bin/sh
#
#  Copyright 2026 Conductor authors
#  <p>
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
#  <p>
#  http://www.apache.org/licenses/LICENSE-2.0
#  <p>
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
#

# startup.sh - startup script for the migrator docker image.
# Config is normally supplied as env vars (k8s ConfigMap via envFrom); Spring Boot reads them
# directly. Optionally mount a properties file and point CONFIG_LOCATION at it.

echo "Starting Conductor migrator"
cd /app/libs

EXTRA_ARGS=""
if [ -n "$CONFIG_LOCATION" ]; then
  echo "Using additional config location: $CONFIG_LOCATION"
  EXTRA_ARGS="--spring.config.additional-location=$CONFIG_LOCATION"
fi

echo "Using java options: $JAVA_OPTS"
# exec so java is PID 1 and receives SIGTERM from k8s for graceful shutdown.
exec java ${JAVA_OPTS} -jar conductor-migrator.jar ${EXTRA_ARGS}
