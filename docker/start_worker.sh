#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source $DOCKER_FOLDER/docker_build_common.sh

# ===============================[global variables]===============================
declare -r ACCOUNT=${ACCOUNT:-skiptests}
declare -r KAFKA_ACCOUNT=${KAFKA_ACCOUNT:-apache}
declare -r VERSION=${REVISION:-${VERSION:-3.4.0}}
declare -r DOCKERFILE=$DOCKER_FOLDER/worker.dockerfile
declare -r WORKER_PORT=${WORKER_PORT:-"$(getRandomPort)"}
declare -r CONTAINER_NAME="worker-$WORKER_PORT"
declare -r WORKER_JMX_PORT="${WORKER_JMX_PORT:-"$(getRandomPort)"}"
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$WORKER_JMX_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=$WORKER_JMX_PORT \
  -Djava.rmi.server.hostname=$ADDRESS"
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"
declare -r WORKER_PROPERTIES="/tmp/worker-${WORKER_PORT}.properties"
declare -r WORKER_PLUGIN_PATH=${WORKER_PLUGIN_PATH:-/tmp/worker-plugins}
declare -r IMAGE_NAME="ghcr.io/${ACCOUNT}/astraea/worker:$VERSION"
declare -r SCRIPT_LOCATION_IN_CONTAINER="./bin/connect-distributed.sh"
# cleanup the file if it is existent
[[ -f "$WORKER_PROPERTIES" ]] && rm -f "$WORKER_PROPERTIES"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_worker.sh [ ARGUMENTS ]"
  echo "Required Argument: "
  echo "    bootstrap.servers=node:22222,node:1111   set brokers connection"
  echo "ENV: "
  echo "    KAFKA_ACCOUNT=apache                      set the github account for kafka repo"
  echo "    ACCOUNT=skiptests                      set the github account for astraea repo"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"              set worker JVM memory"
  echo "    REVISION=trunk                           set revision of kafka source code to build container"
  echo "    VERSION=3.4.0                            set version of kafka distribution"
  echo "    BUILD=false                              set true if you want to build image locally"
  echo "    RUN=false                                set false if you want to build/pull image only"
  echo "    WORKER_PLUGIN_PATH=/tmp/worker-plugins   set plugin path to kafka worker"
}

function rejectProperty() {
  local key=$1
  if [[ -f "$WORKER_PROPERTIES" ]] && [[ "$(cat $WORKER_PROPERTIES | grep $key)" != "" ]]; then
    echo "$key is NOT configurable"
    exit 2
  fi
}

function requireProperty() {
  local key=$1
  if [[ ! -f "$WORKER_PROPERTIES" ]] || [[ "$(cat $WORKER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key is required"
    exit 2
  fi
}

function generateDockerfileBySource() {
  local kafka_repo="https://github.com/${KAFKA_ACCOUNT}/kafka"
  local repo="https://github.com/${ACCOUNT}/astraea"

  echo "# this dockerfile is generated dynamically
FROM ghcr.io/skiptests/astraea/deps AS build

# build kafka from source code
RUN git clone ${kafka_repo} /tmp/kafka
WORKDIR /tmp/kafka
RUN git checkout $VERSION
# generate gradlew for previous
RUN cp /tmp/kafka/gradlew /tmp/gradlew || /tmp/gradle-5.6.4/bin/gradle
RUN ./gradlew clean releaseTarGz
RUN mkdir /opt/kafka
RUN tar -zxvf \$(find ./core/build/distributions/ -maxdepth 1 -type f \( -iname \"kafka*tgz\" ! -iname \"*sit*\" \)) -C /opt/kafka --strip-components=1
RUN git clone ${repo} /tmp/astraea
WORKDIR /tmp/astraea
RUN ./gradlew clean shadowJar
RUN cp /tmp/astraea/connector/build/libs/astraea-*-all.jar /opt/kafka/libs/

FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y openjdk-17-jre

# copy kafka
COPY --from=build /opt/kafka /opt/kafka

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/kafka
USER $USER

# export ENV
ENV KAFKA_HOME /opt/kafka
WORKDIR /opt/kafka
" >"$DOCKERFILE"
}

function generateDockerfileByVersion() {
  local repo="https://github.com/${ACCOUNT}/astraea"
  echo "# this dockerfile is generated dynamically
FROM ghcr.io/skiptests/astraea/deps AS build

# install tools
RUN apt-get update && apt-get install -y wget

# download kafka
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/kafka/${VERSION}/kafka_2.13-${VERSION}.tgz
RUN mkdir /opt/kafka
RUN tar -zxvf kafka_2.13-${VERSION}.tgz -C /opt/kafka --strip-components=1
RUN git clone ${repo} /tmp/astraea
WORKDIR /tmp/astraea
RUN ./gradlew clean shadowJar
RUN cp /tmp/astraea/connector/build/libs/astraea-*-all.jar /opt/kafka/libs/

FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y openjdk-17-jre

# copy kafka
COPY --from=build /opt/kafka /opt/kafka

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/kafka
USER $USER

# export ENV
ENV KAFKA_HOME /opt/kafka
WORKDIR /opt/kafka
" >"$DOCKERFILE"
}

function generateDockerfile() {
  if [[ -n "$REVISION" ]]; then
    generateDockerfileBySource
  else
    generateDockerfileByVersion
  fi
}

function setPropertyIfEmpty() {
  local key=$1
  local value=$2
  # performance configs
  if [[ "$(cat $WORKER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key=$value" >>"$WORKER_PROPERTIES"
  fi
}

# ===================================[main]===================================

checkDocker
buildImageIfNeed "$IMAGE_NAME"
if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork

while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  echo "$1" >>"$WORKER_PROPERTIES"
  shift
done

# these properties are set internally
rejectProperty "offset.storage.topic"
rejectProperty "config.storage.topic"
rejectProperty "status.storage.topic"
rejectProperty "listeners"
rejectProperty "rest.advertised.host.name"
rejectProperty "rest.advertised.port"
rejectProperty "rest.advertised.listener"

requireProperty "bootstrap.servers"
setPropertyIfEmpty "plugin.path" "/opt/worker-plugins"
setPropertyIfEmpty "group.id" "worker-$(cat /dev/random | env LC_CTYPE=C tr -dc 'a-zA-Z0-9' | fold -w 5 | head -n 1)"

# take group id from prop file
WORKER_GROUP_ID=$(cat $WORKER_PROPERTIES | grep "group.id" | cut -d "=" -f2)

# set the default converter
setPropertyIfEmpty "key.converter" "org.apache.kafka.connect.json.JsonConverter"
setPropertyIfEmpty "value.converter" "org.apache.kafka.connect.json.JsonConverter"
# Topic to use for storing offsets
setPropertyIfEmpty "offset.storage.topic" "offsets-$WORKER_GROUP_ID"
setPropertyIfEmpty "offset.storage.replication.factor" "1"
# Topic to use for storing connector and task configurations
setPropertyIfEmpty "config.storage.topic" "config-$WORKER_GROUP_ID"
setPropertyIfEmpty "config.storage.replication.factor" "1"
# Topic to use for storing statuses
setPropertyIfEmpty "status.storage.topic" "status-$WORKER_GROUP_ID"
setPropertyIfEmpty "status.storage.replication.factor" "1"
# this is the hostname/port that will be given out to other workers to connect to
setPropertyIfEmpty "rest.advertised.host.name" "$ADDRESS"
setPropertyIfEmpty "rest.advertised.port" "$WORKER_PORT"

# WORKER_PLUGIN_PATH is used to mount connector jars to worker container
mkdir -p "$WORKER_PLUGIN_PATH"

docker run -d --init \
  --name "$CONTAINER_NAME" \
  -e KAFKA_HEAP_OPTS="$HEAP_OPTS" \
  -e KAFKA_JMX_OPTS="$JMX_OPTS" \
  -v "$WORKER_PROPERTIES":/tmp/worker.properties:ro \
  -v "$WORKER_PLUGIN_PATH":/opt/worker-plugins:ro \
  -p "$WORKER_PORT":8083 \
  -p $WORKER_JMX_PORT:$WORKER_JMX_PORT \
  "$IMAGE_NAME" "$SCRIPT_LOCATION_IN_CONTAINER" /tmp/worker.properties

echo "================================================="
echo "worker address: ${ADDRESS}:$WORKER_PORT"
echo "group.id: $WORKER_GROUP_ID"
echo "jmx address: ${ADDRESS}:$WORKER_JMX_PORT"
echo "================================================="
