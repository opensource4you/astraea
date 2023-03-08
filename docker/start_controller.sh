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
declare -r DOCKERFILE=$DOCKER_FOLDER/controller.dockerfile
declare -r EXPORTER_VERSION="0.16.1"
declare -r CLUSTER_ID=${CLUSTER_ID:-"$(randomString)"}
declare -r EXPORTER_PORT=${EXPORTER_PORT:-"$(getRandomPort)"}
declare -r NODE_ID=${NODE_ID:-"$(getRandomPort)"}
declare -r CONTROLLER_PORT=${CONTROLLER_PORT:-"$(getRandomPort)"}
declare -r CONTAINER_NAME="controller-$CONTROLLER_PORT"
declare -r CONTROLLER_JMX_PORT="${CONTROLLER_JMX_PORT:-"$(getRandomPort)"}"
declare -r JMX_CONFIG_FILE="${JMX_CONFIG_FILE}"
declare -r JMX_CONFIG_FILE_IN_CONTAINER_PATH="/opt/jmx_exporter/jmx_exporter_config.yml"
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$CONTROLLER_JMX_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=$CONTROLLER_JMX_PORT \
  -Djava.rmi.server.hostname=$ADDRESS"
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"
declare -r CONTROLLER_PROPERTIES="/tmp/controller-${CONTROLLER_PORT}.properties"
declare -r IMAGE_NAME="ghcr.io/${ACCOUNT}/astraea/controller:$VERSION"
# cleanup the file if it is existent
[[ -f "$CONTROLLER_PROPERTIES" ]] && rm -f "$CONTROLLER_PROPERTIES"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_controller.sh [ ARGUMENTS ]"
  echo "ENV: "
  echo "    KAFKA_ACCOUNT=apache                      set the github account for kafka repo"
  echo "    ACCOUNT=skiptests                      set the github account for astraea repo"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"                set controller JVM memory"
  echo "    REVISION=trunk                           set revision of kafka source code to build container"
  echo "    VERSION=3.4.0                            set version of kafka distribution"
  echo "    BUILD=false                              set true if you want to build image locally"
  echo "    RUN=false                                set false if you want to build/pull image only"
  echo "    META_FOLDER=/tmp/folder1                set host folder used by controller"
}

function generateDockerfileBySource() {
  local kafka_repo="https://github.com/${KAFKA_ACCOUNT}/kafka"
  echo "# this dockerfile is generated dynamically
FROM ghcr.io/skiptests/astraea/deps AS build

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml --output-document=$JMX_CONFIG_FILE_IN_CONTAINER_PATH
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar

# build kafka from source code
RUN git clone ${kafka_repo} /tmp/kafka
WORKDIR /tmp/kafka
RUN git checkout $VERSION
RUN ./gradlew clean releaseTarGz
RUN mkdir /opt/kafka
RUN tar -zxvf \$(find ./core/build/distributions/ -maxdepth 1 -type f \( -iname \"kafka*tgz\" ! -iname \"*sit*\" \)) -C /opt/kafka --strip-components=1

FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre

# copy kafka
COPY --from=build /opt/jmx_exporter /opt/jmx_exporter
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
  echo "# this dockerfile is generated dynamically
FROM ubuntu:22.04 AS build

# install tools
RUN apt-get update && apt-get install -y wget

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml --output-document=$JMX_CONFIG_FILE_IN_CONTAINER_PATH
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar

# download kafka
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/kafka/${VERSION}/kafka_2.13-${VERSION}.tgz
RUN mkdir /opt/kafka
RUN tar -zxvf kafka_2.13-${VERSION}.tgz -C /opt/kafka --strip-components=1
WORKDIR /opt/kafka

FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre

# copy kafka
COPY --from=build /opt/jmx_exporter /opt/jmx_exporter
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

function generateJmxConfigMountCommand() {
    if [[ "$JMX_CONFIG_FILE" != "" ]]; then
        echo "--mount type=bind,source=$JMX_CONFIG_FILE,target=$JMX_CONFIG_FILE_IN_CONTAINER_PATH"
    else
        echo ""
    fi
}

function setPropertyIfEmpty() {
  local key=$1
  local value=$2
  if [[ ! -f "$CONTROLLER_PROPERTIES" ]] || [[ "$(cat $CONTROLLER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key=$value" >>"$CONTROLLER_PROPERTIES"
  fi
}

function rejectProperty() {
  local key=$1
  if [[ -f "$CONTROLLER_PROPERTIES" ]] && [[ "$(cat $CONTROLLER_PROPERTIES | grep $key)" != "" ]]; then
    echo "$key is NOT configurable"
    exit 2
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
  echo "$1" >>"$CONTROLLER_PROPERTIES"
  shift
done

rejectProperty "node.id"
rejectProperty "controller.quorum.voters"
rejectProperty "listeners"
rejectProperty "process.roles"
rejectProperty "controller.listener.names"
rejectProperty "advertised.listeners"
rejectProperty "zookeeper.connect"
rejectProperty "log.dirs"
rejectProperty "broker.id"
# we don't use this property as kraft can use head of log.dirs instead
rejectProperty "metadata.log.dir"

setPropertyIfEmpty "node.id" "$NODE_ID"
setPropertyIfEmpty "controller.quorum.voters" "$NODE_ID@${ADDRESS}:$CONTROLLER_PORT"
setPropertyIfEmpty "listeners" "CONTROLLER://:9093"
setPropertyIfEmpty "process.roles" "controller"
setPropertyIfEmpty "controller.listener.names" "CONTROLLER"
setPropertyIfEmpty "num.partitions" "8"
setPropertyIfEmpty "transaction.state.log.replication.factor" "1"
setPropertyIfEmpty "offsets.topic.replication.factor" "1"
setPropertyIfEmpty "transaction.state.log.min.isr" "1"
setPropertyIfEmpty "min.insync.replicas" "1"
setPropertyIfEmpty "log.dirs" "/tmp/kafka-meta"

metaMountCommand=""
if [[ -n "$META_FOLDER" ]]; then
  metaMountCommand="-v $META_FOLDER:/tmp/kafka-meta"
fi

docker run -d --init \
  --name $CONTAINER_NAME \
  -e KAFKA_HEAP_OPTS="$HEAP_OPTS" \
  -e KAFKA_JMX_OPTS="$JMX_OPTS" \
  -e KAFKA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar=$EXPORTER_PORT:$JMX_CONFIG_FILE_IN_CONTAINER_PATH" \
  -v $CONTROLLER_PROPERTIES:/tmp/controller.properties:ro \
  $(generateJmxConfigMountCommand) \
  $metaMountCommand \
  -p $CONTROLLER_PORT:9093 \
  -p $CONTROLLER_JMX_PORT:$CONTROLLER_JMX_PORT \
  -p $EXPORTER_PORT:$EXPORTER_PORT \
  "$IMAGE_NAME" sh -c "./bin/kafka-storage.sh format -t $CLUSTER_ID -c /tmp/controller.properties && ./bin/kafka-server-start.sh /tmp/controller.properties"

echo "================================================="
[[ -n "$META_FOLDER" ]] && echo "mount $META_FOLDER to container: $CONTAINER_NAME"
echo "controller address: ${ADDRESS}:$CONTROLLER_PORT"
echo "jmx address: ${ADDRESS}:$CONTROLLER_JMX_PORT"
echo "exporter address: ${ADDRESS}:$EXPORTER_PORT"
echo "================================================="
echo "run CLUSTER_ID=$CLUSTER_ID $DOCKER_FOLDER/start_broker.sh controller.quorum.voters=$NODE_ID@${ADDRESS}:$CONTROLLER_PORT to join broker"
echo "================================================="
