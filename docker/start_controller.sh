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
declare -r ACCOUNT=${ACCOUNT:-opensource4you}
declare -r KAFKA_ACCOUNT=${KAFKA_ACCOUNT:-apache}
declare -r KAFKA_VERSION=${KAFKA_REVISION:-${KAFKA_VERSION:-4.0.0}}
declare -r DOCKERFILE=$DOCKER_FOLDER/controller.dockerfile
declare -r EXPORTER_VERSION="0.16.1"
declare -r CLUSTER_ID=${CLUSTER_ID:-"$(randomString)"}
declare -r EXPORTER_PORT=${EXPORTER_PORT:-"$(getRandomPort)"}
declare -r NODE_ID=${NODE_ID:-"$(getRandomPort)"}
declare -r VOTERS=${VOTERS:-""}
declare -r CONTROLLER_PORT=${CONTROLLER_PORT:-"$(generateControllerPort)"}
declare -r CONTAINER_NAME="controller-$NODE_ID"
declare -r BOOTSTRAP_HOST=${BOOTSTRAP_HOST:-""}
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
declare -r IMAGE_NAME="ghcr.io/${ACCOUNT:l}/astraea/controller:$KAFKA_VERSION"
declare -r METADATA_VERSION=${METADATA_VERSION:-""}
# cleanup the file if it is existent
[[ -f "$CONTROLLER_PROPERTIES" ]] && rm -f "$CONTROLLER_PROPERTIES"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_controller.sh [ ARGUMENTS ]"
  echo "ENV: "
  echo "    KAFKA_ACCOUNT=apache                      set the github account for kafka repo"
  echo "    ACCOUNT=opensource4you                      set the github account for astraea repo"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"                set controller JVM memory"
  echo "    KAFKA_REVISION=trunk                           set revision of kafka source code to build container"
  echo "    KAFKA_VERSION=4.0.0                            set version of kafka distribution"
  echo "    BUILD=false                              set true if you want to build image locally"
  echo "    RUN=false                                set false if you want to build/pull image only"
  echo "    META_FOLDER=/tmp/folder1                set host folder used by controller"
}

function generateDockerfileBySource() {
  local kafka_repo="https://github.com/${KAFKA_ACCOUNT}/kafka"
  echo "# this dockerfile is generated dynamically
FROM ghcr.io/opensource4you/astraea/deps AS build

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/examples/kafka-2_0_0.yml --output-document=$JMX_CONFIG_FILE_IN_CONTAINER_PATH
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar

# build kafka from source code
RUN git clone --depth=1 ${kafka_repo} /tmp/kafka
WORKDIR /tmp/kafka
RUN git fetch --depth=1 origin $KAFKA_VERSION
RUN git checkout $KAFKA_VERSION
RUN ./gradlew clean releaseTarGz
RUN mkdir /opt/kafka
RUN tar -zxvf \$(find ./core/build/distributions/ -maxdepth 1 -type f \( -iname \"kafka*tgz\" ! -iname \"*sit*\" \)) -C /opt/kafka --strip-components=1

FROM azul/zulu-openjdk:23-jre

# copy kafka
COPY --from=build /opt/jmx_exporter /opt/jmx_exporter
COPY --from=build /opt/kafka /opt/kafka

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/kafka
USER $USER

# export ENV
ENV KAFKA_HOME=/opt/kafka
WORKDIR /opt/kafka
" >"$DOCKERFILE"
}

function generateDockerfileByVersion() {
  local kafka_url="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz"
  local version=$KAFKA_VERSION
  if [[ "$KAFKA_VERSION" == *"rc"* ]]; then
    ## `4.0.0-rc1` the rc release does not exist in archive repo
    version=${KAFKA_VERSION%-*}
    kafka_url="https://dist.apache.org/repos/dist/dev/kafka/${KAFKA_VERSION}/kafka_2.13-${version}.tgz"
  fi
  echo "# this dockerfile is generated dynamically
FROM ubuntu:24.04 AS build

# install tools
RUN apt-get update && apt-get install -y wget

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/examples/kafka-2_0_0.yml --output-document=$JMX_CONFIG_FILE_IN_CONTAINER_PATH
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar

# download kafka
WORKDIR /tmp
RUN wget $kafka_url
RUN mkdir /opt/kafka
RUN tar -zxvf kafka_2.13-${version}.tgz -C /opt/kafka --strip-components=1

FROM azul/zulu-openjdk:23-jre

# copy kafka
COPY --from=build /opt/jmx_exporter /opt/jmx_exporter
COPY --from=build /opt/kafka /opt/kafka

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/kafka
USER $USER

# export ENV
ENV KAFKA_HOME=/opt/kafka
WORKDIR /opt/kafka
" >"$DOCKERFILE"
}

function generateDockerfile() {
  if [[ -n "$KAFKA_REVISION" ]]; then
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

addVoter="false"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  echo "$1" >>"$CONTROLLER_PROPERTIES"
  if [[ "$1" == "controller.quorum.bootstrap.servers"* ]]; then
    addVoter="true"
  fi
  shift
done

rejectProperty "node.id"
rejectProperty "listeners"
rejectProperty "process.roles"
rejectProperty "controller.listener.names"
rejectProperty "advertised.listeners"
rejectProperty "log.dirs"
rejectProperty "broker.id"
# we don't use this property as kraft can use head of log.dirs instead
rejectProperty "metadata.log.dir"

setPropertyIfEmpty "node.id" "$NODE_ID"
if [[ "$addVoter" == "false" ]]; then
  setPropertyIfEmpty "controller.quorum.bootstrap.servers" "${ADDRESS}:$CONTROLLER_PORT"
fi
setPropertyIfEmpty "listeners" "CONTROLLER://:$CONTROLLER_PORT"
setPropertyIfEmpty "advertised.listeners" "CONTROLLER://${ADDRESS}:$CONTROLLER_PORT"
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

release_version=""
if [[ -n "$METADATA_VERSION" ]]; then
  release_version="--release-version $METADATA_VERSION"
fi


command="./bin/kafka-storage.sh format -t $CLUSTER_ID $release_version -c /tmp/controller.properties --standalone --ignore-formatted && ./bin/kafka-server-start.sh /tmp/controller.properties"
if [[ "$addVoter" == "true" ]]; then
  command="./bin/kafka-storage.sh format -t $CLUSTER_ID $release_version -c /tmp/controller.properties --no-initial-controllers && ./bin/kafka-server-start.sh /tmp/controller.properties"
fi

docker run -d --init \
  --name $CONTAINER_NAME \
  -e KAFKA_HEAP_OPTS="$HEAP_OPTS" \
  -e KAFKA_JMX_OPTS="$JMX_OPTS" \
  -e KAFKA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar=$EXPORTER_PORT:$JMX_CONFIG_FILE_IN_CONTAINER_PATH" \
  -v $CONTROLLER_PROPERTIES:/tmp/controller.properties:ro \
  $(generateJmxConfigMountCommand) \
  $metaMountCommand \
  -h $CONTAINER_NAME \
  -p $CONTROLLER_PORT:$CONTROLLER_PORT \
  -p $CONTROLLER_JMX_PORT:$CONTROLLER_JMX_PORT \
  -p $EXPORTER_PORT:$EXPORTER_PORT \
  "$IMAGE_NAME" sh -c "$command"

if [ "$(echo $?)" -eq 0 ]; then
  echo "================================================="
  [[ -n "$META_FOLDER" ]] && echo "mount $META_FOLDER to container: $CONTAINER_NAME"
  echo "controller address: ${ADDRESS}:$CONTROLLER_PORT"
  echo "jmx address: ${ADDRESS}:$CONTROLLER_JMX_PORT"
  echo "exporter address: ${ADDRESS}:$EXPORTER_PORT"
  echo "================================================="
  echo "run CLUSTER_ID=$CLUSTER_ID $DOCKER_FOLDER/start_controller.sh controller.quorum.bootstrap.servers=${ADDRESS}:$CONTROLLER_PORT to join observer"
  echo "run CLUSTER_ID=$CLUSTER_ID $DOCKER_FOLDER/start_broker.sh controller.quorum.bootstrap.servers=${ADDRESS}:$CONTROLLER_PORT to join broker"
  echo "================================================="
else
    echo "Start up fail"
fi
