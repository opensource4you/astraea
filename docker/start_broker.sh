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
declare -r VERSION=${VERSION:-main}
declare -r KAFKA_ACCOUNT=${KAFKA_ACCOUNT:-apache}
declare -r KAFKA_VERSION=${KAFKA_REVISION:-${KAFKA_VERSION:-4.0.0}}
declare -r DOCKERFILE=$DOCKER_FOLDER/broker.dockerfile
declare -r DATA_FOLDER_IN_CONTAINER_PREFIX="/tmp/log-folder"
declare -r EXPORTER_VERSION="0.16.1"
declare -r EXPORTER_PORT=${EXPORTER_PORT:-"$(getRandomPort)"}
declare -r NODE_ID=${NODE_ID:-"$(getRandomPort)"}
declare -r BROKER_PORT=${BROKER_PORT:-"$(getRandomPort)"}
declare -r CONTAINER_NAME="broker-$NODE_ID"
declare -r BROKER_JMX_PORT="${BROKER_JMX_PORT:-"$(getRandomPort)"}"
declare -r ADMIN_NAME="admin"
declare -r ADMIN_PASSWORD="admin-secret"
declare -r USER_NAME="user"
declare -r USER_PASSWORD="user-secret"
declare -r JMX_CONFIG_FILE="${JMX_CONFIG_FILE}"
declare -r JMX_CONFIG_FILE_IN_CONTAINER_PATH="/opt/jmx_exporter/jmx_exporter_config.yml"
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$BROKER_JMX_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=$BROKER_JMX_PORT \
  -Djava.rmi.server.hostname=$ADDRESS"
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"
declare -r BROKER_PROPERTIES="/tmp/server-${BROKER_PORT}.properties"
declare -r IMAGE_NAME="ghcr.io/${ACCOUNT:l}/astraea/broker:${KAFKA_VERSION:l}"
declare -r METADATA_VERSION=${METADATA_VERSION:-""}
# cleanup the file if it is existent
[[ -f "$BROKER_PROPERTIES" ]] && rm -f "$BROKER_PROPERTIES"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_broker.sh [ ARGUMENTS ]"
  echo "Required Argument (zk mode): "
  echo "    zookeeper.connect=node:22222              set zookeeper connection"
  echo "Required Argument (kraft mode): "
  echo "    controller.quorum.bootstrap.servers=12310@node:14335 set controller connection"
  echo "Optional Arguments: "
  echo "    num.io.threads=10                        set broker I/O threads"
  echo "    num.network.threads=10                   set broker network threads"
  echo "ENV: "
  echo "    KAFKA_ACCOUNT=apache                      set the github account for kafka repo"
  echo "    ACCOUNT=opensource4you                      set the github account for astraea repo"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"                set broker JVM memory"
  echo "    KAFKA_REVISION=trunk                           set revision of kafka source code to build container"
  echo "    KAFKA_VERSION=4.0.0                            set version of kafka distribution"
  echo "    BUILD=false                              set true if you want to build image locally"
  echo "    RUN=false                                set false if you want to build/pull image only"
  echo "    DATA_FOLDERS=/tmp/folder1                set host folders used by broker"
  echo "    CLUSTER_ID=asdasdasdsd                set cluster id for krafe mode"
}

function generateDockerfileBySource() {
  local kafka_repo="https://github.com/${KAFKA_ACCOUNT}/kafka"
  local astraea_repo="https://github.com/${ACCOUNT}/astraea"
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

# build astraea from source code
RUN git clone --depth=1 ${astraea_repo} /tmp/astraea
WORKDIR /tmp/astraea
RUN git fetch --depth=1 origin $VERSION
RUN git checkout $VERSION
RUN ./gradlew clean build -x test
RUN cp /tmp/astraea/common/build/libs/*.jar /opt/kafka/libs/

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

function setListener() {
  if [[ "$SASL" == "true" ]]; then
    echo "listeners=SASL_PLAINTEXT://:9092" >>"$BROKER_PROPERTIES"
    echo "advertised.listeners=SASL_PLAINTEXT://${ADDRESS}:$BROKER_PORT" >>"$BROKER_PROPERTIES"
    echo "security.inter.broker.protocol=SASL_PLAINTEXT" >>"$BROKER_PROPERTIES"
    echo "sasl.mechanism.inter.broker.protocol=PLAIN" >>"$BROKER_PROPERTIES"
    echo "sasl.enabled.mechanisms=PLAIN" >>"$BROKER_PROPERTIES"
    echo "listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username="$ADMIN_NAME" \
                 password="$ADMIN_PASSWORD" \
                 user_${ADMIN_NAME}="$ADMIN_PASSWORD" \
                 user_${USER_NAME}="${USER_PASSWORD}";" >>"$BROKER_PROPERTIES"
    echo "authorizer.class.name=kafka.security.authorizer.AclAuthorizer" >>"$BROKER_PROPERTIES"
    # allow brokers to communicate each other
    echo "super.users=User:admin" >>"$BROKER_PROPERTIES"
  else
    echo "listeners=PLAINTEXT://:9092" >>"$BROKER_PROPERTIES"
    echo "advertised.listeners=PLAINTEXT://${ADDRESS}:$BROKER_PORT" >>"$BROKER_PROPERTIES"
  fi
}

function setLogDirs() {
  declare -i count=0
  if [[ -n "$DATA_FOLDERS" ]]; then
    IFS=',' read -ra folders <<<"$DATA_FOLDERS"
    for folder in "${folders[@]}"; do
      # create the folder if it is nonexistent
      mkdir -p "$folder"
      count=$((count + 1))
    done
  else
    count=3
  fi

  local logConfigs="log.dirs=$DATA_FOLDER_IN_CONTAINER_PREFIX-0"
  for ((i = 1; i < count; i++)); do
    logConfigs="$logConfigs,$DATA_FOLDER_IN_CONTAINER_PREFIX-$i"
  done

  echo $logConfigs >>"$BROKER_PROPERTIES"
}

function generateJmxConfigMountCommand() {
    if [[ "$JMX_CONFIG_FILE" != "" ]]; then
        echo "--mount type=bind,source=$JMX_CONFIG_FILE,target=$JMX_CONFIG_FILE_IN_CONTAINER_PATH"
    else
        echo ""
    fi
}

function generateDataFolderMountCommand() {
  local mount=""
  if [[ -n "$DATA_FOLDERS" ]]; then
    IFS=',' read -ra folders <<<"$DATA_FOLDERS"
    # start with 0 rather than 1 (see setLogDirs)
    declare -i count=0

    for folder in "${folders[@]}"; do
      mount="$mount -v $folder:$DATA_FOLDER_IN_CONTAINER_PREFIX-$count"
      count=$((count + 1))
    done
  fi
  echo "$mount"
}

function rejectProperty() {
  local key=$1
  if [[ -f "$BROKER_PROPERTIES" ]] && [[ "$(cat $BROKER_PROPERTIES | grep $key)" != "" ]]; then
    echo "$key is NOT configurable"
    exit 2
  fi
}

function requireProperty() {
  local key=$1
  if [[ ! -f "$BROKER_PROPERTIES" ]] || [[ "$(cat $BROKER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key is required"
    exit 2
  fi
}

function setPropertyIfEmpty() {
  local key=$1
  local value=$2
  # performance configs
  if [[ ! -f "$BROKER_PROPERTIES" ]] || [[ "$(cat $BROKER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key=$value" >>"$BROKER_PROPERTIES"
  fi
}

function getProperty() {
  local key=$1
  echo $(cat "$BROKER_PROPERTIES" | grep $key | cut -d = -f 2)
}

# ===================================[main]===================================

checkDocker
buildImageIfNeed "$IMAGE_NAME"
if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork

quorum="unknown"
migration="false"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  echo "$1" >>"$BROKER_PROPERTIES"
  if [[ "$1" == "controller.quorum.bootstrap.servers"* ]]; then
    quorum="kraft"
  fi
  if [[ "$1" == "zookeeper.connect"* ]]; then
    quorum="zookeeper"
  fi
  if [[ "$1" == "zookeeper.metadata.migration.enable=true"* ]]; then
    migration="true"
  fi
  shift
done

if [[ "$quorum" == "unknown" ]]; then
  echo "please define either zookeeper.connect or controller.quorum.bootstrap.servers"
  exit 2
fi

rejectProperty "listeners"
rejectProperty "log.dirs"
rejectProperty "broker.id"
rejectProperty "node.id"
rejectProperty "process.roles"
rejectProperty "controller.listener.names"

setListener
setPropertyIfEmpty "num.io.threads" "8"
setPropertyIfEmpty "num.network.threads" "8"
setPropertyIfEmpty "num.partitions" "8"
setPropertyIfEmpty "transaction.state.log.replication.factor" "1"
setPropertyIfEmpty "offsets.topic.replication.factor" "1"
setPropertyIfEmpty "share.coordinator.state.topic.replication.factor" "1"
setPropertyIfEmpty "transaction.state.log.min.isr" "1"
setPropertyIfEmpty "min.insync.replicas" "1"
setLogDirs

command="./bin/kafka-server-start.sh /tmp/broker.properties"
if [[ "$quorum" == "kraft" ]]; then
  if [[ "$migration" == "false" ]]; then

      release_version=""
      if [[ -n "$METADATA_VERSION" ]]; then
        release_version="--release-version $METADATA_VERSION"
      fi

      if [[ -z "$CLUSTER_ID" ]]; then
        echo "please set CLUSTER_ID for krafe mode"
        exit 2
      fi
      setPropertyIfEmpty "node.id" "$NODE_ID"
      setPropertyIfEmpty "process.roles" "broker"
      setPropertyIfEmpty "controller.listener.names" "CONTROLLER"
      command="./bin/kafka-storage.sh format -t $CLUSTER_ID $release_version -c /tmp/broker.properties --ignore-formatted && ./bin/kafka-server-start.sh /tmp/broker.properties"
  else
      setPropertyIfEmpty "broker.id" "$NODE_ID"
      setPropertyIfEmpty "broker.id.generation.enable" "false"
      setPropertyIfEmpty "listener.security.protocol.map" "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
      setPropertyIfEmpty "controller.listener.names" "CONTROLLER"
  fi
fi

docker run -d --init \
  --name $CONTAINER_NAME \
  -e KAFKA_HEAP_OPTS="$HEAP_OPTS" \
  -e KAFKA_JMX_OPTS="$JMX_OPTS" \
  -e KAFKA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar=$EXPORTER_PORT:$JMX_CONFIG_FILE_IN_CONTAINER_PATH" \
  -v $BROKER_PROPERTIES:/tmp/broker.properties:ro \
  $(generateJmxConfigMountCommand) \
  $(generateDataFolderMountCommand) \
  -p $BROKER_PORT:9092 \
  -p $BROKER_JMX_PORT:$BROKER_JMX_PORT \
  -p $EXPORTER_PORT:$EXPORTER_PORT \
  "$IMAGE_NAME" sh -c "$command"

if [ "$(echo $?)" -eq 0 ]; then
  echo "================================================="
  [[ -n "$DATA_FOLDERS" ]] && echo "mount $DATA_FOLDERS to container: $CONTAINER_NAME"
  echo "node.id=$NODE_ID"
  echo "broker address: ${ADDRESS}:$BROKER_PORT"
  echo "jmx address: ${ADDRESS}:$BROKER_JMX_PORT"
  echo "exporter address: ${ADDRESS}:$EXPORTER_PORT"
  if [[ "$SASL" == "true" ]]; then
    user_jaas_file=/tmp/user-jaas-${BROKER_PORT}.conf
    echo "
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${USER_NAME} password=${USER_PASSWORD};
    security.protocol=SASL_PLAINTEXT
    sasl.mechanism=PLAIN
    " >$user_jaas_file

    admin_jaas_file=/tmp/admin-jaas-${BROKER_PORT}.conf
    echo "
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${ADMIN_NAME} password=${ADMIN_PASSWORD};
    security.protocol=SASL_PLAINTEXT
    sasl.mechanism=PLAIN
    " >$admin_jaas_file
    echo "SASL_PLAINTEXT is enabled. user config: $user_jaas_file admin config: $admin_jaas_file"
  fi
  echo "================================================="
  echo "run $DOCKER_FOLDER/start_worker.sh bootstrap.servers=$ADDRESS:$BROKER_PORT group.id=$(randomString) to join kafka worker"
  echo "================================================="
else
    echo "Start up fail"
fi
