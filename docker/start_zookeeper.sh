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
declare -r VERSION=${VERSION:-3.7.1}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/zookeeper}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r ZOOKEEPER_PORT=${ZOOKEEPER_PORT:-"$(getRandomPort)"}
declare -r ZOOKEEPER_JMX_PORT="${ZOOKEEPER_JMX_PORT:-"$(getRandomPort)"}"
declare -r DOCKERFILE=$DOCKER_FOLDER/zookeeper.dockerfile
declare -r DATA_FOLDER_IN_CONTAINER="/tmp/zookeeper-dir"
declare -r CONTAINER_NAME="zookeeper-$ZOOKEEPER_PORT"
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx1G -Xms256m"}"
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$ZOOKEEPER_JMX_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=$ZOOKEEPER_JMX_PORT \
  -Djava.rmi.server.hostname=$ADDRESS"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_zookeeper.sh"
  echo "ENV: "
  echo "    REPO=astraea/zk            set the docker repo"
  echo "    VERSION=3.7.1              set version of zookeeper distribution"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
  echo "    DATA_FOLDER=/tmp/folder1   set host folders used by zookeeper"
}

function generateDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:22.04 AS build

# install tools
RUN apt-get update && apt-get install -y wget

# download zookeeper
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/zookeeper/zookeeper-${VERSION}/apache-zookeeper-${VERSION}-bin.tar.gz
RUN mkdir /opt/zookeeper
RUN tar -zxvf apache-zookeeper-${VERSION}-bin.tar.gz -C /opt/zookeeper --strip-components=1

FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre

# copy zookeeper
COPY --from=build /opt/zookeeper /opt/zookeeper

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# create config file
RUN echo "tickTime=2000" >> /opt/zookeeper/conf/zoo.cfg
RUN echo "dataDir=$DATA_FOLDER_IN_CONTAINER" >> /opt/zookeeper/conf/zoo.cfg
RUN echo "clientPort=2181" >> /opt/zookeeper/conf/zoo.cfg

# change user
RUN chown -R $USER:$USER /opt/zookeeper
USER $USER

# export ENV
ENV ZOOKEEPER_HOME /opt/zookeeper
WORKDIR /opt/zookeeper
" >"$DOCKERFILE"
}

# ===================================[main]===================================

if [[ "$1" == "help" ]]; then
  showHelp
  exit 0
fi

checkDocker
buildImageIfNeed "$IMAGE_NAME"

if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork

if [[ -n "$DATA_FOLDER" ]]; then
  mkdir -p "$DATA_FOLDER"
  echo "mount $DATA_FOLDER to container"
  docker run -d --init \
    --name $CONTAINER_NAME \
    -e JVMFLAGS="$HEAP_OPTS" \
    -p $ZOOKEEPER_PORT:2181 \
    -v "$DATA_FOLDER":$DATA_FOLDER_IN_CONTAINER \
    "$IMAGE_NAME" ./bin/zkServer.sh start-foreground
else
  # TODO: zookeeper does not support java.rmi.server.hostname so we have to disable the default settings of jmx from zookeeper
  # and then add our custom settings. see https://issues.apache.org/jira/browse/ZOOKEEPER-3606
  docker run -d --init \
    -e JMXDISABLE="true" \
    -e JVMFLAGS="$HEAP_OPTS $JMX_OPTS" \
    -p $ZOOKEEPER_JMX_PORT:$ZOOKEEPER_JMX_PORT \
    -p $ZOOKEEPER_PORT:2181 \
    "$IMAGE_NAME" ./bin/zkServer.sh start-foreground
fi

echo "================================================="
echo "jmx address: ${ADDRESS}:$ZOOKEEPER_JMX_PORT"
echo "run $DOCKER_FOLDER/start_broker.sh zookeeper.connect=$ADDRESS:$ZOOKEEPER_PORT to join kafka broker"
echo "run env CONFLUENT_BROKER=true $DOCKER_FOLDER/start_broker.sh zookeeper.connect=$ADDRESS:$ZOOKEEPER_PORT to join confluent kafka broker"
echo "================================================="
