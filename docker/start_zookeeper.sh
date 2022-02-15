#!/bin/bash

# ===============================[global variables]===============================
declare -r VERSION=${VERSION:-3.7.0}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/zookeeper}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source $DOCKER_FOLDER/docker_build_common.sh
declare -r ZOOKEEPER_PORT=${ZOOKEEPER_PORT:-"$(getRandomPort)"}
declare -r DOCKERFILE=$DOCKER_FOLDER/zookeeper.dockerfile
declare -r DATA_FOLDER_IN_CONTAINER="/tmp/zookeeper-dir"
declare -r CONTAINER_NAME="zookeeper-$ZOOKEEPER_PORT"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_zookeeper.sh"
  echo "ENV: "
  echo "    REPO=astraea/zk            set the docker repo"
  echo "    VERSION=3.7.0              set version of zookeeper distribution"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
  echo "    DATA_FOLDER=/tmp/folder1   set host folders used by zookeeper"
}

function generateDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# download zookeeper
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/zookeeper/zookeeper-${VERSION}/apache-zookeeper-${VERSION}-bin.tar.gz
RUN mkdir /opt/zookeeper
RUN tar -zxvf apache-zookeeper-${VERSION}-bin.tar.gz -C /opt/zookeeper --strip-components=1
WORKDIR /opt/zookeeper

# create config file
RUN echo "tickTime=2000" >> ./conf/zoo.cfg
RUN echo "dataDir=$DATA_FOLDER_IN_CONTAINER" >> ./conf/zoo.cfg
RUN echo "clientPort=2181" >> ./conf/zoo.cfg

# export ENV
ENV ZOOKEEPER_HOME /opt/zookeeper

# change user
RUN chown -R $USER:$USER /opt/zookeeper
USER $USER
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
    -p $ZOOKEEPER_PORT:2181 \
    -v "$DATA_FOLDER":$DATA_FOLDER_IN_CONTAINER \
    "$IMAGE_NAME" ./bin/zkServer.sh start-foreground
else
  docker run -d --init \
    -p $ZOOKEEPER_PORT:2181 \
    "$IMAGE_NAME" ./bin/zkServer.sh start-foreground
fi

echo "================================================="
echo "run $DOCKER_FOLDER/start_broker.sh zookeeper.connect=$ADDRESS:$ZOOKEEPER_PORT to join kafka broker"
echo "================================================="
