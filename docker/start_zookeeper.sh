#!/bin/bash

# ===============================[global variables]===============================

declare -r USER=astraea
declare -r VERSION=${VERSION:-3.7.0}
declare -r REPO=${REPO:-astraea/zookeeper}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r PORT="$(($(($RANDOM % 10000)) + 10000))"
declare -r RUN=${RUN:-true}
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r DOCKERFILE=$DOCKER_FOLDER/zookeeper.dockerfile
declare -r DATA_FOLDER_IN_CONTAINER="/tmp/zookeeper-dir"
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_zookeeper.sh"
  echo "ENV: "
  echo "    REPO=astraea/zk            set the docker repo"
  echo "    VERSION=3.7.0              set version of zookeeper distribution"
  echo "    RUN=false                  set false if you want to build image only"
  echo "    DATA_FOLDER=/tmp/folder1   set host folders used by zookeeper"
}

function checkDocker() {
  if [[ "$(which docker)" == "" ]]; then
    echo "you have to install docker"
    exit 2
  fi
}

function checkNetwork() {
  if [[ "$ADDRESS" == "127.0.0.1" || "$ADDRESS" == "127.0.1.1" ]]; then
    echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
    exit 2
  fi
}

function generateDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
USER $USER

# download zookeeper
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/zookeeper/zookeeper-${VERSION}/apache-zookeeper-${VERSION}-bin.tar.gz
RUN mkdir /home/$USER/zookeeper
RUN tar -zxvf apache-zookeeper-${VERSION}-bin.tar.gz -C /home/$USER/zookeeper --strip-components=1
WORKDIR /home/$USER/zookeeper

# create config file
RUN echo "tickTime=2000" >> ./conf/zoo.cfg
RUN echo "dataDir=$DATA_FOLDER_IN_CONTAINER" >> ./conf/zoo.cfg
RUN echo "clientPort=2181" >> ./conf/zoo.cfg
" >"$DOCKERFILE"
}

# build image only if the image does not exist locally
function buildImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
  fi
}

# ===================================[main]===================================

if [[ "$1" == "help" ]]; then
  showHelp
  exit 0
fi

checkDocker
generateDockerfile
buildImageIfNeed

if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork

if [[ -n "$DATA_FOLDER" ]]; then
  mkdir -p "$DATA_FOLDER"
  echo "mount $DATA_FOLDER to container"
  docker run -d \
    -p $PORT:2181 \
    -v "$DATA_FOLDER":$DATA_FOLDER_IN_CONTAINER \
    $IMAGE_NAME ./bin/zkServer.sh start-foreground
else
  docker run -d \
    -p $PORT:2181 \
    $IMAGE_NAME ./bin/zkServer.sh start-foreground
fi

echo "================================================="
echo "run $DOCKER_FOLDER/start_broker.sh zookeeper.connect=$ADDRESS:$PORT to join kafka broker"
echo "================================================="
