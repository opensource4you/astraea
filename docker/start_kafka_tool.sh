#!/bin/bash

# ===============================[global variables]===============================

declare -r VERSION=${REVISION:-${VERSION:-main}}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/kafka-tool}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r DOCKERFILE=$DOCKER_FOLDER/kafka_tool.dockerfile
declare -r BUILD=${BUILD:-false}
declare -r RUN=${RUN:-true}
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)
declare -r JMX_PORT=${JMX_PORT:-$(($(($RANDOM % 10000)) + 10000))}
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
                     -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT -Djava.rmi.server.hostname=$ADDRESS"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_kafka_tool.sh"
  echo "ENV: "
  echo "    REPO=astraea/kafka-tool    set the docker repo"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
}

function checkDocker() {
  if [[ "$(which docker)" == "" ]]; then
    echo "you have to install docker"
    exit 2
  fi
}

function generateDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:20.04 AS build

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jdk git curl

# clone repo
WORKDIR /tmp
RUN git clone https://github.com/skiptests/astraea

# pre-build project to collect all dependencies
WORKDIR /tmp/astraea
RUN git checkout $VERSION
RUN ./gradlew clean build -x test --no-daemon
RUN mkdir /opt/astraea
RUN cp \$(find ./app/build/libs/ -maxdepth 1 -type f -name app-*-all.jar) /opt/astraea/app.jar

FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre

# copy astraea
COPY --from=build /opt/astraea /opt/astraea

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/astraea
USER $USER

# export ENV
ENV ASTRAEA_HOME /opt/astraea
WORKDIR /opt/astraea
" >"$DOCKERFILE"
}

function buildImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    local needToBuild="true"
    if [[ "$BUILD" == "false" ]]; then
      docker pull $IMAGE_NAME 2>/dev/null
      if [[ "$?" == "0" ]]; then
        needToBuild="false"
      else
        echo "Can't find $IMAGE_NAME from repo. Will build $IMAGE_NAME on the local"
      fi
    fi
    if [[ "$needToBuild" == "true" ]]; then
      generateDockerfile
      docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
      if [[ "$?" != "0" ]]; then
        exit 2
      fi
    fi
  fi
}

function runContainer() {
  local args=$1
  echo "JMX address: $ADDRESS:$JMX_PORT"
  docker run --rm --init \
    -p $JMX_PORT:$JMX_PORT \
    $IMAGE_NAME \
    /bin/bash -c "java $JMX_OPTS -jar /opt/astraea/app.jar $args"
}

# ===================================[main]===================================

checkDocker
generateDockerfile
buildImageIfNeed

if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

if [[ -n "$1" ]]; then
  runContainer "$*"
else
  showHelp
  runContainer "help"
fi
