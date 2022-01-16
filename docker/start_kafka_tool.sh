#!/bin/bash

# ===============================[global variables]===============================

declare -r VERSION=${REVISION:-${VERSION:-main}}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/kafka-tool}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r DOCKERFILE=$DOCKER_FOLDER/kafka_tool.dockerfile
declare -r BUILD=${BUILD:-false}
declare -r RUN=${RUN:-true}

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
FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk git curl

# clone repo
WORKDIR /tmp
RUN git clone https://github.com/skiptests/astraea

# pre-build project to collect all dependencies
WORKDIR /tmp/astraea
RUN git checkout $VERSION
RUN ./gradlew clean build -x test --no-daemon
RUN cp \$(find ./app/build/libs/ -maxdepth 1 -type f -name app-*-all.jar) /tmp/app.jar
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
       docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
       if [[ "$?" != "0" ]]; then
         exit 2
       fi
    fi
  fi
}

function runContainer() {
  local args=$1
  docker run --rm \
    $IMAGE_NAME \
    /bin/bash -c "java -jar /tmp/app.jar $args"
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
  showHelp
  runContainer "$*"
else
  runContainer "help"
fi
