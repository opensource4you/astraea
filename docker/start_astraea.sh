#!/bin/bash

# ===============================[global variables]===============================

IMAGE_NAME="astraea/astraea:latest"
DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
DOCKER_FILE=$DOCKER_FOLDER/astraea.dockerfile

# ===================================[functions]===================================

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
RUN ./gradlew clean build -x test --no-daemon
RUN cp \$(find ./app/build/libs/ -maxdepth 1 -type f -name app-*-all.jar) /tmp/app.jar
" >"$DOCKER_FILE"
}

function buildImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKER_FILE" "$DOCKER_FOLDER"
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
if [[ -n "$1" ]]; then
  runContainer "$*"
else
  runContainer "help"
fi
