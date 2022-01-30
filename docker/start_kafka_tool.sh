#!/bin/bash

# ===============================[global variables]===============================
declare -r VERSION=${REVISION:-${VERSION:-main}}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/kafka-tool}
declare -r DOCKERFILE=$DOCKER_FOLDER/kafka_tool.dockerfile
declare -r BUILD=${BUILD:-false}
. ./init.sh
# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_kafka_tool.sh"
  echo "ENV: "
  echo "    REPO=astraea/kafka-tool    set the docker repo"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
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



function runContainer() {
  local args=$1
  docker run --rm --init \
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
  runContainer "$*"
else
  showHelp
  runContainer "help"
fi
