#!/bin/bash

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source $DOCKER_FOLDER/docker_build_common.sh
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/deps}
declare -r VERSION=${VERSION:-latest}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/deps.dockerfile
# ===================================[functions]===================================
function generateDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y \
  git \
  openjdk-11-jdk \
  wget \
  unzip \
  libaio1 \
  numactl \
  libncurses5 \
  curl

# build code and download dependencies
WORKDIR /astraea
RUN git clone https://github.com/skiptests/astraea.git /astraea
RUN ./gradlew clean build -x test
# trigger download of database
RUN ./gradlew cleanTest test --tests DatabaseTest

WORKDIR /root
" >"$DOCKERFILE"
}

function buildBaseImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
    docker push $IMAGE_NAME
  else
    echo "$IMAGE_NAME is existent in local"
  fi
}
# ===================================[main]===================================
generateDockerfile
buildBaseImageIfNeed