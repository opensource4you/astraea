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

# download gradle 5 for previous kafka having no built-in gradlew
WORKDIR /tmp
RUN wget https://downloads.gradle-dn.com/distributions/gradle-5.6.4-bin.zip
RUN unzip gradle-5.6.4-bin.zip

# build code and download dependencies
WORKDIR /astraea
RUN git clone https://github.com/skiptests/astraea.git /astraea
RUN ./gradlew clean build -x test
# trigger download of database
RUN ./gradlew cleanTest it:test --tests DatabaseTest

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