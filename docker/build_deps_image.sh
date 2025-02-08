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
declare -r REPO=${REPO:-ghcr.io/opensource4you/astraea/deps}
declare -r VERSION="latest"
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/deps.dockerfile
# ===================================[functions]===================================
function generateDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM azul/zulu-openjdk:23

# install tools
RUN apt-get update && apt-get install -y \
  git \
  wget \
  unzip \
  curl

# build code and download dependencies
WORKDIR /astraea
RUN git clone https://github.com/opensource4you/astraea.git /astraea
RUN ./gradlew clean build -x test
# download test dependencies
RUN ./gradlew clean build testClasses -x test --no-daemon

WORKDIR /kafka
RUN git clone https://github.com/apache/kafka.git /kafka
RUN ./gradlew clean build testClasses -x test --no-daemon
# download test dependencies

WORKDIR /root
" >"$DOCKERFILE"
}

function buildBaseImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    docker build --platform linux/amd64 -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
    docker push $IMAGE_NAME
  else
    echo "$IMAGE_NAME is existent in local"
  fi
}
# ===================================[main]===================================
generateDockerfile
buildBaseImageIfNeed