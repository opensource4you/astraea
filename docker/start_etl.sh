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

declare -A PROPERTIES_MAP
declare -r SINK_KEY="sink_path"
declare -r SOURCE_KEY="source_path"
declare -r TOPIC_KEY="topic_name"

source $DOCKER_FOLDER/docker_build_common.sh

# ===============================[global variables]===============================
declare -r VERSION=${REVISION:-${VERSION:-main}}
declare -r ACCOUNT=${ACCOUNT:-skiptests}
declare -r IMAGE_NAME="ghcr.io/${ACCOUNT}/astraea/etl:$VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/app.dockerfile
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_etl.sh properties-path"
  echo "Optional Arguments: "
  echo "    properties-path=spar://node00:1111    start a spark worker. Or start a spark master if master-url is not defined"
  echo "ENV: "
  echo "    VERSION=3.1.2                    set version of spark distribution"
  echo "    BUILD=false                      set true if you want to build image locally"
  echo "    RUN=false                        set false if you want to build/pull image only"
  echo "    PYTHON_DEPS=delta-spark=1.0.0    set the python dependencies which are pre-installed in the docker image"
}

function generateDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM ghcr.io/skiptests/astraea/deps AS build

# clone repo
WORKDIR /tmp
RUN git clone https://github.com/$ACCOUNT/astraea

# pre-build project to collect all dependencies
WORKDIR /tmp/astraea
RUN git checkout $VERSION
RUN ./gradlew clean build -x test --no-daemon
RUN mkdir /opt/astraea
RUN tar -xvf \$(find ./etl/build/distributions/ -maxdepth 1 -type f -name app-*.tar) -C /opt/astraea/ --strip-components=1

FROM ubuntu:22.04

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

function checkConflictContainer() {
  local name=PROPERTIES_MAP[${SINK_KEY}]
  local container_names=$(docker ps --format "{{.Names}}")
  if [[ $(echo "${container_names}" | grep -P "${name}") != "" ]]; then
    echo "It is disallowed to run multiples etl in same sink path: $name."
    exit 2
  fi
}

function readProperties() {
    local path=$1
    if [ -f "$path" ]
    then
            echo "$path found."
            while IFS='=' read -r key value
            do
                    key=$(echo $key | tr '.' '_')
                    value=$(echo $value | grep -o -P '.+')

                    [[ -n $(echo "$key" | grep -P '^\w+') ]] \
                            && PROPERTIES_MAP[${key}]=${value}
            done < "$path"
    else
            echo "$path not found."
    fi
}

function runContainer() {
    local args=$1

    docker run -d --rm --init \
        -v "${PROPERTIES_MAP[${SINK_KEY}]}":/tmp/sink_path:ro \
        -v "${PROPERTIES_MAP[${SOURCE_KEY}]}":/tmp/source_path \
        -- name "csv-kafka-${PROPERTIES_MAP[${TOPIC_KEY}]}-${PROPERTIES_MAP[${SINK_KEY}]}" \
        -e JAVA_OPTS="$HEAP_OPTS" \
        "$IMAGE_NAME" \
        /opt/astraea/bin/etl $args
}

# ===================================[main]===================================

checkDocker
generateDockerfile
buildImageIfNeed "$IMAGE_NAME"

if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

if [[ -n "$1" ]]; then
  readProperties "$*"
  runContainer "$*"
else
  showHelp
fi
