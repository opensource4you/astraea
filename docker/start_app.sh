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

# ===============================[global variables]===============================
declare -r VERSION=${REVISION:-${VERSION:-main}}
declare -r ACCOUNT=${ACCOUNT:-skiptests}
declare -r IMAGE_NAME="ghcr.io/${ACCOUNT}/astraea/app:$VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/app.dockerfile
declare -r JMX_PORT=${JMX_PORT:-"$(getRandomPort)"}
# for web service
declare -r WEB_PORT=${WEB_PORT:-"$(getRandomPort)"}
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
                     -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT -Djava.rmi.server.hostname=$ADDRESS"
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"
# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_app.sh"
  echo "ENV: "
  echo "    ACCOUNT=skiptests          set the account to clone from"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
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
RUN tar -xvf \$(find ./app/build/distributions/ -maxdepth 1 -type f -name app-*.tar) -C /opt/astraea/ --strip-components=1

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

function runContainer() {
  local args=$1
  echo "JMX address: $ADDRESS:$JMX_PORT"

  # web service needs to bind a port to expose Restful APIs, so we have to open a port of container
  local need_to_bind_web=""
  local background=""
  local sentence=($args)
  if [[ "$args" == web* ]]; then
    background="-d"
    defined_port="false"
    # use random port by default
    web_port="$WEB_PORT"
    for word in "${sentence[@]}"; do
      # user has pre-defined port, so we will replace the random port by this one in next loop
      if [[ "$word" == "--port" ]]; then
        defined_port="true"
        continue
      fi
      # this element must be port
      if [[ "$defined_port" == "true" ]]; then
        web_port="$word"
        break
      fi
    done
    # manually add "--port" to make sure web service bind on random port we pass
    if [[ "$defined_port" == "false" ]]; then
      args="$args --port $web_port"
    fi
    need_to_bind_web="-p $web_port:$web_port"
    echo "web address: $ADDRESS:$web_port"
  fi

  local need_to_bind_file=""
  local defined_file="false"
  for word in "${sentence[@]}"; do
    # user has pre-defined directories/files, so we will mount directories/files
    if [[ "$word" == "--prop.file" || "$word" == "--report.path" ]]; then
      defined_file="true"
      continue
    fi
    # this element must be something to mount
    if [[ "$defined_file" == "true" ]]; then
      need_to_bind_file="${need_to_bind_file} -v $word:$word"
      defined_file="false"
    fi
  done

  docker run --rm --init \
    $background \
    -e JAVA_OPTS="$JMX_OPTS $HEAP_OPTS" \
    -p $JMX_PORT:$JMX_PORT \
    $need_to_bind_web \
    $need_to_bind_file \
    "$IMAGE_NAME" \
    /opt/astraea/bin/app $args
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
  runContainer "$*"
else
  showHelp
  runContainer "help"
fi
