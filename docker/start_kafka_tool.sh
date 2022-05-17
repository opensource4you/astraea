#!/bin/bash

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source $DOCKER_FOLDER/docker_build_common.sh

# ===============================[global variables]===============================
declare -r VERSION=${REVISION:-${VERSION:-main}}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/kafka-tool}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/kafka_tool.dockerfile
declare -r JMX_PORT=${JMX_PORT:-"$(getRandomPort)"}
# for web service
declare -r WEB_PORT=${WEB_PORT:-"$(getRandomPort)"}
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
                     -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT -Djava.rmi.server.hostname=$ADDRESS"
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"
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
FROM ubuntu:22.04 AS build

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
  if [[ "$args" == web* ]]; then
    background="-d"
    sentence=($args)
    defined_port="false"
    # use random port by default
    web_port="$WEB_PORT"
    for word in "${sentence[@]}"; do
      # user has pre-defined port, so we will replace the random port by this one in next loop
      if [[ "$word" == "--port" ]]; then
        defined_port="true"
      fi
      # this element must be port
      if [[ "$defined_port" == "true" ]]; then
        web_port="$word"
      fi
    done
    # manually add "--port" to make sure web service bind on random port we pass
    if [[ "$defined_port" == "false" ]]; then
      args="$args --port $web_port"
    fi
    need_to_bind_web="-p $web_port:$web_port"
    echo "web address: $ADDRESS:$web_port"
  fi

  docker run --rm --init \
    $background \
    -p $JMX_PORT:$JMX_PORT \
    $need_to_bind_web \
    "$IMAGE_NAME" \
    /bin/bash -c "java $JMX_OPTS $HEAP_OPTS -jar /opt/astraea/app.jar $args"
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
