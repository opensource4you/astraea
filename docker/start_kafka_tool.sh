#!/bin/bash

# =============================[functions]=============================
function showHelp() {
  echo "Usage: [ KAFKA SCRIPT ] [ OPTIONS ]"
}
# =====================================================================

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

if [[ "$1" == "" ]]; then
  showHelp
  exit 2
else
  script=$1
  # remove first command lines
  shift 1
fi

if [[ -z "$KAFKA_VERSION" ]]; then
  KAFKA_VERSION=2.8.1
fi
image_name=astraea/kafka-tool:$KAFKA_VERSION

# set JVM heap
KAFKA_HEAP="${KAFKA_HEAP:-"-Xmx2G -Xms2G"}"

kafka_user=astraea

docker build -t $image_name - <<Dockerfile
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget

# add user
RUN groupadd $kafka_user && useradd -ms /bin/bash -g $kafka_user $kafka_user

# change user
USER $kafka_user

# download kafka
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz
RUN mkdir /home/$kafka_user/kafka
RUN tar -zxvf kafka_2.13-${KAFKA_VERSION}.tgz -C /home/$kafka_user/kafka --strip-components=1
WORKDIR "/home/$kafka_user/kafka"

Dockerfile

if [[ "$script" == "help" ]]; then
  docker run --rm $image_name /bin/bash -c "ls ./bin"
else
  docker run --rm -ti \
    -e KAFKA_HEAP_OPTS="$KAFKA_HEAP" \
    $image_name ./bin/"$script" "$@"
fi
