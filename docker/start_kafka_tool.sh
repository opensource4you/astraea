#!/bin/bash

# =============================[functions]=============================

function showHelp() {
  echo "Usage: [ENV] start_kafka_tool.sh [ ARGUMENTS ]"
  echo "ENV: "
  echo "    REPO=astraea/broker                      set the docker repo"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"                set broker JVM memory"
  echo "    REVISION=trunk                           set revision of kafka source code to build container"
  echo "    VERSION=2.8.1                            set version of kafka distribution"
}

# ===============================[checks]===============================

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

if [[ "$(which ipconfig)" != "" ]]; then
  address=$(ipconfig getifaddr en0)
else
  address=$(hostname -i)
fi

if [[ "$address" == "127.0.0.1" || "$address" == "127.0.1.1" ]]; then
  echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
  exit 2
fi

# =================================[main]=================================

if [[ "$1" == "" ]]; then
  showHelp
  exit 0
else
  script=$1
  # remove first command lines
  shift 1
fi

version=${REVISION:-${VERSION:-2.8.1}}
repo=${REPO:-astraea/broker}
image_name="$repo:$version"

heap_opts="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"

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
RUN wget https://archive.apache.org/dist/kafka/${version}/kafka_2.13-${version}.tgz
RUN mkdir /home/$kafka_user/kafka
RUN tar -zxvf kafka_2.13-${version}.tgz -C /home/$kafka_user/kafka --strip-components=1
WORKDIR "/home/$kafka_user/kafka"

Dockerfile

if [[ "$script" == "help" ]]; then
  docker run --rm $image_name /bin/bash -c "ls ./bin"
else
  docker run --rm -ti \
    -e KAFKA_HEAP_OPTS="$heap_opts" \
    $image_name ./bin/"$script" "$@"
fi
