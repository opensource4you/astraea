#!/bin/bash

# =============================[functions]=============================

function showHelp() {
  echo "Usage: [ENV] start_zookeeper.sh"
  echo "ENV: "
  echo "    REPO=astraea/zk     set the docker repo"
  echo "    VERSION=3.7.0    set version of zookeeper distribution"
  echo "    DATA_FOLDER=/tmp/folder1   set host folders used by zookeeper"
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

while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  shift
done

zookeeper_user=astraea
version=${VERSION:-3.7.0}
repo=${REPO:-astraea/zookeeper}
image_name="$repo:$version"
zk_port="$(($(($RANDOM % 10000)) + 10000))"

hostFolderConfigs=""
if [[ -n "$DATA_FOLDER" ]]; then
  mkdir -p "$DATA_FOLDER"
  hostFolderConfigs="-v $DATA_FOLDER:/tmp/zookeeper-dir"
fi

docker build -t $image_name - <<Dockerfile
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget

# add user
RUN groupadd $zookeeper_user && useradd -ms /bin/bash -g $zookeeper_user $zookeeper_user

# change user
USER $zookeeper_user

# download zookeeper
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/zookeeper/zookeeper-${version}/apache-zookeeper-${version}-bin.tar.gz
RUN mkdir /home/$zookeeper_user/zookeeper
RUN tar -zxvf apache-zookeeper-${version}-bin.tar.gz -C /home/$zookeeper_user/zookeeper --strip-components=1
WORKDIR /home/$zookeeper_user/zookeeper

# create config file
RUN echo "tickTime=2000" >> ./conf/zoo.cfg
RUN echo "dataDir=/tmp/zookeeper-dir" >> ./conf/zoo.cfg
RUN echo "clientPort=2181" >> ./conf/zoo.cfg
Dockerfile

docker run -d \
  -p $zk_port:2181 \
  $hostFolderConfigs \
  $image_name ./bin/zkServer.sh start-foreground

echo "================================================="
echo "folder mapping: $hostFolderConfigs"
echo "run ./docker/start_broker.sh zookeeper.connect=$address:$zk_port to join kafka broker"
echo "================================================="
