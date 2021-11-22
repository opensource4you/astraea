#!/bin/bash

# =============================[functions]=============================
function getAddress() {
  if [[ "$(which ipconfig)" != "" ]]; then
    address=$(ipconfig getifaddr en0)
  else
    address=$(hostname -i)
  fi
  if [[ "$address" == "127.0.0.1" ]]; then
    echo "the address: 127.0.0.1 can't be used in this script. Please check /etc/hosts"
    exit 2
  fi
  echo "$address"
}
# =====================================================================

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

if [[ -z "$ZOOKEEPER_VERSION" ]]; then
  ZOOKEEPER_VERSION=3.7.0
fi

zookeeper_user=astraea
image_name=astraea/zookeeper:$ZOOKEEPER_VERSION
zk_port="$(($(($RANDOM % 10000)) + 10000))"
address=$(getAddress)

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
RUN wget https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
RUN mkdir /home/$zookeeper_user/zookeeper
RUN tar -zxvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz -C /home/$zookeeper_user/zookeeper --strip-components=1
WORKDIR /home/$zookeeper_user/zookeeper

# create config file
RUN echo "tickTime=2000" >> ./conf/zoo.cfg
RUN echo "dataDir=/tmp/zookeeper" >> ./conf/zoo.cfg
RUN echo "clientPort=2181" >> ./conf/zoo.cfg
Dockerfile

docker run -d \
  -p $zk_port:2181 \
  $image_name ./bin/zkServer.sh start-foreground

echo "================================================="
echo "run ./docker/start_broker.sh zookeeper.connect=$address:$zk_port to join kafka broker"
echo "================================================="
