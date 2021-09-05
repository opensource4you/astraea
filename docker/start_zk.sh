#!/bin/bash

# =============================[functions]=============================
function getAddress(){
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

USER=zookeeper
ZK_VERSION=3.6.3
image_name=astraea/zookeeper
zk_port="$(($(($RANDOM % 10000 )) + 10000))"
address=$(getAddress)

docker build -t $image_name - <<Dockerfile
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
USER $USER

# download zookeeper
WORKDIR /home/$USER
RUN wget https://dlcdn.apache.org/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz
RUN tar -zxvf apache-zookeeper-${ZK_VERSION}-bin.tar.gz
WORKDIR /home/$USER/apache-zookeeper-${ZK_VERSION}-bin

# create config file
RUN echo "tickTime=2000" >> ./conf/zoo.cfg
RUN echo "dataDir=/tmp/zookeeper" >> ./conf/zoo.cfg
RUN echo "clientPort=2181" >> ./conf/zoo.cfg
Dockerfile

docker run -d \
  -p $zk_port:2181 \
  $image_name /home/$USER/apache-zookeeper-${ZK_VERSION}-bin/bin/zkServer.sh start-foreground

echo "================================================="
echo "run ./docker/start_broker.sh zookeeper.connect=$address:$zk_port to join kafka broker"
echo "================================================="