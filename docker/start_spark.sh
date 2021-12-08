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

function showHelp() {
  echo "Usage: [ENV] start_spark.sh master-url"
  echo "ENV: "
  echo "    SPARK_VERSION=3.7.0    set version of spark distribution"
}

# =====================================================================

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

master_url=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 2
  fi
  master_url=$1
  shift
done

if [[ -z "$SPARK_VERSION" ]]; then
  SPARK_VERSION=3.1.2
fi

spark_user=astraea
image_name=astraea/spark:$SPARK_VERSION
spark_port="$(($(($RANDOM % 10000)) + 10000))"
spark_ui_port="$(($(($RANDOM % 10000)) + 10000))"
address=$(getAddress)

docker build -t $image_name - <<Dockerfile
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget python3

# add user
RUN groupadd $spark_user && useradd -ms /bin/bash -g $spark_user $spark_user

# change user
USER $spark_user

# download spark
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz
RUN mkdir /home/$spark_user/spark
RUN tar -zxvf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz -C /home/$spark_user/spark --strip-components=1
ENV SPARK_MASTER_WEBUI_PORT=$spark_ui_port
ENV SPARK_WORKER_WEBUI_PORT=$spark_ui_port
ENV SPARK_MASTER_PORT=$spark_port
ENV SPARK_NO_DAEMONIZE=true
ENV SPARK_PUBLIC_DNS=$address
WORKDIR /home/$spark_user/spark

Dockerfile

if [[ -n "$master_url" ]]; then
  # worker mode
    docker run -d \
      -p $spark_ui_port:$spark_ui_port \
      $image_name ./sbin/start-worker.sh "$master_url"

    echo "================================================="
    echo "Bound WorkerWebUI started at http://${address}:${spark_ui_port}"
    echo "================================================="
else
    docker run -d \
      -p $spark_port:$spark_port \
      -p $spark_ui_port:$spark_ui_port \
      $image_name ./sbin/start-master.sh

    echo "================================================="
    echo "Starting Spark master at spark://$address:$spark_port"
    echo "Bound MasterWebUI started at http://${address}:${spark_ui_port}"
    echo "execute ./docker/start_spark.sh spark://$address:$spark_port to add worker"
    echo "================================================="

fi


