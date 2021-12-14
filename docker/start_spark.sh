#!/bin/bash

function showHelp() {
  echo "Usage: [ENV] start_spark.sh master-url"
  echo "Optional Arguments: "
  echo "    master-url=spar://node00:1111    start a spark worker. Or start a spark master if master-url is not defined"
  echo "ENV: "
  echo "    REPO=astraea/spark               set the docker repo"
  echo "    VERSION=3.1.2                    set version of spark distribution"
  echo "    RUN=false                        set false if you want to build image only"
}

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

if [[ "$(which ipconfig)" != "" ]]; then
  address=$(ipconfig getifaddr en0)
else
  address=$(hostname -i)
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

# Spark needs to manage the hardware resource for this node, so we don't run multiples workers/masters in same node.
if [[ -n "$master_url" ]]; then
  master_port=$(echo "$master_url" | cut -d':' -f 3)
  worker_name="spark-worker-$master_port"
  container_names=$(docker ps --format "{{.Names}}")
  if [[ $(echo "${container_names}" | grep "$worker_name") != "" ]]; then
    echo "It is disallowed to run multiples spark workers in same node"
    exit 2
  fi
else
  master_name="spark-master"
  container_names=$(docker ps --format "{{.Names}}")
  if [[ $(echo "${container_names}" | grep "$master_name") != "" ]]; then
    echo "It is disallowed to run multiples spark masters in same node"
    exit 2
  fi
fi


spark_user=astraea
version=${REVISION:-${VERSION:-3.1.2}}
repo=${REPO:-astraea/spark}
image_name="$repo:$version"
run_container=${RUN:-true}
spark_port="$(($(($RANDOM % 10000)) + 10000))"
spark_ui_port="$(($(($RANDOM % 10000)) + 10000))"

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
RUN wget https://archive.apache.org/dist/spark/spark-${version}/spark-${version}-bin-hadoop3.2.tgz
RUN mkdir /home/$spark_user/spark
RUN tar -zxvf spark-${version}-bin-hadoop3.2.tgz -C /home/$spark_user/spark --strip-components=1
ENV SPARK_MASTER_WEBUI_PORT=$spark_ui_port
ENV SPARK_WORKER_WEBUI_PORT=$spark_ui_port
ENV SPARK_MASTER_PORT=$spark_port
ENV SPARK_WORKER_PORT=$spark_port
ENV SPARK_NO_DAEMONIZE=true
WORKDIR /home/$spark_user/spark

Dockerfile

if [[ "$run_container" != "true" ]]; then
  echo "docker image: $image_name is created"
  exit 0
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
  echo "This script requires to run container with \"--network host\", but the feature is unsupported by Mac OS"
  exit 2
fi

if [[ "$address" == "127.0.0.1" || "$address" == "127.0.1.1" ]]; then
  echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
  exit 2
fi


if [[ -n "$master_url" ]]; then
  docker run -d \
    --name "$worker_name" \
    --network host \
    $image_name ./sbin/start-worker.sh "$master_url"

  echo "================================================="
  echo "Starting Spark worker $address:$spark_port"
  echo "Bound WorkerWebUI started at http://${address}:${spark_ui_port}"
  echo "================================================="
else
  docker run -d \
    --name "$master_name" \
    --network host \
    $image_name ./sbin/start-master.sh

  echo "================================================="
  echo "Starting Spark master at spark://$address:$spark_port"
  echo "Bound MasterWebUI started at http://${address}:${spark_ui_port}"
  echo "execute ./docker/start_spark.sh spark://$address:$spark_port to add worker"
  echo "================================================="
fi
