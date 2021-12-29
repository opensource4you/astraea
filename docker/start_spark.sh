#!/bin/bash

# ===============================[global variables]===============================

declare -r USER=astraea
declare -r VERSION=${REVISION:-${VERSION:-3.1.2}}
declare -r REPO=${REPO:-astraea/spark}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r RUN=${RUN:-true}
declare -r SPARK_PORT="$(($(($RANDOM % 10000)) + 10000))"
declare -r SPARK_UI_PORT="$(($(($RANDOM % 10000)) + 10000))"
declare -r IVY_VERSION=2.5.0
declare -r DELTA_VERSION=${DELTA_VERSION:-1.0.0}
declare -r PYTHON_KAFKA_VERSION=${PYTHON_KAFKA_VERSION:-1.7.0}
# hardcode VERSION to avoid NPE (see https://issues.apache.org/jira/browse/HADOOP-16410)
declare -r hadoop_VERSION=3.2.2
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r DOCKERFILE=$DOCKER_FOLDER/spark.dockerfile
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)
declare -r MASTER_NAME="spark-master"
declare -r WORKER_NAME="spark-worker"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_spark.sh master-url"
  echo "Optional Arguments: "
  echo "    master-url=spar://node00:1111    start a spark worker. Or start a spark master if master-url is not defined"
  echo "ENV: "
  echo "    REPO=astraea/spark               set the docker repo"
  echo "    VERSION=3.1.2                    set version of spark distribution"
  echo "    DELTA_VERSION=1.0.0              set version of delta distribution"
  echo "    PYTHON_KAFKA_VERSION=1.7.0           set version of confluent kafka distribution"
  echo "    RUN=false                        set false if you want to build image only"
  echo "    PYTHON_DEPS=delta-spark=1.0.0    those dependencies will be pre-installed in the docker image"
}

function checkDocker() {
  if [[ "$(which docker)" == "" ]]; then
    echo "you have to install docker"
    exit 2
  fi
}

function checkNetwork() {
  if [[ "$ADDRESS" == "127.0.0.1" || "$ADDRESS" == "127.0.1.1" ]]; then
    echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
    exit 2
  fi
}

function checkOs() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "This script requires to run container with \"--network host\", but the feature is unsupported by Mac OS"
    exit 2
  fi
}

# Spark needs to manage the hardware resource for this node, so we don't run multiples workers/masters in same node.
function checkConflictContainer() {
  local name=$1
  local role=$2
  local container_names=$(docker ps --format "{{.Names}}")
  if [[ $(echo "${container_names}" | grep "$name") != "" ]]; then
    echo "It is disallowed to run multiples spark $role in same node"
    exit 2
  fi
}

function generateDockerfile() {
  echo "# this DOCKERFILE is generated dynamically
FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget python3 python3-pip unzip

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
USER $USER

# pre-install python dependencies
RUN pip3 install confluent-kafka==$PYTHON_KAFKA_VERSION delta-spark==$DELTA_VERSION pyspark==$VERSION

# install java dependencies
WORKDIR /tmp
RUN wget https://dlcdn.apache.org//ant/ivy/${IVY_VERSION}/apache-ivy-${IVY_VERSION}-bin.zip
RUN unzip apache-ivy-${IVY_VERSION}-bin.zip
WORKDIR apache-ivy-${IVY_VERSION}
RUN java -jar ./ivy-${IVY_VERSION}.jar -dependency io.delta delta-core_2.12 $DELTA_VERSION
RUN java -jar ./ivy-${IVY_VERSION}.jar -dependency org.apache.spark spark-sql-kafka-0-10_2.12 $VERSION
RUN java -jar ./ivy-${IVY_VERSION}.jar -dependency org.apache.spark spark-token-provider-kafka-0-10_2.12 $VERSION
RUN java -jar ./ivy-${IVY_VERSION}.jar -dependency org.apache.hadoop hadoop-azure $hadoop_VERSION

# download spark
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-${VERSION}/spark-${VERSION}-bin-hadoop3.2.tgz
RUN mkdir /home/$USER/spark
RUN tar -zxvf spark-${VERSION}-bin-hadoop3.2.tgz -C /home/$USER/spark --strip-components=1
WORKDIR /home/$USER/spark
" >"$DOCKERFILE"
}

# build image only if the image does not exist locally
function buildImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
  fi
}

# ===================================[main]===================================

if [[ "$1" == "help" ]]; then
  showHelp
  exit 0
fi

declare -r master_url=$1

checkDocker
generateDockerfile
buildImageIfNeed

if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork
checkOs

if [[ -n "$master_url" ]]; then
  checkConflictContainer $WORKER_NAME "worker"
  docker run -d \
    -e SPARK_WORKER_WEBUI_PORT=$SPARK_UI_PORT \
    -e SPARK_WORKER_PORT=$SPARK_PORT \
    -e SPARK_NO_DAEMONIZE=true \
    --name "$WORKER_NAME" \
    --network host \
    $IMAGE_NAME ./sbin/start-worker.sh "$master_url"

  echo "================================================="
  echo "Starting Spark worker $ADDRESS:$SPARK_PORT"
  echo "Bound WorkerWebUI started at http://${ADDRESS}:${SPARK_UI_PORT}"
  echo "================================================="
else
  checkConflictContainer $MASTER_NAME "master"
  docker run -d \
    -e SPARK_MASTER_WEBUI_PORT=$SPARK_UI_PORT \
    -e SPARK_MASTER_PORT=$SPARK_PORT \
    -e SPARK_NO_DAEMONIZE=true \
    --name "$MASTER_NAME" \
    --network host \
    $IMAGE_NAME ./sbin/start-master.sh

  echo "================================================="
  echo "Starting Spark master at spark://$ADDRESS:$SPARK_PORT"
  echo "Bound MasterWebUI started at http://${ADDRESS}:${SPARK_UI_PORT}"
  echo "execute $DOCKER_FOLDER/start_spark.sh spark://$ADDRESS:$SPARK_PORT to add worker"
  echo "================================================="
fi
