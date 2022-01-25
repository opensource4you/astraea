#!/bin/bash

# ===============================[global variables]===============================

declare -r USER=astraea
declare -r VERSION=${REVISION:-${VERSION:-3.1.2}}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/spark}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r BUILD=${BUILD:-false}
declare -r RUN=${RUN:-true}
declare -r SPARK_PORT=${SPARK_PORT:-$(($(($RANDOM % 10000)) + 10000))}
declare -r SPARK_UI_PORT=${SPARK_UI_PORT:-$(($(($RANDOM % 10000)) + 10000))}
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
  echo "    VERSION=3.1.2                    set version of spark distribution"
  echo "    DELTA_VERSION=1.0.0              set version of delta distribution"
  echo "    PYTHON_KAFKA_VERSION=1.7.0       set version of confluent kafka distribution"
  echo "    BUILD=false                      set true if you want to build image locally"
  echo "    RUN=false                        set false if you want to build/pull image only"
  echo "    PYTHON_DEPS=delta-spark=1.0.0    set the python dependencies which are pre-installed in the docker image"
}

function checkDocker() {
  if [[ "$(which docker)" == "" ]]; then
    echo "you have to install docker"
    exit 2
  fi
}

function checkNetwork() {
  if [[ "$ADDRESS" == "127.0.0.1" || "$ADDRESS" == "127.0.1.1" ]]; then
    echo "Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
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

function generateDockerfileByVersion() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget python3 python3-pip unzip tini

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# download spark
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-${VERSION}/spark-${VERSION}-bin-hadoop3.2.tgz
RUN mkdir /opt/spark
RUN tar -zxvf spark-${VERSION}-bin-hadoop3.2.tgz -C /opt/spark --strip-components=1

# export ENV
ENV SPARK_HOME /opt/spark

# change user
RUN chown -R $USER:$USER /opt/spark
USER $USER

WORKDIR /opt/spark
" >"$DOCKERFILE"
}

function generateDockerfileBySource() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk python3 python3-pip git curl

# install python dependencies
RUN pip3 install confluent-kafka==$PYTHON_KAFKA_VERSION

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# build spark from source code
RUN git clone https://github.com/apache/spark /tmp/spark
WORKDIR /tmp/spark
RUN git checkout $VERSION
RUN ./dev/make-distribution.sh --pip --tgz
RUN mkdir /opt/spark
RUN tar -zxvf \$(find ./ -maxdepth 1 -type f -name spark-*SNAPSHOT*.tgz) -C /opt/spark --strip-components=1
RUN ./build/mvn install -DskipTests

# export ENV
ENV SPARK_HOME /opt/spark

# change user
RUN chown -R $USER:$USER /opt/spark
USER $USER

WORKDIR /opt/spark
" >"$DOCKERFILE"
}

function generateDockerfile() {
  if [[ -n "$REVISION" ]]; then
    generateDockerfileBySource
  else
    generateDockerfileByVersion
  fi
}

function buildImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    local needToBuild="true"
    if [[ "$BUILD" == "false" ]]; then
      docker pull $IMAGE_NAME 2>/dev/null
      if [[ "$?" == "0" ]]; then
        needToBuild="false"
      else
        echo "Can't find $IMAGE_NAME from repo. Will build $IMAGE_NAME on the local"
      fi
    fi
    if [[ "$needToBuild" == "true" ]]; then
      generateDockerfile
      docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
      if [[ "$?" != "0" ]]; then
        exit 2
      fi
    fi
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
  docker run -d --init \
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
  docker run -d --init \
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
