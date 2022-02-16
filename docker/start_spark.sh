#!/bin/bash

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source $DOCKER_FOLDER/docker_build_common.sh

# ===============================[global variables]===============================
declare -r VERSION=${REVISION:-${VERSION:-3.1.2}}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/spark}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r SPARK_PORT=${SPARK_PORT:-"$(getRandomPort)"}
declare -r SPARK_UI_PORT=${SPARK_UI_PORT:-"$(getRandomPort)"}
declare -r DOCKERFILE=$DOCKER_FOLDER/spark.dockerfile
declare -r MASTER_NAME="spark-master"
declare -r WORKER_NAME="spark-worker"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_spark.sh master-url"
  echo "Optional Arguments: "
  echo "    master-url=spar://node00:1111    start a spark worker. Or start a spark master if master-url is not defined"
  echo "ENV: "
  echo "    VERSION=3.1.2                    set version of spark distribution"
  echo "    BUILD=false                      set true if you want to build image locally"
  echo "    RUN=false                        set false if you want to build/pull image only"
  echo "    PYTHON_DEPS=delta-spark=1.0.0    set the python dependencies which are pre-installed in the docker image"
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
FROM ubuntu:20.04 AS build

# install tools
RUN apt-get update && apt-get install -y wget unzip

# download spark
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-${VERSION}/spark-${VERSION}-bin-hadoop3.2.tgz
RUN mkdir /opt/spark
RUN tar -zxvf spark-${VERSION}-bin-hadoop3.2.tgz -C /opt/spark --strip-components=1

FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre python3 python3-pip

# copy spark
COPY --from=build /opt/spark /opt/spark

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/spark
USER $USER

# export ENV
ENV SPARK_HOME /opt/spark
WORKDIR /opt/spark
" >"$DOCKERFILE"
}

function generateDockerfileBySource() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:20.04 AS build

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jdk python3 python3-pip git curl

# build spark from source code
RUN git clone https://github.com/apache/spark /tmp/spark
WORKDIR /tmp/spark
RUN git checkout $VERSION
ENV MAVEN_OPTS=\"-Xmx3g\"
RUN ./dev/make-distribution.sh --pip --tgz
RUN mkdir /opt/spark
RUN tar -zxvf \$(find ./ -maxdepth 1 -type f -name spark-*SNAPSHOT*.tgz) -C /opt/spark --strip-components=1
RUN ./build/mvn install -DskipTests

FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre python3 python3-pip

# copy spark
COPY --from=build /opt/spark /opt/spark

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/spark
USER $USER

# export ENV
ENV SPARK_HOME /opt/spark
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

# ===================================[main]===================================

if [[ "$1" == "help" ]]; then
  showHelp
  exit 0
fi

declare -r master_url=$1

checkDocker
generateDockerfile
buildImageIfNeed "$IMAGE_NAME"

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
    "$IMAGE_NAME" ./sbin/start-worker.sh "$master_url"

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
    "$IMAGE_NAME" ./sbin/start-master.sh

  echo "================================================="
  echo "Starting Spark master at spark://$ADDRESS:$SPARK_PORT"
  echo "Bound MasterWebUI started at http://${ADDRESS}:${SPARK_UI_PORT}"
  echo "execute $DOCKER_FOLDER/start_spark.sh spark://$ADDRESS:$SPARK_PORT to add worker"
  echo "================================================="
fi
