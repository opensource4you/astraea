#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configure the path of Spark2Kafka.properties to run. For example: ./docker/start_etl.sh PropertiesPath
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source "$DOCKER_FOLDER"/docker_build_common.sh

# ===============================[version control]=================================
declare -r SPARK_VERSION=${SPARK_VERSION:-3.1.2}
declare -r ASTRAEA_VERSION=${ASTRAEA_VERSION:-0.0.1}
# ===============================[global variables]================================
declare -r VERSION=${REVISION:-${VERSION:-main}}
declare -r ACCOUNT=${ACCOUNT:-skiptests}
declare -r IMAGE_NAME="ghcr.io/${ACCOUNT}/astraea/etl:$VERSION"
declare -r LOCAL_PATH=$(cd -- "$(dirname -- "${DOCKER_FOLDER}")" &>/dev/null && pwd)/etl/build/libs/astraea-etl-${ASTRAEA_VERSION}-SNAPSHOT-all.jar
declare -r DOCKERFILE=$DOCKER_FOLDER/etl.dockerfile
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx4G -Xms4G"}"
declare -r PROPERTIES=$1
# ===============================[properties keys]=================================
declare -r SINK_KEY="sink.path"
declare -r SOURCE_KEY="source.path"
declare -r TOPIC_KEY="topic.name"
declare -r CHECKPOINT_KEY="checkpoint"
# ===============================[spark driver/executor resource]==================
declare -r RESOURCES_CONFIGS="${RESOURCES_CONFIGS:-"2G"}"
# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_etl.sh properties_path"
  echo "Optional Arguments: "
  echo "    properties_path                         The path of Spark2Kafka.properties."
  echo "ENV: "
  echo "    ACCOUNT=skiptests                       set the account to clone from"
  echo "    VERSION=main                            set branch of astraea"
  echo "    BUILD=false                             set true if you want to build image locally"
  echo "    RUN=false                               set false if you want to build/pull image only"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"             set broker JVM memory"
  echo "    RESOURCES_CONFIGS=\"-Xmx2G -Xms2G\"     set spark memory"
}

function generateDockerfile() {
  if [[ "$BUILD" == "false" ]]; then
    generateDockerfileByGithub
  else
    generateDockerfileByLocal
  fi
}

function generateDockerfileByLocal() {
    echo "# this dockerfile is generated dynamically
      FROM ghcr.io/skiptests/astraea/spark:$SPARK_VERSION

      # export ENV
      ENV SPARK_HOME /opt/spark
      WORKDIR /opt/spark
      " >"$DOCKERFILE"
}

function generateDockerfileByGithub() {
  echo "# this dockerfile is generated dynamically
    FROM ghcr.io/skiptests/astraea/deps AS build

    # clone repo
    WORKDIR /tmp
    RUN git clone https://github.com/$ACCOUNT/astraea

    # pre-build project to collect all dependencies
    WORKDIR /tmp/astraea
    RUN git checkout $VERSION
    RUN ./gradlew clean shadowJar

    FROM ghcr.io/skiptests/astraea/spark:$SPARK_VERSION

    # copy astraea
    COPY --from=build /tmp/astraea /opt/astraea

    # export ENV
    ENV SPARK_HOME /opt/spark
    WORKDIR /opt/spark
    " >"$DOCKERFILE"
}

function prop() {
	[ -f "$PROPERTIES" ] && grep -P "^\s*[^#]?${1}=.*$" "$PROPERTIES" | cut -d'=' -f2
}

function checkPath() {
  mkdir -p "$1"
  if [ $? -ne 0 ]; then
      echo "mkdir $1 failed"
      exit 2
  fi
  echo "$1"
}

function runContainer() {
  if [[ "$BUILD" == "false" ]]; then
    runContainerByGithub "$1"
  else
    echo "local"
    ./gradlew clean shadowJar
    runContainerByLocal "$1"
  fi
}

#run spark submit local mode
function runContainerByGithub() {
    local propertiesPath=$1

    sink_path=$(checkPath "$(prop $SINK_KEY)")
    source_path=$(checkPath "$(prop $SOURCE_KEY)")
    topic=$(prop $TOPIC_KEY)
    checkpoint_path=$(checkPath "$(prop $CHECKPOINT_KEY)")
    source_name=$(echo "${source_path}" | tr '/' '-')

    docker run -d --init \
        --name "csv-kafka-${topic}${source_name}" \
        -v "$propertiesPath":"$propertiesPath":ro \
        -v "${sink_path}":"${sink_path}" \
        -v "${source_path}":"${source_path}":ro \
        -v "${checkpoint_path}":"${checkpoint_path}" \
        -e JAVA_OPTS="$HEAP_OPTS" \
        "$IMAGE_NAME" \
        ./bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:"$SPARK_VERSION" \
        --executor-memory "$RESOURCES_CONFIGS" \
        --class org.astraea.etl.Spark2Kafka \
        --master local \
        /opt/astraea/etl/build/libs/astraea-etl-"${ASTRAEA_VERSION}"-SNAPSHOT-all.jar \
        "$propertiesPath"
}

function runContainerByLocal() {
    local propertiesPath=$1

    sink_path=$(checkPath "$(prop $SINK_KEY)")
    source_path=$(checkPath "$(prop $SOURCE_KEY)")
    topic=$(prop $TOPIC_KEY)
    checkpoint_path=$(checkPath "$(prop $CHECKPOINT_KEY)")
    source_name=$(echo "${source_path}" | tr '/' '-')

    docker run -d --init \
        --name "csv-kafka-${topic}${source_name}" \
        -v "$propertiesPath":"$propertiesPath":ro \
        -v "${sink_path}":"${sink_path}" \
        -v "${source_path}":"${source_path}":ro \
        -v "${checkpoint_path}":"${checkpoint_path}" \
        -v "${LOCAL_PATH}":"${LOCAL_PATH}":ro \
        -e JAVA_OPTS="$HEAP_OPTS" \
        "$IMAGE_NAME" \
        ./bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:"$SPARK_VERSION" \
        --executor-memory "$RESOURCES_CONFIGS" \
        --class org.astraea.etl.Spark2Kafka \
        --master local \
        "$LOCAL_PATH" \
        "$propertiesPath"
}

# ===================================[main]===================================
checkDocker
generateDockerfile
buildImageIfNeed "$IMAGE_NAME"

if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

if [[ -n "$1" ]]; then
  runContainer "$*"
else
  showHelp
fi
