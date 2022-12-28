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
declare -r SPARK_VERSION=${SPARK_VERSION:-3.3.1}
# ===============================[global variables]================================
declare -r LOCAL_PATH=$(cd -- "$(dirname -- "${DOCKER_FOLDER}")" &>/dev/null && pwd)
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx4G -Xms4G"}"
# ===============================[properties keys]=================================
declare -r SOURCE_KEY="source.path"
# ===============================[spark driver/executor resource]==================
declare -r RESOURCES_CONFIGS="${RESOURCES_CONFIGS:-"2G"}"
# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_etl.sh properties_path"
  echo "required Arguments: "
  echo "    master                         where to submit spark job"
  echo "    property.file                         The path of Spark2Kafka.properties."
  echo "ENV: "
  echo "    ACCOUNT=skiptests                       set the account to clone from"
  echo "    VERSION=main                            set branch of astraea"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"             set broker JVM memory"
  echo "    RESOURCES_CONFIGS=\"-Xmx2G -Xms2G\"     set spark memory"
}

function runContainer() {
  local master=$1
  local propertiesPath=$2

  if [[ ! -f "$propertiesPath" ]]; then
    echo "$propertiesPath is not a property file"
    exit 1
  fi

  source_path=$(cat $propertiesPath | grep $SOURCE_KEY | cut -d "=" -f2)
  source_name=$(echo "${source_path}" | tr '/' '-')

  docker run --rm -v "$LOCAL_PATH":/tmp/astraea \
    ghcr.io/skiptests/astraea/deps \
    /bin/bash \
    -c "cd /tmp/astraea && ./gradlew clean shadowJar --no-daemon"

  jar_path=$(find "$LOCAL_PATH"/etl/build/libs -type f -name "*all.jar")

  if [[ ! -f "$jar_path" ]]; then
    echo "$jar_path is not a uber jar"
    exit 1
  fi

  # the driver is running on client mode, so the source path must be readable from following container.
  # hence, we will mount source path to container directly
  mkdir -p "$source_path"
  if [ $? -ne 0 ]; then
    echo "failed to create folder on $source_path"
    exit 1
  fi

  ui_port=$(($(($RANDOM % 10000)) + 10000))
  if [[ "$master" == "local"* ]]; then
    network_config="-p ${ui_port}:${ui_port}"
  else
    # expose the driver network
    network_config="--network host"
  fi

  if [[ "$master" == "spark:"* ]] || [[ "$master" == "local"* ]]; then
    docker run -d --init \
      --name "csv-kafka-${source_name}" \
      $network_config \
      -v "$propertiesPath":"$propertiesPath":ro \
      -v "$jar_path":/tmp/astraea-etl.jar:ro \
      -v "${source_path}":"${source_path}" \
      -e JAVA_OPTS="$HEAP_OPTS" \
      ghcr.io/skiptests/astraea/spark:$SPARK_VERSION \
      ./bin/spark-submit \
      --conf "spark.ui.port=$ui_port" \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:"$SPARK_VERSION" \
      --executor-memory "$RESOURCES_CONFIGS" \
      --class org.astraea.etl.Spark2Kafka \
      --jars file:///tmp/astraea-etl.jar \
      --master $master \
      /tmp/astraea-etl.jar \
      "$propertiesPath"
  else
    echo "$master is unsupported"
    exit 1
  fi

}

# ===================================[main]===================================
checkDocker

master="local[*]"
property_file_path=""

while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi

  if [[ "$1" == "master"* ]]; then
    master=$(echo "$1" | cut -d "=" -f 2)
  fi

  if [[ "$1" == "property.file"* ]]; then
    property_file_path=$(echo "$1" | cut -d "=" -f 2)
  fi

  shift
done

if [[ "$property_file_path" == "" ]]; then
    showHelp
    exit 0
fi

runContainer "$master" "$property_file_path"
