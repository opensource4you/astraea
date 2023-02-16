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

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source $DOCKER_FOLDER/docker_build_common.sh

# ===============================[global variables]===============================
declare -r VERSION=${VERSION:-3.3.4}
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/hadoop}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/hadoop.dockerfile
declare -r EXPORTER_VERSION="0.16.1"
declare -r EXPORTER_PORT=${EXPORTER_PORT:-"$(getRandomPort)"}
declare -r HADOOP_PORT=${HADOOP_PORT:-"$(getRandomPort)"}
declare -r HADOOP_JMX_PORT="${HADOOP_JMX_PORT:-"$(getRandomPort)"}"
declare -r HADOOP_IPC_PORT="${HADOOP_IPC_PORT:-"$(getRandomPort)"}"
declare -r HADOOP_HTTP_PORT="${HADOOP_HTTP_PORT:-"$(getRandomPort)"}"
declare -r JMX_CONFIG_FILE_IN_CONTAINER_PATH="/opt/jmx_exporter/jmx_exporter_config.yml"
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$HADOOP_JMX_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=$HADOOP_JMX_PORT \
  -Djava.rmi.server.hostname=$ADDRESS \
  -javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar=$EXPORTER_PORT:$JMX_CONFIG_FILE_IN_CONTAINER_PATH"
declare -r HDFS_SITE_XML="/tmp/${HADOOP_PORT}-hdfs.xml"
declare -r CORE_SITE_XML="/tmp/${HADOOP_PORT}-core.xml"
# cleanup the file if it is existent
[[ -f "$HDFS_SITE_XML" ]] && rm -f "$HDFS_SITE_XML"
[[ -f "$CORE_SITE_XML" ]] && rm -f "$CORE_SITE_XML"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_hadoop.sh namenode"
  echo "       or [ENV] start_hadoop.sh datanode"
  echo "ENV: "
  echo "    REPO=astraea/hadoop        set the docker repo"
  echo "    VERSION=3.3.4              set version of hadoop distribution"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
}

function generateDockerfile() {
  echo "#this dockerfile is generated dynamically
FROM ubuntu:22.04 AS build

#install tools
RUN apt-get update && apt-get install -y wget

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${EXPORTER_VERSION}/jmx_prometheus_javaagent-${EXPORTER_VERSION}.jar
RUN touch $JMX_CONFIG_FILE_IN_CONTAINER_PATH
RUN echo \"rules:\\n- pattern: \\\".*\\\"\" >> $JMX_CONFIG_FILE_IN_CONTAINER_PATH

#download hadoop
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${VERSION}/hadoop-${VERSION}.tar.gz
RUN mkdir /opt/hadoop
RUN tar -zxvf hadoop-${VERSION}.tar.gz -C /opt/hadoop --strip-components=1

FROM ubuntu:22.04

#install tools
RUN apt-get update && apt-get install -y openjdk-11-jre

#copy hadoop
COPY --from=build /opt/jmx_exporter /opt/jmx_exporter
COPY --from=build /opt/hadoop /opt/hadoop

#add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# expose JAVA_HOME for hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

#change user
RUN chown -R $USER:$USER /opt/hadoop
USER $USER

#export ENV
ENV HADOOP_HOME /opt/hadoop
WORKDIR /opt/hadoop
" >"$DOCKERFILE"
}

function rejectProperty() {
  local key=$1
  local file=$2
  if grep -q "<name>$key</name>" $file; then
    echo "$key is NOT configurable"
    exit 2
  fi
}

function requireProperty() {
  local key=$1
  local file=$2
  if ! grep -q "<name>$key</name>" $file; then
    echo "$key is required"
    exit 2
  fi
}

function setProperty() {
  local name=$1
  local value=$2
  local path=$3

  echo "<property>" >> "$path"
  echo "<name>$name</name>" >> "$path"
  echo "<value>$value</value>" >> "$path"
  echo "</property>" >> "$path"
}

function completeConfigFile() {
  echo "</configuration>" >> "$HDFS_SITE_XML"
  echo "</configuration>" >> "$CORE_SITE_XML"
}

function initArg() {

  echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" > "$HDFS_SITE_XML"
  echo "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>" >> "$HDFS_SITE_XML"
  echo "<configuration>" >> "$HDFS_SITE_XML"

  echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" > "$CORE_SITE_XML"
  echo "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>" >> "$CORE_SITE_XML"
  echo "<configuration>" >> "$CORE_SITE_XML"

  node=""
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "help" ]]; then
      showHelp
      exit 0
    fi
    if [[ "$1" == "namenode" || "$1" == "datanode" ]]; then
      node=$1
      shift
      continue
    fi
    local name=${1%=*}
    local value=${1#*=}
    if [[ "$name" == "fs.defaultFS" ]]; then
      setProperty $name $value $CORE_SITE_XML
    else
      setProperty $name $value $HDFS_SITE_XML
    fi
    shift
  done
}

# ===================================[namenode]===================================

function startNamenode() {
  local container_name=namenode-$HADOOP_PORT

  rejectProperty fs.defaultFS $CORE_SITE_XML
  rejectProperty dfs.permissions $HDFS_SITE_XML
  rejectProperty dfs.namenode.http-address $HDFS_SITE_XML
  rejectProperty dfs.namenode.datanode.registration.ip-hostname-check $HDFS_SITE_XML
  rejectProperty dfs.namenode.rpc-bind-host $HDFS_SITE_XML

  setProperty dfs.namenode.http-address 0.0.0.0:$HADOOP_HTTP_PORT $HDFS_SITE_XML
  setProperty dfs.permissions false $HDFS_SITE_XML
  setProperty dfs.namenode.datanode.registration.ip-hostname-check false $HDFS_SITE_XML
  setProperty fs.defaultFS hdfs://$ADDRESS:$HADOOP_PORT $CORE_SITE_XML
  setProperty dfs.namenode.rpc-bind-host 0.0.0.0 $HDFS_SITE_XML

  completeConfigFile
  docker run -d --init \
    --name $container_name \
    -h $ADDRESS \
    -e HDFS_NAMENODE_OPTS="$JMX_OPTS" \
    -v $HDFS_SITE_XML:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro \
    -v $CORE_SITE_XML:/opt/hadoop/etc/hadoop/core-site.xml:ro \
    -p $HADOOP_HTTP_PORT:$HADOOP_HTTP_PORT \
    -p $HADOOP_JMX_PORT:$HADOOP_JMX_PORT \
    -p $HADOOP_PORT:$HADOOP_PORT \
    -p $EXPORTER_PORT:$EXPORTER_PORT \
    "$IMAGE_NAME" /bin/bash -c "./bin/hdfs namenode -format && ./bin/hdfs namenode"

  echo "================================================="
  echo "configs: ${CORE_SITE_XML} ${HDFS_SITE_XML}"
  echo "http address: ${ADDRESS}:$HADOOP_HTTP_PORT"
  echo "jmx address: ${ADDRESS}:$HADOOP_JMX_PORT"
  echo "exporter address: ${ADDRESS}:$EXPORTER_PORT"
  echo "run $DOCKER_FOLDER/start_hadoop.sh datanode fs.defaultFS=hdfs://${ADDRESS}:$HADOOP_PORT to join datanode"
  echo "================================================="
}

# ===================================[datanode]===================================

function startDatanode() {
  local container_name=datanode-$HADOOP_PORT

  rejectProperty dfs.datanode.address $HDFS_SITE_XML
  rejectProperty dfs.datanode.ipc.address $HDFS_SITE_XML
  rejectProperty dfs.datanode.http.address $HDFS_SITE_XML
  rejectProperty dfs.datanode.use.datanode.hostname $HDFS_SITE_XML
  rejectProperty dfs.client.use.datanode.hostname $HDFS_SITE_XML
  rejectProperty dfs.datanode.hostname $HDFS_SITE_XML
  requireProperty fs.defaultFS $CORE_SITE_XML

  setProperty dfs.datanode.address 0.0.0.0:$HADOOP_PORT $HDFS_SITE_XML
  setProperty dfs.datanode.ipc.address 0.0.0.0:$HADOOP_IPC_PORT $HDFS_SITE_XML
  setProperty dfs.datanode.http.address 0.0.0.0:$HADOOP_HTTP_PORT $HDFS_SITE_XML
  setProperty dfs.datanode.use.datanode.hostname true $HDFS_SITE_XML
  setProperty dfs.client.use.datanode.hostname true $HDFS_SITE_XML
  setProperty dfs.datanode.hostname $ADDRESS $HDFS_SITE_XML

  completeConfigFile
  docker run -d --init \
    --name $container_name \
    -h $ADDRESS \
    -e HDFS_DATANODE_OPTS="$JMX_OPTS" \
    -v $HDFS_SITE_XML:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro \
    -v $CORE_SITE_XML:/opt/hadoop/etc/hadoop/core-site.xml:ro \
    -p $HADOOP_HTTP_PORT:$HADOOP_HTTP_PORT \
    -p $HADOOP_IPC_PORT:$HADOOP_IPC_PORT \
    -p $HADOOP_PORT:$HADOOP_PORT \
    -p $HADOOP_JMX_PORT:$HADOOP_JMX_PORT \
    -p $EXPORTER_PORT:$EXPORTER_PORT \
    "$IMAGE_NAME" /bin/bash -c "./bin/hdfs datanode"

  echo "================================================="
  echo "configs: ${CORE_SITE_XML} ${HDFS_SITE_XML}"
  echo "http address: ${ADDRESS}:$HADOOP_HTTP_PORT"
  echo "jmx address: ${ADDRESS}:$HADOOP_JMX_PORT"
  echo "exporter address: ${ADDRESS}:$EXPORTER_PORT"
  echo "================================================="
}

# ===================================[main]===================================

checkDocker
buildImageIfNeed "$IMAGE_NAME"
if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork

initArg "$@"

if [[ "$node" == "namenode" ]]; then
  startNamenode
elif [[ "$node" == "datanode" ]]; then
  startDatanode
else
  showHelp
  exit 0
fi