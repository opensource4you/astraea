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


get_ipv4_address() {
  if command -v ip &>/dev/null; then
    ip -o -4 address show | awk '!/127.0.0.1/ {gsub(/\/.*/, "", $4); print $4; exit}'
  elif command -v ifconfig &>/dev/null; then
    ifconfig | awk '/inet / && $2 != "127.0.0.1" { print $2; exit }'
  elif command -v networksetup &>/dev/null; then
    networksetup -getinfo Wi-Fi | awk '/IP address:/ { print $3; exit }'
  else
    echo "Error: No supported command found to fetch IP address." >&2
    return 1
  fi
}

# ===============================[global variables]===============================
declare -r ADDRESS=$(get_ipv4_address)
declare -r KAFKA_VERSION=${KAFKA_VERSION:-2.1.1}
declare -r DOCKERFILE=/tmp/astraea/kafka.dockerfile
declare -r JMX0_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=11111 \
  -Dcom.sun.management.jmxremote.rmi.port=11111 \
  -Djava.rmi.server.hostname=$ADDRESS"
declare -r JMX1_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=11112 \
  -Dcom.sun.management.jmxremote.rmi.port=11112 \
  -Djava.rmi.server.hostname=$ADDRESS"
declare -r JMX2_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=11113 \
  -Dcom.sun.management.jmxremote.rmi.port=11113 \
  -Djava.rmi.server.hostname=$ADDRESS"
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"
declare -r BROKER_PROPERTIES="/tmp/kafka-${BROKER_PORT}.properties"
declare -r IMAGE_NAME="ghcr.io/opensource4you/astraea/kafka:$(echo "$KAFKA_VERSION" | tr '[:upper:]' '[:lower:]')"

function generateDockerfileByVersion() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu:24.04 AS build

# install tools
RUN apt-get update && apt-get install -y wget

# download kafka
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.12-${KAFKA_VERSION}.tgz
RUN mkdir /opt/kafka
RUN tar -zxvf kafka_2.12-${KAFKA_VERSION}.tgz -C /opt/kafka --strip-components=1

FROM azul/zulu-openjdk:8-jre

# copy kafka
COPY --from=build /opt/kafka /opt/kafka

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
RUN chown -R $USER:$USER /opt/kafka
USER $USER

# export ENV
ENV KAFKA_HOME=/opt/kafka
WORKDIR /opt/kafka

# cp configs
RUN cp config/server.properties config/server0.properties
RUN cp config/server.properties config/server1.properties
RUN cp config/server.properties config/server2.properties

# update zk connect
RUN echo \"\nzookeeper.connect=$ADDRESS:2181\" >> config/server0.properties
RUN echo \"\nzookeeper.connect=$ADDRESS:2181\" >> config/server1.properties
RUN echo \"\nzookeeper.connect=$ADDRESS:2181\" >> config/server2.properties

# update advertised.listeners
RUN echo \"\nadvertised.listeners=PLAINTEXT://$ADDRESS:9092\" >> config/server0.properties
RUN echo \"\nadvertised.listeners=PLAINTEXT://$ADDRESS:9093\" >> config/server1.properties
RUN echo \"\nadvertised.listeners=PLAINTEXT://$ADDRESS:9094\" >> config/server2.properties

# update broker.id
RUN echo \"\nbroker.id=0\" >> config/server0.properties
RUN echo \"\nbroker.id=1\" >> config/server1.properties
RUN echo \"\nbroker.id=2\" >> config/server2.properties

" >"$DOCKERFILE"
}

mkdir -p /tmp/astraea
generateDockerfileByVersion
docker build --platform linux/amd64 -t "$IMAGE_NAME" -f "$DOCKERFILE" /tmp/astraea

# cleanup
docker rm -f zk bk-00 bk-01 bk-02

# start zk
docker run --rm \
  --name zk \
  -h zk \
  -p 2181:2181 \
  -d \
  "$IMAGE_NAME" bin/zookeeper-server-start.sh config/zookeeper.properties

# start bk-00
docker run --rm \
  --name bk-00 \
  -h bk-00 \
  -p 9092:9092 \
  -p 11111:11111 \
  -d \
  -e KAFKA_JMX_OPTS="$JMX0_OPTS" \
  "$IMAGE_NAME" bin/kafka-server-start.sh config/server0.properties

# start bk-01
docker run --rm \
  --name bk-01 \
  -h bk-01 \
  -p 9093:9092 \
  -p 11112:11112 \
  -d \
  -e KAFKA_JMX_OPTS="$JMX1_OPTS" \
  "$IMAGE_NAME" bin/kafka-server-start.sh config/server1.properties

# start bk-02
docker run --rm \
  --name bk-02 \
  -h bk-02 \
  -p 9094:9092 \
  -p 11113:11113 \
  -d \
  -e KAFKA_JMX_OPTS="$JMX2_OPTS" \
  "$IMAGE_NAME" bin/kafka-server-start.sh config/server2.properties
