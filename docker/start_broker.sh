#!/bin/bash

# =============================[functions]=============================
function getAddress(){
  if [[ "$(which ipconfig)" != "" ]]; then
    ipconfig getifaddr en0
  else
    hostname -i
  fi
}

function showHelp(){
  echo "Usage: start_broker.sh [ OPTIONS ]"
  echo "Required: "
  echo "    zookeeper.connect=node:22222  set zookeeper connection"
  echo "Optional: "
  echo "    num.io.threads=10             set JVM memory"
  echo "    num.network.threads=10        set JVM memory"
  echo "    memory=\"-Xmx2G -Xms2G\"        set JVM memory"
}

# =====================================================================

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

USER=broker
KAFKA_VERSION=2.8.0
image_name=astraea/broker
broker_id="$(($RANDOM % 1000))"
address=$(getAddress)
jvm_memory="-Xmx2G -Xms2G"
broker_port="$(($(($RANDOM % 10000 )) + 10000))"
broker_jmx_port="$(($(($RANDOM % 10000 )) + 10000))"
jmx_opts="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$broker_jmx_port \
  -Dcom.sun.management.jmxremote.rmi.port=$broker_jmx_port \
  -Djava.rmi.server.hostname=$address"

# initialize broker config
config_file="/tmp/server${broker_id}.properties"
echo "" > "$config_file"

while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 2
  fi
  if [[ "$1" == "memory=*" ]]; then
    jvm_memory="$(echo $1 | cut -d'=' -f 2)"
  fi
  echo "$1" >> "$config_file"
  shift
done

# check zk connection
if [[ "$(cat $config_file | grep zookeeper.connect)" == "" ]]; then
  showHelp
  exit 2
fi

# listeners will be generated automatically
if [[ "$(cat $config_file | grep listeners)" != "" ]]; then
  echo "you should not define listeners"
  exit 2
else
  echo "listeners=PLAINTEXT://:9092" >> "$config_file"
  echo "advertised.listeners=PLAINTEXT://${address}:$broker_port" >> "$config_file"
fi

# log.dirs is not exposed so it should be generated automatically
if [[ "$(cat $config_file | grep log.dirs)" != "" ]]; then
  echo "you should not define log.dirs"
  exit 2
else
  echo "log.dirs=/tmp/kafka-logs" >> "$config_file"
fi

# auto-generate broker id if it does not exist
if [[ "$(cat $config_file | grep broker.id)" != "" ]]; then
  echo "you should not define broker.id"
  exit 2
else
  echo "broker.id=${broker_id}" >> "$config_file"
fi

# =============================[performance configs]=============================
if [[ "$(cat $config_file | grep num.io.threads)" == "" ]]; then
  echo "num.io.threads=8" >> "$config_file"
fi

if [[ "$(cat $config_file | grep num.network.threads)" == "" ]]; then
  echo "num.network.threads=8" >> "$config_file"
fi

if [[ "$(cat $config_file | grep num.partitions)" == "" ]]; then
  echo "num.partitions=8" >> "$config_file"
fi

if [[ "$(cat $config_file | grep transaction.state.log.replication.factor)" == "" ]]; then
  echo "transaction.state.log.replication.factor=1" >> "$config_file"
fi

if [[ "$(cat $config_file | grep offsets.topic.replication.factor)" == "" ]]; then
  echo "offsets.topic.replication.factor=1" >> "$config_file"
fi

if [[ "$(cat $config_file | grep transaction.state.log.min.isr)" == "" ]]; then
  echo "transaction.state.log.min.isr=1" >> "$config_file"
fi
# ==============================================================================

docker build -t $image_name - <<Dockerfile
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget

# add user
RUN groupadd $USER && useradd -ms /bin/bash -g $USER $USER

# change user
USER $USER

# download kafka
WORKDIR /home/$USER
RUN wget https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz
RUN tar -zxvf kafka_2.13-${KAFKA_VERSION}.tgz
WORKDIR /home/$USER/kafka_2.13-${KAFKA_VERSION}
Dockerfile

docker run -d \
  -e KAFKA_HEAP_OPTS="$jvm_memory" \
  -e KAFKA_JMX_OPTS="$jmx_opts" \
  -v $config_file:/tmp/broker.properties:ro \
  -p $broker_port:9092 \
  -p $broker_jmx_port:$broker_jmx_port \
  $image_name /home/$USER/kafka_2.13-${KAFKA_VERSION}/bin/kafka-server-start.sh /tmp/broker.properties

echo "================================================="
echo "broker address ${address}:$broker_port"
echo "jmx address: ${address}:$broker_jmx_port"
echo "================================================="