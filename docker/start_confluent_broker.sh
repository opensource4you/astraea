#!/bin/bash

# ===============================[global variables]===============================

declare -r USER=astraea
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r DATA_FOLDER_IN_CONTAINER_PREFIX="/tmp/log-folder"
declare -r EXPORTER_PORT=${EXPORTER_PORT:-$(($(($RANDOM % 10000)) + 10000))}
declare -r BROKER_PORT=${BROKER_PORT:-$(($(($RANDOM % 10000)) + 10000))}
declare -r CONTAINER_NAME="broker-$BROKER_PORT"
declare -r BROKER_JMX_PORT="${BROKER_JMX_PORT:-$(($(($RANDOM % 10000)) + 10000))}"
declare -r ADMIN_NAME="admin"
declare -r ADMIN_PASSWORD="admin-secret"
declare -r USER_NAME="user"
declare -r USER_PASSWORD="user-secret"
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)
declare -r JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$BROKER_JMX_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=$BROKER_JMX_PORT \
  -Djava.rmi.server.hostname=$ADDRESS"
declare -r HEAP_OPTS="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"
declare -r BROKER_PROPERTIES="/tmp/server-${BROKER_PORT}.properties"
declare -r ZOOKEEPER_CONNECT=${1:18}
# cleanup the file if it is existent
[[ -f "$BROKER_PROPERTIES" ]] && rm -f "$BROKER_PROPERTIES"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_broker.sh [ ARGUMENTS ]"
  echo "Required Argument: "
  echo "    zookeeper.connect=node:22222             set zookeeper connection"
  echo "Optional Arguments: "
  echo "    num.io.threads=10                        set broker I/O threads"
  echo "    num.network.threads=10                   set broker network threads"
  echo "ENV: "
  echo "    REPO=astraea/broker                      set the docker repo"
  echo "    HEAP_OPTS=\"-Xmx2G -Xms2G\"                set broker JVM memory"
  echo "    REVISION=trunk                           set revision of kafka source code to build container"
  echo "    VERSION=2.8.1                            set version of kafka distribution"
  echo "    BUILD=false                              set true if you want to build image locally"
  echo "    RUN=false                                set false if you want to build/pull image only"
  echo "    DATA_FOLDERS=/tmp/folder1,/tmp/folder2   set host folders used by broker"
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

function rejectProperty() {
  local key=$1
  if [[ -f "$BROKER_PROPERTIES" ]] && [[ "$(cat $BROKER_PROPERTIES | grep $key)" != "" ]]; then
    echo "$key is NOT configurable"
    exit 2
  fi
}

function requireProperty() {
  local key=$1
  if [[ ! -f "$BROKER_PROPERTIES" ]] || [[ "$(cat $BROKER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key is required"
    exit 2
  fi
}

function setListener() {
  if [[ "$SASL" == "true" ]]; then
    echo "listeners=SASL_PLAINTEXT://:9092" >>"$BROKER_PROPERTIES"
    echo "advertised.listeners=SASL_PLAINTEXT://${ADDRESS}:$BROKER_PORT" >>"$BROKER_PROPERTIES"
    echo "security.inter.broker.protocol=SASL_PLAINTEXT" >>"$BROKER_PROPERTIES"
    echo "sasl.mechanism.inter.broker.protocol=PLAIN" >>"$BROKER_PROPERTIES"
    echo "sasl.enabled.mechanisms=PLAIN" >>"$BROKER_PROPERTIES"
    echo "listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username="$ADMIN_NAME" \
                 password="$ADMIN_PASSWORD" \
                 user_${ADMIN_NAME}="$ADMIN_PASSWORD" \
                 user_${USER_NAME}="${USER_PASSWORD}";" >>"$BROKER_PROPERTIES"
    echo "authorizer.class.name=kafka.security.authorizer.AclAuthorizer" >>"$BROKER_PROPERTIES"
    # allow brokers to communicate each other
    echo "super.users=User:admin" >>"$BROKER_PROPERTIES"
  else
    echo "listeners=PLAINTEXT://:9092" >>"$BROKER_PROPERTIES"
    echo "advertised.listeners=PLAINTEXT://${ADDRESS}:$BROKER_PORT" >>"$BROKER_PROPERTIES"
    echo "confluent.metadata.server.listeners=http://0.0.0.0:$BROKER_PORT" >>"$BROKER_PROPERTIES"
  fi
}

function setLogDirs() {
  declare -i count=0
  if [[ -n "$DATA_FOLDERS" ]]; then
    IFS=',' read -ra folders <<<"$DATA_FOLDERS"
    for folder in "${folders[@]}"; do
      # create the folder if it is nonexistent
      mkdir -p "$folder"
      count=$((count + 1))
    done
  else
    count=1
  fi

  local logConfigs="log.dirs=$DATA_FOLDER_IN_CONTAINER_PREFIX-0"
  for ((i = 1; i < count; i++)); do
    logConfigs="$logConfigs,$DATA_FOLDER_IN_CONTAINER_PREFIX-$i"
  done

  echo $logConfigs >>"$BROKER_PROPERTIES"
}

function generateMountCommand() {
  local mount=""
  if [[ -n "$DATA_FOLDERS" ]]; then
    IFS=',' read -ra folders <<<"$DATA_FOLDERS"
    # start with 0 rather than 1 (see setLogDirs)
    declare -i count=0

    for folder in "${folders[@]}"; do
      mount="$mount -v $folder:$DATA_FOLDER_IN_CONTAINER_PREFIX-$count"
      count=$((count + 1))
    done
  fi
  echo "$mount"
}

function setPropertyIfEmpty() {
  local key=$1
  local value=$2
  # performance configs
  if [[ "$(cat $BROKER_PROPERTIES | grep $key)" == "" ]]; then
    echo "$key=$value" >>"$BROKER_PROPERTIES"
  fi
}

function fetchBrokerId() {
  local id=""
  for i in {1..10}; do
    id=$(docker logs $CONTAINER_NAME | grep -o "KafkaServer id=[0-9]*" | cut -d = -f 2)
    if [[ "$id" != "" ]]; then
      break
    fi
    sleep 1
  done
  if [[ "$id" == "" ]]; then
    echo "failed to get broker id from container: $CONTAINER_NAME"
  else
    echo "$id"
  fi

}

# ===================================[main]===================================

checkDocker
checkNetwork

while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  echo "$1" >>"$BROKER_PROPERTIES"
  shift
done


rejectProperty "listeners"
rejectProperty "log.dirs"
rejectProperty "broker.id"
rejectProperty "metric.reporters"
requireProperty "zookeeper.connect"
setListener
setPropertyIfEmpty "num.io.threads" "8"
setPropertyIfEmpty "num.network.threads" "8"
setPropertyIfEmpty "num.partitions" "8"
setPropertyIfEmpty "transaction.state.log.replication.factor" "1"
setPropertyIfEmpty "offsets.topic.replication.factor" "1"
setPropertyIfEmpty "transaction.state.log.min.isr" "1"
setPropertyIfEmpty "confluent.metrics.reporter.zookeeper.connect" "$ZOOKEEPER_CONNECT"
setPropertyIfEmpty "metric.reporters" "io.confluent.metrics.reporter.ConfluentMetricsReporter"
setPropertyIfEmpty "confluent.metrics.reporter.bootstrap.servers" "${ADDRESS}:$BROKER_PORT"
setPropertyIfEmpty "confluent.metrics.reporter.topic.replicas" "1"
setPropertyIfEmpty "confluent.topic.replication.factor" "1"
setPropertyIfEmpty "confluent.license.topic.replication.factor" "1"
setPropertyIfEmpty "confluent.metadata.topic.replication.factor" "1"
setPropertyIfEmpty "confluent.security.event.logger.exporter.kafka.topic.replicas" "1"
setPropertyIfEmpty "confluent.balancer.topic.replication.factor" "1"
setPropertyIfEmpty "confluent.balancer.enable" "true"
setPropertyIfEmpty "confluent.topic.bootstrap.servers" "${ADDRESS}:$BROKER_PORT" 
setLogDirs

docker run -d --init \
    --name $CONTAINER_NAME \
    -e KAFKA_HEAP_OPTS="$HEAP_OPTS" \
    -e KAFKA_JMX_OPTS="$JMX_OPTS" \
    -p $BROKER_JMX_PORT:$BROKER_JMX_PORT \
    -p $EXPORTER_PORT:$EXPORTER_PORT \
    -p $BROKER_PORT:9092 \
    -v $BROKER_PROPERTIES:/tmp/broker.properties:ro \
    $(generateMountCommand) \
    confluentinc/cp-server:latest /bin/kafka-server-start /tmp/broker.properties
echo "================================================="
[[ -n "$DATA_FOLDERS" ]] && echo "mount $DATA_FOLDERS to container: $CONTAINER_NAME"
echo "broker id: $(fetchBrokerId)"
echo "broker address: ${ADDRESS}:$BROKER_PORT"
echo "jmx address: ${ADDRESS}:$BROKER_JMX_PORT"
echo "exporter address: ${ADDRESS}:$EXPORTER_PORT"
if [[ "$SASL" == "true" ]]; then
  user_jaas_file=/tmp/user-jaas-${BROKER_PORT}.conf
  echo "
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${USER_NAME} password=${USER_PASSWORD};
  security.protocol=SASL_PLAINTEXT
  sasl.mechanism=PLAIN
  " >$user_jaas_file

  admin_jaas_file=/tmp/admin-jaas-${BROKER_PORT}.conf
  echo "
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${ADMIN_NAME} password=${ADMIN_PASSWORD};
  security.protocol=SASL_PLAINTEXT
  sasl.mechanism=PLAIN
  " >$admin_jaas_file
  echo "SASL_PLAINTEXT is enabled. user config: $user_jaas_file admin config: $admin_jaas_file"
fi
echo "================================================="

