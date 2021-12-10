#!/bin/bash

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
  echo "    RUN=false                                set false if you want to build image only"
  echo "    DATA_FOLDERS=/tmp/folder1,/tmp/folder2   set host folders used by broker"
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

# =================================[main]=================================

kafka_user=astraea
exporter_version="0.16.1"
exporter_port="$(($(($RANDOM % 10000)) + 10000))"
version=${REVISION:-${VERSION:-2.8.1}}
repo=${REPO:-astraea/broker}
image_name="$repo:$version"
broker_id="$(($RANDOM % 1000))"
broker_port="$(($(($RANDOM % 10000)) + 10000))"
broker_jmx_port="$(($(($RANDOM % 10000)) + 10000))"
run_container=${RUN:-true}
admin_name="admin"
admin_password="admin-secret"
user_name="user"
user_password="user-secret"
jmx_opts="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=$broker_jmx_port \
  -Dcom.sun.management.jmxremote.rmi.port=$broker_jmx_port \
  -Djava.rmi.server.hostname=$address"
heap_opts="${HEAP_OPTS:-"-Xmx2G -Xms2G"}"

config_file="/tmp/server-${broker_port}.properties"
echo "" >"$config_file"

while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  echo "$1" >> "$config_file"
  shift
done

if [[ -n "$REVISION" ]]; then

  docker build -t $image_name - <<Dockerfile
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk wget git curl

# add user
RUN groupadd $kafka_user && useradd -ms /bin/bash -g $kafka_user $kafka_user

# change user
USER $kafka_user

# download jmx exporter
RUN mkdir /tmp/jmx_exporter
WORKDIR /tmp/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml
RUN wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${exporter_version}/jmx_prometheus_javaagent-${exporter_version}.jar

# build kafka from source code
RUN git clone https://github.com/apache/kafka /tmp/kafka
WORKDIR /tmp/kafka
RUN git checkout $version
RUN ./gradlew clean releaseTarGz
RUN mkdir /home/$kafka_user/kafka
RUN tar -zxvf \$(find ./core/build/distributions/ -maxdepth 1 -type f -name kafka_*SNAPSHOT.tgz) -C /home/$kafka_user/kafka --strip-components=1
WORKDIR "/home/$kafka_user/kafka"

Dockerfile

else
  docker build -t $image_name - <<Dockerfile
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget

# add user
RUN groupadd $kafka_user && useradd -ms /bin/bash -g $kafka_user $kafka_user

# change user
USER $kafka_user

# download jmx exporter
RUN mkdir /tmp/jmx_exporter
WORKDIR /tmp/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml
RUN wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${exporter_version}/jmx_prometheus_javaagent-${exporter_version}.jar

# download kafka
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/kafka/${version}/kafka_2.13-${version}.tgz
RUN mkdir /home/$kafka_user/kafka
RUN tar -zxvf kafka_2.13-${version}.tgz -C /home/$kafka_user/kafka --strip-components=1
WORKDIR "/home/$kafka_user/kafka"

Dockerfile
fi

if [[ "$run_container" != "true" ]]; then
  echo "docker image: $image_name is created"
  exit 0
fi

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
  if [[ "$SASL" == "true" ]]; then
    echo "listeners=SASL_PLAINTEXT://:9092" >> "$config_file"
    echo "advertised.listeners=SASL_PLAINTEXT://${address}:$broker_port" >> "$config_file"
    echo "security.inter.broker.protocol=SASL_PLAINTEXT" >> "$config_file"
    echo "sasl.mechanism.inter.broker.protocol=PLAIN" >> "$config_file"
    echo "sasl.enabled.mechanisms=PLAIN" >> "$config_file"
    echo "listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
             username="$admin_name" \
             password="$admin_password" \
             user_${admin_name}="$admin_password" \
             user_${user_name}="${user_password}";" >> "$config_file"
    echo "authorizer.class.name=kafka.security.authorizer.AclAuthorizer" >> "$config_file"
    # allow brokers to communicate each other
    echo "super.users=User:admin" >> "$config_file"
  else
    echo "listeners=PLAINTEXT://:9092" >>"$config_file"
    echo "advertised.listeners=PLAINTEXT://${address}:$broker_port" >>"$config_file"
  fi
fi

# create log folders and find out the existent broker.id
hostFolderConfigs=""
if [[ "$(cat $config_file | grep log.dirs)" != "" ]]; then
  echo "you should not define log.dirs"
  exit 2
else
  logConfigs="log.dirs"
  index="1"
  if [[ -n "$DATA_FOLDERS" ]]; then
    IFS=',' read -ra folders <<< "$DATA_FOLDERS"
    for folder in "${folders[@]}"; do
      # create the folder if it is nonexistent
      mkdir -p "$folder"
      # update the broker.id used by this script if it is exist
      if [[ -f "$folder/meta.properties" ]]; then
        broker_id=$(grep broker.id "$folder/meta.properties" | cut -d = -f 2)
      fi
      if [[ "$index" == "1" ]]; then
        logConfigs="$logConfigs=/tmp/kafka-logs$index"
        hostFolderConfigs="-v $folder:/tmp/kafka-logs$index"
      else
        logConfigs="$logConfigs,/tmp/kafka-logs$index"
        hostFolderConfigs="$hostFolderConfigs -v $folder:/tmp/kafka-logs$index"
      fi
      index=$((index+1))
    done
  else
    # In order to enable replica folders migration, we create three folders for broker.
    logConfigs="log.dirs=/tmp/kafka-logs1,/tmp/kafka-logs2,/tmp/kafka-logs3"
  fi
  echo $logConfigs >> "$config_file"
fi

# auto-generate broker id if it does not exist
if [[ "$(cat $config_file | grep broker.id)" != "" ]]; then
  echo "you should not define broker.id"
  exit 2
else
  echo "broker.id=${broker_id}" >> "$config_file"
fi

# performance configs
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

if [[ "$address" == "127.0.0.1" || "$address" == "127.0.1.1" ]]; then
  echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
  exit 2
fi

docker run -d \
  -e KAFKA_HEAP_OPTS="$heap_opts" \
  -e KAFKA_JMX_OPTS="$jmx_opts" \
  -e KAFKA_OPTS="-javaagent:/tmp/jmx_exporter/jmx_prometheus_javaagent-${exporter_version}.jar=$exporter_port:/tmp/jmx_exporter/kafka-2_0_0.yml" \
  -v $config_file:/tmp/broker.properties:ro \
  $hostFolderConfigs \
  -p $broker_port:9092 \
  -p $broker_jmx_port:$broker_jmx_port \
  -p $exporter_port:$exporter_port \
  $image_name ./bin/kafka-server-start.sh /tmp/broker.properties

echo "================================================="
echo "broker address: ${address}:$broker_port"
echo "jmx address: ${address}:$broker_jmx_port"
echo "exporter address: ${address}:$exporter_port"
echo "broker id: $broker_id"
echo "folder mapping: $hostFolderConfigs"
if [[ "$SASL" == "true" ]]; then
  user_jaas_file=/tmp/user-jaas-${broker_port}.conf
  echo "
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${user_name} password=${user_password};
  security.protocol=SASL_PLAINTEXT
  sasl.mechanism=PLAIN
  " > $user_jaas_file

  admin_jaas_file=/tmp/admin-jaas-${broker_port}.conf
  echo "
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${admin_name} password=${admin_password};
  security.protocol=SASL_PLAINTEXT
  sasl.mechanism=PLAIN
  " > $admin_jaas_file
  echo "SASL_PLAINTEXT is enabled. user config: $user_jaas_file admin config: $admin_jaas_file"
fi
echo "================================================="
