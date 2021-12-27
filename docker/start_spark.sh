#!/bin/bash

function showHelp() {
  echo "Usage: [ENV] start_spark.sh master-url"
  echo "Optional Arguments: "
  echo "    master-url=spar://node00:1111    start a spark worker. Or start a spark master if master-url is not defined"
  echo "ENV: "
  echo "    REPO=astraea/spark               set the docker repo"
  echo "    VERSION=3.1.2                    set version of spark distribution"
  echo "    DELTA_VERSION=1.0.0              set version of delta distribution"
  echo "    PY_KAFKA_VERSION=1.7.0           set version of confluent kafka distribution"
  echo "    RUN=false                        set false if you want to build image only"
  echo "    PYTHON_DEPS=delta-spark=1.0.0    those dependencies will be pre-installed in the docker image"
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

master_url=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 2
  fi
  master_url=$1
  shift
done

# Spark needs to manage the hardware resource for this node, so we don't run multiples workers/masters in same node.
if [[ -n "$master_url" ]]; then
  master_port=$(echo "$master_url" | cut -d':' -f 3)
  worker_name="spark-worker-$master_port"
  container_names=$(docker ps --format "{{.Names}}")
  if [[ $(echo "${container_names}" | grep "$worker_name") != "" ]]; then
    echo "It is disallowed to run multiples spark workers in same node"
    exit 2
  fi
else
  master_name="spark-master"
  container_names=$(docker ps --format "{{.Names}}")
  if [[ $(echo "${container_names}" | grep "$master_name") != "" ]]; then
    echo "It is disallowed to run multiples spark masters in same node"
    exit 2
  fi
fi


spark_user=astraea
version=${REVISION:-${VERSION:-3.1.2}}
repo=${REPO:-astraea/spark}
image_name="$repo:$version"
run_container=${RUN:-true}
spark_port="$(($(($RANDOM % 10000)) + 10000))"
spark_ui_port="$(($(($RANDOM % 10000)) + 10000))"
ivy_version=2.5.0
delta_version=${DELTA_VERSION:-1.0.0}
py_kafka_version=${PY_KAFKA_VERSION:-1.7.0}
# hardcode version to avoid NPE (see https://issues.apache.org/jira/browse/HADOOP-16410)
hadoop_version=3.2.2
docker_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
dockerfile=$docker_dir/spark.dockerfile

echo "# this dockerfile is generated dynamically
FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget python3 python3-pip unzip

# add user
RUN groupadd $spark_user && useradd -ms /bin/bash -g $spark_user $spark_user

# change user
USER $spark_user

# pre-install python dependencies
RUN pip3 install confluent-kafka==$py_kafka_version delta-spark==$delta_version pyspark==$version

# install java dependencies
WORKDIR /tmp
RUN wget https://dlcdn.apache.org//ant/ivy/${ivy_version}/apache-ivy-${ivy_version}-bin.zip
RUN unzip apache-ivy-${ivy_version}-bin.zip
WORKDIR apache-ivy-${ivy_version}
RUN java -jar ./ivy-${ivy_version}.jar -dependency io.delta delta-core_2.12 $delta_version
RUN java -jar ./ivy-${ivy_version}.jar -dependency org.apache.spark spark-sql-kafka-0-10_2.12 $version
RUN java -jar ./ivy-${ivy_version}.jar -dependency org.apache.spark spark-token-provider-kafka-0-10_2.12 $version
RUN java -jar ./ivy-${ivy_version}.jar -dependency org.apache.hadoop hadoop-azure $hadoop_version

# download spark
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-${version}/spark-${version}-bin-hadoop3.2.tgz
RUN mkdir /home/$spark_user/spark
RUN tar -zxvf spark-${version}-bin-hadoop3.2.tgz -C /home/$spark_user/spark --strip-components=1
WORKDIR /home/$spark_user/spark
" > "$dockerfile"

# build image only if the image does not exist locally
if [[ "$(docker images -q $image_name 2> /dev/null)" == "" ]]; then
  docker build -t $image_name -f "$dockerfile" "$docker_dir"
fi

if [[ "$run_container" != "true" ]]; then
  echo "docker image: $image_name is created"
  exit 0
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
  echo "This script requires to run container with \"--network host\", but the feature is unsupported by Mac OS"
  exit 2
fi

if [[ "$address" == "127.0.0.1" || "$address" == "127.0.1.1" ]]; then
  echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
  exit 2
fi


if [[ -n "$master_url" ]]; then
  docker run -d \
    -e SPARK_WORKER_WEBUI_PORT=$spark_ui_port \
    -e SPARK_WORKER_PORT=$spark_port \
    -e SPARK_NO_DAEMONIZE=true \
    --name "$worker_name" \
    --network host \
    $image_name ./sbin/start-worker.sh "$master_url"

  echo "================================================="
  echo "Starting Spark worker $address:$spark_port"
  echo "Bound WorkerWebUI started at http://${address}:${spark_ui_port}"
  echo "================================================="
else
  docker run -d \
    -e SPARK_MASTER_WEBUI_PORT=$spark_ui_port \
    -e SPARK_MASTER_PORT=$spark_port \
    -e SPARK_NO_DAEMONIZE=true \
    --name "$master_name" \
    --network host \
    $image_name ./sbin/start-master.sh

  echo "================================================="
  echo "Starting Spark master at spark://$address:$spark_port"
  echo "Bound MasterWebUI started at http://${address}:${spark_ui_port}"
  echo "execute ./docker/start_spark.sh spark://$address:$spark_port to add worker"
  echo "================================================="
fi
