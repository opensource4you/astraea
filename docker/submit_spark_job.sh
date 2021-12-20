#!/bin/bash

# =============================[functions]=============================

function showHelp() {
  echo "Usage: [ENV] submit_spark_job.sh [ ARGUMENTS ]"
  echo "ENV: "
  echo "    REPO=astraea/spark-job             set the docker repo"
  echo "    VERSION=3.1.2                      set version of spark distribution"
  echo "    DELTA_VERSION=1.0.0                set version of delta distribution"
  echo "    PY_KAFKA_VERSION=1.7.0             set version of confluent kafka distribution"
  echo "    RUN=false                          set false if you want to build image only"
  echo "    MOUNT=host_path:container_path     mount the host path to container path"
}

# ===============================[checks]===============================

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
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
  fi
  if [[ "$1" != "--"* ]] && [[ "$1" == *".py" ]]; then
    py_file=$1
  fi
  args="$args $1"
  shift
done

spark_user=astraea
version=${REVISION:-${VERSION:-3.1.2}}
repo=${REPO:-astraea/spark-job}
image_name="$repo:$version"
run_container=${RUN:-true}
ivy_version=2.5.0
delta_version=${DELTA_VERSION:-1.0.0}
py_kafka_version=${PY_KAFKA_VERSION:-1.7.0}
# hardcode version to avoid NPE (see https://issues.apache.org/jira/browse/HADOOP-16410)
hadoop_version=3.2.2
docker_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
dockerfile=$docker_dir/spark-job.dockerfile

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

if [[ -z "$py_file" ]]; then
  echo "failed to get main py file from input arguments"
  exit 2
fi

if [[ ! -f "$py_file" ]]; then
  echo "\"$py_file\" is not a file"
  exit 2
fi

# spark stores package files to ~/.ivy2 by default. We keep those files in host path to avoid download them again.
mkdir -p "$HOME"/.ivy2

py_folder=$(dirname "$py_file")

if [[ -n "$MOUNT" ]]; then
  index="1"
  IFS=',' read -ra folders <<< "$MOUNT"
  for mapping in "${folders[@]}"; do
    host_folder=$(echo "$mapping" | cut -d : -f 1)
    container_folder=$(echo "$mapping" | cut -d : -f 2)

    # create folder on host
    mkdir -p "$host_folder"

    if [[ "$index" == "1" ]]; then
      hostFolderConfigs="-v $host_folder:$container_folder"
    else
      hostFolderConfigs="$hostFolderConfigs -v $host_folder:$container_folder"
    fi
    index=$((index+1))
  done

  docker run \
    --network host \
    $hostFolderConfigs \
    -v "$py_folder":"$py_folder":ro \
    $image_name ./bin/spark-submit $args
else
  docker run \
    --network host \
    -v "$py_folder":"$py_folder":ro \
    $image_name ./bin/spark-submit $args
fi
