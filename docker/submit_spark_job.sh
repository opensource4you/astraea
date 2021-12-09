#!/bin/bash

# =============================[functions]=============================

function showHelp() {
  echo "Usage: [ENV] submit_spark_job.sh [ ARGUMENTS ]"
  echo "ENV: "
  echo "    SPARK_VERSION=3.7.0    set version of spark distribution"
}

# ===============================[checks]===============================

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
  echo "This script requires to run container with \"--network host\", but the feature is unsupported by Mac OS"
  exit 2
fi

if [[ "$(which ipconfig)" != "" ]]; then
  address=$(ipconfig getifaddr en0)
else
  address=$(hostname -i)
fi

if [[ "$address" == "127.0.0.1" || "$address" == "127.0.1.1" ]]; then
  echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
  exit 2
fi

# =================================[main]=================================
while [[ $# -gt 0 ]]; do
  if [[ "$1" != "--"* ]] && [[ "$1" == *".py" ]]; then
    py_file=$1
  fi
  args="$args $1"
  shift
done

if [[ -z "$py_file" ]]; then
  echo "failed to get main py file from input arguments"
  exit 2
fi

if [[ ! -f "$py_file" ]]; then
  echo "\"$py_file\" is not a file"
  exit 2
fi

py_folder=$(dirname "$py_file")

if [[ -z "$SPARK_VERSION" ]]; then
  SPARK_VERSION=3.1.2
fi

spark_user=astraea
image_name=astraea/spark-job:$SPARK_VERSION

docker build -t $image_name - <<Dockerfile
FROM ubuntu:20.04

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget python3

# add user
RUN groupadd $spark_user && useradd -ms /bin/bash -g $spark_user $spark_user

# change user
USER $spark_user

# download spark
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz
RUN mkdir /home/$spark_user/spark
RUN tar -zxvf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz -C /home/$spark_user/spark --strip-components=1
WORKDIR /home/$spark_user/spark

Dockerfile

# spark stores package files to ~/.ivy2 by default. We keep those files in host path to avoid download them again.
mkdir -p "$HOME"/.ivy2

docker run \
  --network host \
  -v "$HOME"/.ivy2:/home/$spark_user/.ivy2 \
  -v "$py_folder":"$py_folder":ro \
  $image_name ./bin/spark-submit $args
