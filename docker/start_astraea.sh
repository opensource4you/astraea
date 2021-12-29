#!/bin/bash

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

docker_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
dockerfile=$docker_dir/astraea.dockerfile
image_name="astraea/astraea:latest"

echo "# this dockerfile is generated dynamically
FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get upgrade -y && apt-get install -y openjdk-11-jdk wget git

# clone repo
WORKDIR /tmp
RUN git clone https://github.com/skiptests/astraea

# pre-build project to collect all dependencies
WORKDIR /tmp/astraea
RUN ./gradlew clean build -x test --no-daemon
" > "$dockerfile"

# build image only if the image does not exist locally
if [[ "$(docker images -q $image_name 2> /dev/null)" == "" ]]; then
  docker build -t $image_name -f "$dockerfile" "$docker_dir"
fi

if [[ -z "$1" ]]; then
  result="help"
else
  # shellcheck disable=SC2124
  result="$@"
fi


docker run --rm \
  $image_name \
  /bin/bash -c "./gradlew run --args=\"$result\""