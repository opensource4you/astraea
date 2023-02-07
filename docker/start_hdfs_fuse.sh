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
declare -r REPO=${REPO:-ghcr.io/skiptests/astraea/hdfs_fuse}
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/hdfs-fuse.dockerfile
declare -r CONTAINER_NAME="hdfs-fuse"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_hdfs_fuse.sh"
  echo "ENV: "
  echo "    REPO=astraea/datanode     set the docker repo"
  echo "    VERSION=3.3.4              set version of hadoop distribution"
  echo "    BUILD=false                set true if you want to build image locally"
  echo "    RUN=false                  set false if you want to build/pull image only"
}

function generateDockerfile() {
  echo "#this dockerfile is generated dynamically
FROM ubuntu:22.04 AS build

#install tools
RUN apt-get update && apt-get install -y wget

#download hadoop
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${VERSION}/hadoop-${VERSION}-src.tar.gz
RUN mkdir /opt/hadoop-src
RUN tar -zxvf hadoop-${VERSION}-src.tar.gz -C /opt/hadoop-src --strip-components=1
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${VERSION}/hadoop-${VERSION}.tar.gz
RUN mkdir /opt/hadoop
RUN tar -zxvf hadoop-${VERSION}.tar.gz -C /opt/hadoop --strip-components=1

FROM ubuntu:22.04 AS buildsrc

#install tools
RUN apt-get update \\
    && apt-get install -y openjdk-11-jdk \\
        maven \\
        build-essential \\
        autoconf \\
        automake \\
        libtool \\
        cmake \\
        zlib1g-dev \\
        pkg-config \\
        libssl-dev \\
        libsasl2-dev \\
        g++ \\
        curl \\
        libfuse-dev

WORKDIR /tmp
RUN curl -L -s -S https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/protobuf-java-3.7.1.tar.gz -o protobuf-3.7.1.tar.gz \\
    && mkdir /opt/protobuf-3.7-src \\
    && tar -zxf protobuf-3.7.1.tar.gz --strip-components 1 -C /opt/protobuf-3.7-src && cd /opt/protobuf-3.7-src \\
    && ./configure --prefix=/usr/ \\
    && make -j\$(nproc) \\
    && make install

WORKDIR /tmp
RUN curl -L https://sourceforge.net/projects/boost/files/boost/1.80.0/boost_1_80_0.tar.bz2/download > boost_1_80_0.tar.bz2 \\
    && tar --bzip2 -xf boost_1_80_0.tar.bz2 -C /opt && cd /opt/boost_1_80_0 \\
    && ./bootstrap.sh --prefix=/usr/ \\
    && ./b2 --without-python \\
    && ./b2 --without-python install

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

#copy hadoop
COPY --from=build /opt/hadoop-src /opt/hadoop
WORKDIR /opt/hadoop
RUN mvn clean package -pl hadoop-hdfs-project/hadoop-hdfs-native-client -Pnative -DskipTests -Drequire.fuse=true

FROM ubuntu:22.04

#install tools
RUN apt-get update && apt-get install -y openjdk-11-jre fuse

#copy hadoop
COPY --from=build /opt/hadoop /opt/hadoop
COPY --from=buildsrc /opt/hadoop /opt/hadoop

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME /opt/hadoop

RUN echo \"user_allow_other\" >> /etc/fuse.conf

WORKDIR /opt/hadoop/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/fuse-dfs
RUN sed -i -e '18aexport CLASSPATH=\\\${HADOOP_HOME}/etc/hadoop:\`find \\\${HADOOP_HOME}/share/hadoop/ | awk '\"'\"'{path=path\":\"\\\$0}END{print path}'\"'\"'\`' \\
    -i -e '18aexport LD_LIBRARY_PATH=\\\${HADOOP_HOME}/lib/native:\\\$LD_LIBRARY_PATH' \\
    -i -e 's#export LIBHDFS_PATH=.*#export LIBHDFS_PATH=\\\${HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib#' \\
    -i -e 's/find \"\\\$HADOOP_HOME\/hadoop-client\" -name \"\\*.jar\"/find \"\\\$HADOOP_HOME\/hadoop-client-modules\/hadoop-client\" -name \"\\*.jar\"/g' fuse_dfs_wrapper.sh

#add user
RUN groupadd astraea && useradd -ms /bin/bash -g astraea astraea

RUN mkdir /mnt/hdfs

#change user
RUN chown -R $USER:$USER /opt/hadoop /mnt/hdfs
USER $USER

" >"$DOCKERFILE"
}

# ===================================[main]===================================

checkDocker
buildImageIfNeed "$IMAGE_NAME"
if [[ "$RUN" != "true" ]]; then
  echo "docker image: $IMAGE_NAME is created"
  exit 0
fi

checkNetwork

if [[ $# -gt 0 ]]; then
  HDFS=$1
  HDFS_PORT="${HDFS: -5}"
fi

docker run -d --init \
  --name $CONTAINER_NAME-$HDFS_PORT \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --security-opt apparmor:unconfined \
  "$IMAGE_NAME" /bin/bash -c "./fuse_dfs_wrapper.sh -d $HDFS /mnt/hdfs"

docker exec -it -w /mnt/hdfs $CONTAINER_NAME-$HDFS_PORT /bin/bash