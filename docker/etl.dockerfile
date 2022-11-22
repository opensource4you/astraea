# this dockerfile is generated dynamically
FROM ghcr.io/skiptests/astraea/deps AS astraeabuild

# clone repo
WORKDIR /tmp
RUN git clone https://github.com/wycccccc/astraea

# pre-build project to collect all dependencies
WORKDIR /tmp/astraea
RUN git checkout startFromscript
RUN ./gradlew clean shadowJar

FROM ubuntu:20.04 AS build

# add user
RUN groupadd astraea && useradd -ms /bin/bash -g astraea astraea

# export ENV
ENV ASTRAEA_HOME /opt/astraea

# install tools
RUN apt-get update && apt-get install -y wget unzip

# download spark
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
RUN mkdir /opt/spark
RUN tar -zxvf spark-3.1.2-bin-hadoop3.2.tgz -C /opt/spark --strip-components=1

# the python3 in ubuntu 22.04 is 3.10 by default, and it has a known issue (https://github.com/vmprof/vmprof-python/issues/240)
# The issue obstructs us from installing 3-third python libraries, so we downgrade the ubuntu to 20.04

FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jre python3 python3-pip

# copy spark
COPY --from=build /opt/spark /opt/spark

# copy astraea
COPY --from=astraeabuild /tmp/astraea /opt/astraea

# add user
RUN groupadd astraea && useradd -ms /bin/bash -g astraea astraea

# change user
RUN chown -R astraea:astraea /opt/spark
USER astraea

# export ENV
ENV SPARK_HOME /opt/spark
WORKDIR /opt/spark

