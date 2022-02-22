# this dockerfile is generated dynamically
FROM confluentinc/cp-server:latest
USER root
RUN groupadd astraea && useradd -ms /bin/bash -g astraea astraea
RUN yum -y update && yum -y install git unzip

# download jmx exporter
RUN mkdir /opt/jmx_exporter
WORKDIR /opt/jmx_exporter
RUN wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/kafka-2_0_0.yml
RUN wget https://REPO1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar

# change user
RUN chown -R astraea:astraea /tmp
RUN chown -R astraea:astraea /var
USER astraea

WORKDIR /

