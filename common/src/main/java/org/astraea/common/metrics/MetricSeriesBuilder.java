/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.metrics;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;

/**
 * A utility for generating a series of metric objects, where the measured metric value might be
 * highly correlated to specific variables. For example broker id, calendar time, or unknown noise.
 * This class offers a way to construct large-scale metric sources of a fake cluster, which will be
 * useful for testing and experiment purposes.
 */
public interface MetricSeriesBuilder {

  static MetricSeriesBuilder of() {
    throw new UnsupportedOperationException();
  }

  MetricSeriesBuilder sourceCluster(ClusterInfo clusterInfo);

  MetricSeriesBuilder timeRange(LocalDateTime firstMetricTime, Duration duration);

  MetricSeriesBuilder sampleInterval(Duration interval);

  MetricSeriesBuilder series(
      BiFunction<MetricGenerator, Integer, Stream<? extends HasBeanObject>> seriesGenerator);

  ClusterBean build();

  final class MetricGenerator {

    private final ClusterInfo clusterInfo;
    private final int broker;
    private final LocalDateTime time;

    public MetricGenerator(ClusterInfo clusterInfo, int broker, LocalDateTime time) {
      this.clusterInfo = clusterInfo;
      this.broker = broker;
      this.time = time;
    }

    public LocalDateTime now() {
      return time;
    }

    public <T extends HasBeanObject> Stream<T> perOnlinePartitionLeader(
        Function<Replica, T> mapper) {
      throw new UnsupportedOperationException();
    }

    public <T extends HasBeanObject> Stream<T> perTopic(Function<String, T> mapper) {
      throw new UnsupportedOperationException();
    }

    public <T extends HasBeanObject> Stream<T> perPartition(Function<Partition, T> mapper) {
      throw new UnsupportedOperationException();
    }

    public <T extends HasBeanObject> Stream<T> perReplica(Function<Replica, T> mapper) {
      throw new UnsupportedOperationException();
    }

    public ServerMetrics.Topic.Meter topic(
        ServerMetrics.Topic metric, String topic, Map<String, Object> attributes) {
      var domainName = ServerMetrics.DOMAIN_NAME;
      var properties =
          Map.of("type", "BrokerTopicMetric", "topic", topic, "name", metric.metricName());
      return new ServerMetrics.Topic.Meter(
          new BeanObject(domainName, properties, attributes, time.toEpochSecond(ZoneOffset.UTC)));
    }

    public LogMetrics.Log.Gauge logSize(TopicPartition topicPartition, long size) {
      var domainName = LogMetrics.DOMAIN_NAME;
      var properties =
          Map.of(
              "type", "BrokerTopicMetric",
              "topic", topicPartition.topic(),
              "partition", String.valueOf(topicPartition.partition()),
              "name", LogMetrics.Log.SIZE.metricName());
      var attributes = Map.<String, Object>of("value", size);
      return new LogMetrics.Log.Gauge(new BeanObject(domainName, properties, attributes));
    }
  }
}
