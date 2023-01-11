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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
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

  static MetricSeriesBuilder builder() {
    return new MetricSeriesBuilderImpl();
  }

  MetricSeriesBuilder cluster(ClusterInfo clusterInfo);

  MetricSeriesBuilder timeRange(LocalDateTime firstMetricTime, Duration duration);

  MetricSeriesBuilder sampleInterval(Duration interval);

  MetricSeriesBuilder series(
      BiFunction<MetricGenerator, Integer, Stream<? extends HasBeanObject>> seriesGenerator);

  ClusterBean build();

  final class MetricSeriesBuilderImpl implements MetricSeriesBuilder {

    private final List<Supplier<Map<Integer, Stream<? extends HasBeanObject>>>> series =
        new ArrayList<>();

    private ClusterInfo clusterInfo;
    private LocalDateTime timeStart;
    private Duration timeRange;
    private Duration sampleInterval = Duration.ofSeconds(1);

    @Override
    public MetricSeriesBuilder cluster(ClusterInfo clusterInfo) {
      this.clusterInfo = Objects.requireNonNull(clusterInfo);
      return this;
    }

    @Override
    public MetricSeriesBuilder timeRange(LocalDateTime firstMetricTime, Duration duration) {
      this.timeStart = firstMetricTime;
      this.timeRange = duration;
      return this;
    }

    @Override
    public MetricSeriesBuilder sampleInterval(Duration interval) {
      if (interval.isNegative() || interval.isZero())
        throw new IllegalArgumentException("The sample interval must be positive");
      this.sampleInterval = interval;
      return this;
    }

    @Override
    public MetricSeriesBuilder series(
        BiFunction<MetricGenerator, Integer, Stream<? extends HasBeanObject>> seriesGenerator) {
      // the series state is decided at the call time, instead of build time.
      final var cluster = clusterInfo;
      final var start = timeStart;
      final var end = timeStart.plus(timeRange);
      final var interval = sampleInterval;
      this.series.add(
          () ->
              Stream.iterate(
                      start, (t) -> t.isBefore(end) || t.isEqual(end), (t) -> t.plus(interval))
                  .flatMap(
                      time ->
                          cluster.nodes().stream()
                              .map(node -> new MetricGenerator(cluster, node, time)))
                  .collect(
                      Collectors.toUnmodifiableMap(
                          gen -> gen.node().id(),
                          gen -> seriesGenerator.apply(gen, gen.node().id()),
                          Stream::concat)));
      return this;
    }

    @Override
    public ClusterBean build() {
      Map<Integer, Collection<HasBeanObject>> allMetrics =
          this.series.stream()
              .map(Supplier::get)
              .flatMap(metrics -> metrics.entrySet().stream())
              .collect(
                  Collectors.groupingBy(
                      Map.Entry::getKey,
                      Collectors.flatMapping(
                          Map.Entry::getValue, Collectors.toCollection(ArrayList::new))));
      return ClusterBean.of(allMetrics);
    }
  }

  final class MetricGenerator {

    private final ClusterInfo clusterInfo;
    private final NodeInfo node;
    private final LocalDateTime time;

    MetricGenerator(ClusterInfo clusterInfo, NodeInfo node, LocalDateTime time) {
      this.clusterInfo = clusterInfo;
      this.node = node;
      this.time = time;
    }

    public LocalDateTime now() {
      return time;
    }

    public ClusterInfo cluster() {
      return clusterInfo;
    }

    public NodeInfo node() {
      return node;
    }

    public <T extends HasBeanObject> Stream<T> perBrokerTopic(Function<String, T> mapper) {
      return clusterInfo.replicaStream(node.id()).map(Replica::topic).distinct().map(mapper);
    }

    public <T extends HasBeanObject> Stream<T> perBrokerPartition(
        Function<TopicPartition, T> mapper) {
      return clusterInfo.replicaStream(node.id()).map(Replica::topicPartition).map(mapper);
    }

    public <T extends HasBeanObject> Stream<T> perBrokerReplica(Function<Replica, T> mapper) {
      return clusterInfo.replicaStream(node.id()).map(mapper);
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
              "type", LogMetrics.LOG_TYPE,
              "topic", topicPartition.topic(),
              "partition", String.valueOf(topicPartition.partition()),
              "name", LogMetrics.Log.SIZE.metricName());
      var attributes = Map.<String, Object>of("Value", size);
      return new LogMetrics.Log.Gauge(
          new BeanObject(domainName, properties, attributes, time.toEpochSecond(ZoneOffset.UTC)));
    }
  }
}
