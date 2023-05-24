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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.function.Bi3Function;

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

  MetricSeriesBuilder seriesByBroker(
      BiFunction<LocalDateTime, Integer, Stream<? extends HasBeanObject>> seriesGenerator);

  MetricSeriesBuilder seriesByBrokerTopic(
      Bi3Function<LocalDateTime, Integer, String, HasBeanObject> seriesGenerator);

  MetricSeriesBuilder seriesByBrokerPartition(
      Bi3Function<LocalDateTime, Integer, TopicPartition, HasBeanObject> seriesGenerator);

  MetricSeriesBuilder seriesByBrokerReplica(
      Bi3Function<LocalDateTime, Integer, Replica, HasBeanObject> seriesGenerator);

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
    public MetricSeriesBuilder seriesByBroker(
        BiFunction<LocalDateTime, Integer, Stream<? extends HasBeanObject>> seriesGenerator) {
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
                          cluster.brokers().stream()
                              .map(
                                  node ->
                                      Map.entry(node.id(), seriesGenerator.apply(time, node.id()))))
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Map.Entry::getKey, Map.Entry::getValue, Stream::concat)));
      return this;
    }

    @Override
    public MetricSeriesBuilder seriesByBrokerTopic(
        Bi3Function<LocalDateTime, Integer, String, HasBeanObject> seriesGenerator) {
      final var cluster = clusterInfo;
      final var start = timeStart;
      final var end = timeStart.plus(timeRange);
      final var interval = sampleInterval;
      this.series.add(
          () ->
              cluster.brokers().stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Broker::id,
                          node ->
                              Stream.iterate(
                                      start,
                                      (t) -> t.isBefore(end) || t.isEqual(end),
                                      (t) -> t.plus(interval))
                                  .flatMap(
                                      time ->
                                          cluster
                                              .replicaStream(node.id())
                                              .map(Replica::topic)
                                              .distinct()
                                              .map(
                                                  topic ->
                                                      seriesGenerator.apply(
                                                          time, node.id(), topic))))));
      return this;
    }

    @Override
    public MetricSeriesBuilder seriesByBrokerPartition(
        Bi3Function<LocalDateTime, Integer, TopicPartition, HasBeanObject> seriesGenerator) {
      final var cluster = clusterInfo;
      final var start = timeStart;
      final var end = timeStart.plus(timeRange);
      final var interval = sampleInterval;
      this.series.add(
          () ->
              cluster.brokers().stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Broker::id,
                          node ->
                              Stream.iterate(
                                      start,
                                      (t) -> t.isBefore(end) || t.isEqual(end),
                                      (t) -> t.plus(interval))
                                  .flatMap(
                                      time ->
                                          cluster
                                              .replicaStream(node.id())
                                              .map(Replica::topicPartition)
                                              .map(
                                                  partition ->
                                                      seriesGenerator.apply(
                                                          time, node.id(), partition))))));
      return this;
    }

    @Override
    public MetricSeriesBuilder seriesByBrokerReplica(
        Bi3Function<LocalDateTime, Integer, Replica, HasBeanObject> seriesGenerator) {
      final var cluster = clusterInfo;
      final var start = timeStart;
      final var end = timeStart.plus(timeRange);
      final var interval = sampleInterval;
      this.series.add(
          () ->
              cluster.brokers().stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Broker::id,
                          node ->
                              Stream.iterate(
                                      start,
                                      (t) -> t.isBefore(end) || t.isEqual(end),
                                      (t) -> t.plus(interval))
                                  .flatMap(
                                      time ->
                                          cluster
                                              .replicaStream(node.id())
                                              .map(
                                                  replica ->
                                                      seriesGenerator.apply(
                                                          time, node.id(), replica))))));
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
}
