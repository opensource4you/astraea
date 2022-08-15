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
package org.astraea.app.cost;

import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.partitioner.Configuration;

/**
 * The result is computed by "Size.Value" ,and createdTimestamp in the metrics. "Size.Value"
 * responds to the replica log size of brokers. The calculation method of the score is the rate of
 * increase of log size per unit time divided by the upper limit of broker bandwidth.
 */
public class ReplicaDiskInCost implements HasClusterCost, HasBrokerCost, HasPartitionCost {
  private final Duration duration;
  private final Dispersion dispersion = Dispersion.correlationCoefficient();
  static final double OVERFLOW_SCORE = 9999.0;

  public ReplicaDiskInCost(Configuration configuration) {
    duration =
        Duration.ofSeconds(Integer.parseInt(configuration.string("metrics.duration").orElse("30")));
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var brokerCost = brokerCost(clusterInfo, clusterBean).value();
    // when retention occur, brokerCost will be set to -1 , and return a big score to reject this
    // plan.
    if (brokerCost.containsValue(-1.0)) return () -> OVERFLOW_SCORE;
    return () -> dispersion.calculate(brokerCost.values());
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var partitionCost = partitionCost(clusterInfo, clusterBean);
    var brokerLoad =
        clusterInfo.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        node.id(),
                        partitionCost.value(node.id()).values().stream()
                            .mapToDouble(rate -> rate)
                            .sum()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return () -> brokerLoad;
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var replicaIn = replicaDataRate(clusterBean, duration);
    var scoreForTopic =
        clusterInfo.topics().stream()
            .map(
                topic ->
                    Map.entry(
                        topic,
                        replicaIn.entrySet().stream()
                            .filter(x -> x.getKey().topic().equals(topic))
                            .collect(
                                Collectors.groupingBy(
                                    x ->
                                        TopicPartition.of(
                                            x.getKey().topic(), x.getKey().partition())))
                            .entrySet()
                            .stream()
                            .map(
                                entry ->
                                    Map.entry(
                                        entry.getKey(),
                                        entry.getValue().stream()
                                            .mapToDouble(Map.Entry::getValue)
                                            .max()
                                            .orElseThrow()))
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    var scoreForBroker =
        clusterInfo.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        node.id(),
                        replicaIn.entrySet().stream()
                            .filter(x -> x.getKey().brokerId() == node.id())
                            .collect(
                                Collectors.groupingBy(
                                    x ->
                                        TopicPartition.of(
                                            x.getKey().topic(), x.getKey().partition())))
                            .entrySet()
                            .stream()
                            .map(
                                entry ->
                                    Map.entry(
                                        entry.getKey(),
                                        entry.getValue().stream()
                                            .mapToDouble(Map.Entry::getValue)
                                            .max()
                                            .orElseThrow()))
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    // when retention occur, set all partitionScore to -1.
    if (replicaIn.containsValue(-1.0)) {
      return new PartitionCost() {
        @Override
        public Map<TopicPartition, Double> value(String topic) {
          return scoreForTopic.get(topic).keySet().stream()
              .map(x -> Map.entry(x, -1.0))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public Map<TopicPartition, Double> value(int brokerId) {
          return scoreForBroker.get(brokerId).keySet().stream()
              .map(x -> Map.entry(x, -1.0))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
      };
    }
    return new PartitionCost() {
      @Override
      public Map<TopicPartition, Double> value(String topic) {
        return scoreForTopic.get(topic);
      }

      @Override
      public Map<TopicPartition, Double> value(int brokerId) {
        return scoreForBroker.get(brokerId);
      }
    };
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(LogMetrics.Log.SIZE::fetch);
  }

  /**
   * Calculate the maximum increase rate of each topic/partition, across the whole cluster.
   *
   * @param clusterBean offers the metrics related to topic/partition size
   * @param sampleWindow the time interval for calculating the data rate, noted that if the metrics
   *     doesn't have the sufficient old metric then an exception will likely be thrown.
   * @return a map contain the maximum increase rate of each topic/partition log
   */
  public static Map<TopicPartitionReplica, Double> replicaDataRate(
      ClusterBean clusterBean, Duration sampleWindow) {
    return clusterBean.mapByReplica().entrySet().parallelStream()
        .map(
            metrics -> {
              // calculate the increase rate over a specific window of time
              var sizeTimeSeries =
                  LogMetrics.Log.meters(metrics.getValue(), LogMetrics.Log.SIZE).stream()
                      .sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed())
                      .collect(Collectors.toUnmodifiableList());
              var latestSize = sizeTimeSeries.stream().findFirst().orElseThrow();
              var windowSize =
                  sizeTimeSeries.stream()
                      .dropWhile(
                          bean ->
                              bean.createdTimestamp()
                                  > latestSize.createdTimestamp() - sampleWindow.toMillis())
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "No sufficient info to determine data rate, try later."));
              var dataRate =
                  ((double) (latestSize.value() - windowSize.value()))
                      / 1024.0
                      / 1024.0
                      / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp())
                          / 1000);
              // when retention occur, set all data rate to -1.
              if (dataRate < 0) dataRate = -1.0;
              return Map.entry(metrics.getKey(), dataRate);
            })
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
