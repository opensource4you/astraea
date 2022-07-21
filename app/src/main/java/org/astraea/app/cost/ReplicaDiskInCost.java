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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.Fetcher;

/**
 * The result is computed by "Size.Value" ,and createdTimestamp in the metrics. "Size.Value"
 * responds to the replica log size of brokers. The calculation method of the score is the rate of
 * increase of log size per unit time divided by the upper limit of broker bandwidth.
 */
public class ReplicaDiskInCost implements HasBrokerCost, HasPartitionCost {
  Map<Integer, Integer> brokerBandwidthCap;

  public ReplicaDiskInCost(Map<Integer, Integer> brokerBandwidthCap) {
    this.brokerBandwidthCap = brokerBandwidthCap;
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    final Map<Integer, List<TopicPartitionReplica>> topicPartitionOfEachBroker =
        clusterInfo.topics().stream()
            .flatMap(topic -> clusterInfo.replicas(topic).stream())
            .map(
                replica ->
                    TopicPartitionReplica.of(
                        replica.topic(), replica.partition(), replica.nodeInfo().id()))
            .collect(Collectors.groupingBy(TopicPartitionReplica::brokerId));
    final var actual =
        clusterInfo.nodes().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    NodeInfo::id,
                    node -> topicPartitionOfEachBroker.getOrDefault(node.id(), List.of())));

    final var topicPartitionDataRate = topicPartitionDataRate(clusterBean, Duration.ofSeconds(3));

    final var brokerLoad =
        actual.entrySet().stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().stream()
                            .mapToDouble(
                                x ->
                                    topicPartitionDataRate.get(
                                        TopicPartition.of(x.topic(), x.partition())))
                            .sum()))
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue() / brokerBandwidthCap.get(entry.getKey()) / 1024 / 1024))
            .map(entry -> Map.entry(entry.getKey(), Math.min(entry.getValue(), 1)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return () -> brokerLoad;
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var replicaIn = replicaDataRate(clusterBean, Duration.ofSeconds(2));
    var replicaScore =
        new TreeMap<TopicPartitionReplica, Double>(
            Comparator.comparing(TopicPartitionReplica::brokerId)
                .thenComparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));
    replicaIn.forEach(
        (tpr, rate) -> {
          var score = rate / brokerBandwidthCap.get(tpr.brokerId()) / 1024.0 / 1024.0;
          if (score >= 1) score = 1;
          replicaScore.put(tpr, score);
        });
    var scoreForTopic =
        clusterInfo.topics().stream()
            .map(
                topic ->
                    Map.entry(
                        topic,
                        replicaScore.entrySet().stream()
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
                        replicaScore.entrySet().stream()
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
    return Optional.of(KafkaMetrics.TopicPartition.Size::fetch);
  }

  /**
   * Calculate the maximum increase rate of each topic/partition, across the whole cluster.
   *
   * @param clusterBean offers the metrics related to topic/partition size
   * @param sampleWindow the time interval for calculating the data rate, noted that if the metrics
   *     doesn't have the sufficient old metric then an exception will likely be thrown.
   * @return a map contain the maximum increase rate of each topic/partition log
   */
  public static Map<TopicPartition, Double> topicPartitionDataRate(
      ClusterBean clusterBean, Duration sampleWindow) {
    return replicaDataRate(clusterBean, sampleWindow).entrySet().stream()
        .map(
            x -> {
              var tpr = x.getKey();
              return Map.entry(TopicPartition.of(tpr.topic(), tpr.partition()), x.getValue());
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x1, x2) -> x1));
  }

  public static Map<TopicPartitionReplica, Double> replicaDataRate(
      ClusterBean clusterBean, Duration sampleWindow) {
    return clusterBean.mapByReplica().entrySet().parallelStream()
        .map(
            metrics -> {
              // calculate the increase rate over a specific window of time
              var sizeTimeSeries =
                  metrics.getValue().stream()
                      .filter(bean -> bean instanceof HasValue)
                      .filter(bean -> bean.beanObject().properties().get("type").equals("Log"))
                      .filter(bean -> bean.beanObject().properties().get("name").equals("Size"))
                      .map(bean -> (HasValue) bean)
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
                      / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp())
                          / 1000);
              return Map.entry(metrics.getKey(), dataRate);
            })
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
