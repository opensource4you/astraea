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

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.kafka.HasValue;
import org.astraea.app.metrics.kafka.KafkaMetrics;

/**
 * The result is computed by "Size.Value". "Size.Value" responds to the replica log size of brokers.
 * The calculation method of the score is the replica log usage space divided by the available space
 * on the hard disk
 */
public class ReplicaSizeCost implements HasBrokerCost, HasPartitionCost {
  Map<Integer, Integer> totalBrokerCapacity;

  public ReplicaSizeCost(Map<Integer, Integer> totalBrokerCapacity) {
    this.totalBrokerCapacity = totalBrokerCapacity;
  }

  @Override
  public Fetcher fetcher() {
    return KafkaMetrics.TopicPartition.Size::fetch;
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the ratio of the used space to the free space of each broker
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var sizeOfReplica = getReplicaSize(clusterInfo);
    var totalReplicaSizeInBroker =
        clusterInfo.nodes().stream()
            .collect(
                Collectors.toMap(
                    NodeInfo::id,
                    y ->
                        sizeOfReplica.entrySet().stream()
                            .filter(tpr -> tpr.getKey().brokerId() == y.id())
                            .mapToLong(Map.Entry::getValue)
                            .sum()));
    var brokerSizeScore =
        totalReplicaSizeInBroker.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    y ->
                        Double.valueOf(y.getValue())
                            / totalBrokerCapacity.get(y.getKey())
                            / 1048576));
    return () -> brokerSizeScore;
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the ratio of the used space to the available space of replicas in
   *     each broker
   */
  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo) {
    final long ONEMEGA = Math.round(Math.pow(2, 20));
    var sizeOfReplica = getReplicaSize(clusterInfo);
    TreeMap<TopicPartitionReplica, Double> replicaCost =
        new TreeMap<>(
            Comparator.comparing(TopicPartitionReplica::brokerId)
                .thenComparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));
    sizeOfReplica.forEach(
        (tpr, size) ->
            replicaCost.put(
                tpr, (double) size / totalBrokerCapacity.get(tpr.brokerId()) / ONEMEGA));

    return new PartitionCost() {

      @Override
      public Map<TopicPartition, Double> value(String topic) {
        return clusterInfo.replicas(topic).stream()
            .map(
                partitionInfo ->
                    new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            .map(
                tp -> {
                  final var score =
                      replicaCost.entrySet().stream()
                          .filter(
                              x ->
                                  x.getKey().topic().equals(tp.topic())
                                      && (x.getKey().partition() == tp.partition()))
                          .mapToDouble(Map.Entry::getValue)
                          .max()
                          .orElseThrow(
                              () ->
                                  new IllegalStateException(
                                      tp + " topic/partition size not found"));
                  return Map.entry(tp, score);
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @Override
      public Map<TopicPartition, Double> value(int brokerId) {
        return replicaCost.entrySet().stream()
            .sorted(Comparator.comparing(x -> x.getKey().topic()))
            .collect(Collectors.toList())
            .stream()
            .filter((tprScore) -> tprScore.getKey().brokerId() == brokerId)
            .collect(
                Collectors.toMap(
                    x -> new TopicPartition(x.getKey().topic(), x.getKey().partition()),
                    Map.Entry::getValue));
      }
    };
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a map contain the replica log size of each topic/partition
   */
  public Map<TopicPartitionReplica, Long> getReplicaSize(ClusterInfo clusterInfo) {
    return clusterInfo.beans().broker().entrySet().stream()
        .flatMap(
            brokerBeanObjects ->
                brokerBeanObjects.getValue().stream()
                    .filter(x -> x instanceof HasValue)
                    .filter(x -> x.beanObject().getProperties().get("type").equals("Log"))
                    .filter(x -> x.beanObject().getProperties().get("name").equals("Size"))
                    .map(x -> (HasValue) x)
                    .collect(
                        Collectors.toMap(
                            x ->
                                new TopicPartitionReplica(
                                    x.beanObject().getProperties().get("topic"),
                                    Integer.parseInt(
                                        x.beanObject().getProperties().get("partition")),
                                    brokerBeanObjects.getKey()),
                            HasValue::value))
                    .entrySet()
                    .stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
