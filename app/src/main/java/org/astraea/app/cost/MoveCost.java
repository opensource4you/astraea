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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.kafka.KafkaMetrics;

public class MoveCost implements HasMoveCost {

  @Override
  public Fetcher fetcher() {
    return Fetcher.of(
        List.of(
            client ->
                List.of(
                    KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client),
                    KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(client)),
            KafkaMetrics.TopicPartition.Size::fetch));
  }

  @Override
  public PartitionCost moveCost(
      ClusterInfo clusterInfo, ClusterLogAllocation clusterLogAllocation) {
    var migrateReplicas = getMigrateReplicas(clusterInfo, clusterLogAllocation);

    return new PartitionCost() {

      @Override
      public Map<TopicPartition, Double> value(String topic) {
        return null;
      }

      @Override
      public Map<TopicPartition, Double> value(int brokerId) {
        return null;
      }
    };
  }

  public Map<TopicPartitionReplica, Collection<Integer>> getMigrateReplicas(
      ClusterInfo clusterInfo, ClusterLogAllocation clusterLogAllocation) {
    var leaderReplicas =
        clusterInfo.topics().stream()
            .map(clusterInfo::availableReplicaLeaders)
            .flatMap(
                replicaInfos ->
                    replicaInfos.stream()
                        .map(
                            replicaInfo ->
                                Map.entry(
                                    new TopicPartition(
                                        replicaInfo.topic(), replicaInfo.partition()),
                                    TopicPartitionReplica.of(
                                        replicaInfo.topic(),
                                        replicaInfo.partition(),
                                        replicaInfo.nodeInfo().id()))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var beforeMigrate =
        clusterInfo.topics().stream()
            .map(clusterInfo::availableReplicas)
            .flatMap(
                replicaInfos ->
                    replicaInfos.stream()
                        .map(
                            replicaInfo ->
                                TopicPartitionReplica.of(
                                    replicaInfo.topic(),
                                    replicaInfo.partition(),
                                    replicaInfo.nodeInfo().id())))
            .collect(Collectors.toList());
    var afterMigrate =
        clusterLogAllocation
            .topicPartitionStream()
            .flatMap(
                tp ->
                    clusterLogAllocation.logPlacements(tp).stream()
                        .map(
                            logPlacement ->
                                TopicPartitionReplica.of(
                                    tp.topic(), tp.partition(), logPlacement.broker())))
            .collect(Collectors.toList());
    return afterMigrate.stream()
        .filter(newTPR -> !beforeMigrate.contains(newTPR))
        .map(
            newTPR ->
                Map.entry(
                    leaderReplicas.get(new TopicPartition(newTPR.topic(), newTPR.partition())),
                    List.of(newTPR.brokerId())))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (x1, x2) -> {
                  x1.addAll(x2);
                  return x1;
                }));
  }
}
