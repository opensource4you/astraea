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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.collector.Fetcher;

public class ReplicaSizeMoveCost implements HasMoveCost {

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(LogMetrics.Log.SIZE::fetch);
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var replicaSize =
        before.topics().stream()
            .flatMap(topic -> before.availableReplicas(topic).stream())
            .map(replicaInfo -> (Replica) replicaInfo)
            .map(
                replica ->
                    Map.entry(
                        TopicPartitionReplica.of(
                            replica.topic(), replica.partition(), replica.nodeInfo().id()),
                        replica.size()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    var migrateInfo = migrateInfo(before, after, replicaSize);
    var sizeChanges = migrateInfo.sizeChange;
    var totalMigrateSize = migrateInfo.totalMigrateSize;
    return new MoveCost() {
      @Override
      public String function() {
        return "size";
      }

      @Override
      public long totalCost() {
        return totalMigrateSize;
      }

      @Override
      public String unit() {
        return "byte";
      }

      @Override
      public Map<Integer, Long> changes() {
        return sizeChanges;
      }
    };
  }

  static class MigrateInfo {
    long totalMigrateSize;
    Map<Integer, Long> sizeChange;

    MigrateInfo(long totalMigrateSize, Map<Integer, Long> sizeChange) {
      this.totalMigrateSize = totalMigrateSize;
      this.sizeChange = sizeChange;
    }
  }

  static TopicPartition toTP(TopicPartitionReplica tpr) {
    return TopicPartition.of(tpr.topic(), tpr.partition());
  }

  static MigrateInfo migrateInfo(
      ClusterInfo before, ClusterInfo after, Map<TopicPartitionReplica, Long> replicaSize) {
    var beforeMigrateReplicas =
        before.topics().stream()
            .flatMap(topic -> before.availableReplicas(topic).stream())
            .map(
                replicaInfo ->
                    TopicPartitionReplica.of(
                        replicaInfo.topic(), replicaInfo.partition(), replicaInfo.nodeInfo().id()))
            .collect(Collectors.toList());
    var afterMigrateReplicas =
        after.topics().stream()
            .flatMap(topic -> after.availableReplicas(topic).stream())
            .map(
                replicaInfo ->
                    TopicPartitionReplica.of(
                        replicaInfo.topic(), replicaInfo.partition(), replicaInfo.nodeInfo().id()))
            .collect(Collectors.toList());
    var sourceChange =
        beforeMigrateReplicas.stream()
            .filter(oldTPR -> !afterMigrateReplicas.contains(oldTPR))
            .map(oldTPR -> Map.entry(toTP(oldTPR), oldTPR.brokerId()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var sinkChange =
        afterMigrateReplicas.stream()
            .filter(newTPR -> !beforeMigrateReplicas.contains(newTPR))
            .map(newTPR -> Map.entry(toTP(newTPR), newTPR.brokerId()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var changes = new HashMap<Integer, Long>();
    sourceChange.forEach(
        (tp, brokerId) ->
            changes.put(
                brokerId,
                -replicaSize.get(TopicPartitionReplica.of(tp.topic(), tp.partition(), brokerId))
                    + changes.getOrDefault(brokerId, 0L)));
    sinkChange.forEach(
        (tp, brokerId) ->
            changes.put(
                brokerId,
                replicaSize.get(
                        TopicPartitionReplica.of(
                            tp.topic(),
                            tp.partition(),
                            sourceChange.get(TopicPartition.of(tp.topic(), tp.partition()))))
                    + changes.getOrDefault(brokerId, 0L)));
    var totalSizeChange =
        sourceChange.entrySet().stream()
            .mapToLong(
                e ->
                    replicaSize.get(
                        TopicPartitionReplica.of(
                            e.getKey().topic(), e.getKey().partition(), e.getValue())))
            .sum();
    return new MigrateInfo(totalSizeChange, changes);
  }
}
