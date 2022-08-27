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
            .flatMap(topic -> before.replicas(topic).stream())
            .map(replicaInfo -> (Replica) replicaInfo)
            .map(
                replica ->
                    Map.entry(
                        TopicPartitionReplica.of(
                            replica.topic(), replica.partition(), replica.nodeInfo().id()),
                        replica.size()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    var diff = ClusterInfo.diff(before, after);
    var migrateInfo = migrateInfo(diff, replicaSize);
    var sizeChanges = migrateInfo.sizeChange;
    var totalMigrateSize = migrateInfo.totalMigrateSize;
    return new MoveCost() {
      @Override
      public String name() {
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

  static MigrateInfo migrateInfo(
      ClusterInfo.Diff diff, Map<TopicPartitionReplica, Long> replicaSize) {

    var changes = new HashMap<Integer, Long>();
    diff.sourceChange.forEach(
        (tp, brokerId) ->
            changes.put(
                brokerId,
                -replicaSize.get(TopicPartitionReplica.of(tp.topic(), tp.partition(), brokerId))
                    + changes.getOrDefault(brokerId, 0L)));
    diff.sinkChange.forEach(
        (tp, brokerId) ->
            changes.put(
                brokerId,
                replicaSize.get(
                        TopicPartitionReplica.of(
                            tp.topic(),
                            tp.partition(),
                            diff.sourceChange.get(TopicPartition.of(tp.topic(), tp.partition()))))
                    + changes.getOrDefault(brokerId, 0L)));
    var totalSizeChange =
        diff.sourceChange.entrySet().stream()
            .mapToLong(
                e ->
                    replicaSize.get(
                        TopicPartitionReplica.of(
                            e.getKey().topic(), e.getKey().partition(), e.getValue())))
            .sum();
    return new MigrateInfo(totalSizeChange, changes);
  }
}
