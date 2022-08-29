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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.collector.Fetcher;

public class ReplicaSizeMoveCost implements HasMoveCost {

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(LogMetrics.Log.SIZE::fetch);
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
    var replicaSize =
        before.topics().stream()
            .flatMap(topic -> before.replicas(topic).stream())
            .filter(ReplicaInfo::isLeader)
            .map(
                replica ->
                    Map.entry(
                        TopicPartition.of(replica.topic(), replica.partition()), replica.size()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    var beforeChanges = ClusterInfo.diff4TopicPartitionReplica(before, after);
    var afterChanges = ClusterInfo.diff4TopicPartitionReplica(after, before);
    var migrateInfo = migrateInfo(beforeChanges, afterChanges, replicaSize);

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
      Set<Replica> sourceChange, Set<Replica> sinkChange, Map<TopicPartition, Long> replicaSize) {
    var changes = new HashMap<Integer, Long>();
    AtomicLong totalSizeChange = new AtomicLong(0L);
    sourceChange.forEach(
        replica -> {
          var size = replicaSize.get(TopicPartition.of(replica.topic(), replica.partition()));
          changes.put(
              replica.nodeInfo().id(), -size + changes.getOrDefault(replica.nodeInfo().id(), 0L));
          totalSizeChange.set(totalSizeChange.get() + size);
        });
    sinkChange.forEach(
        replica -> {
          changes.put(
              replica.nodeInfo().id(),
              replicaSize.get(TopicPartition.of(replica.topic(), replica.partition()))
                  + changes.getOrDefault(replica.nodeInfo().id(), 0L));
        });
    return new MigrateInfo(totalSizeChange.get(), changes);
  }
}
