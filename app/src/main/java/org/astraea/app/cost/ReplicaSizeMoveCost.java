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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.collector.Fetcher;

public class ReplicaSizeMoveCost implements HasMoveCost {

  static class MigrateInfo {
    TopicPartition topicPartition;
    int brokerSource;
    int brokerSink;

    public MigrateInfo(TopicPartition topicPartition, int brokerSource, int brokerSink) {
      this.topicPartition = topicPartition;
      this.brokerSource = brokerSource;
      this.brokerSink = brokerSink;
    }

    TopicPartitionReplica sourceTPR() {
      return TopicPartitionReplica.of(
          topicPartition.topic(), topicPartition.partition(), brokerSource);
    }
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(LogMetrics.Log.SIZE::fetch);
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var replicaSize =
        clusterBean.mapByReplica().entrySet().stream()
            .map(
                metrics ->
                    Map.entry(
                        metrics.getKey(),
                        LogMetrics.Log.gauges(metrics.getValue(), LogMetrics.Log.SIZE)))
            .flatMap(
                gauges ->
                    gauges.getValue().stream()
                        .map(gauge -> Map.entry(gauges.getKey(), gauge.value())))
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey, Map.Entry::getValue, (x1, x2) -> x2));
    var replicaChanges = getMigrateReplicas(before, after, false);
    var totalMigrateSize =
        replicaChanges.stream().mapToDouble(x -> replicaSize.getOrDefault(x.sourceTPR(), 0L)).sum();
    return new MoveCost() {
      @Override
      public String name() {
        return "size";
      }

      @Override
      public long totalCost() {
        return (long) totalMigrateSize;
      }

      @Override
      public String unit() {
        return "byte";
      }

      @Override
      public Map<Integer, Integer> changes() {
        return Map.of(1,0,2,-100,3,100);
      }
    };
  }

  static TopicPartition toTP(TopicPartitionReplica tpr) {
    return TopicPartition.of(tpr.topic(), tpr.partition());
  }

  static List<MigrateInfo> getMigrateReplicas(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, boolean fromLeader) {
    var leaderReplicas =
        originClusterInfo.topics().stream()
            .flatMap(topic -> originClusterInfo.availableReplicaLeaders(topic).stream())
            .map(
                replicaInfo ->
                    Map.entry(
                        TopicPartition.of(replicaInfo.topic(), replicaInfo.partition()),
                        replicaInfo.nodeInfo().id()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var beforeMigrateReplicas =
        originClusterInfo.topics().stream()
            .flatMap(topic -> originClusterInfo.availableReplicas(topic).stream())
            .map(
                replicaInfo ->
                    TopicPartitionReplica.of(
                        replicaInfo.topic(), replicaInfo.partition(), replicaInfo.nodeInfo().id()))
            .collect(Collectors.toList());
    var afterMigrateReplicas =
        newClusterInfo.topics().stream()
            .flatMap(topic -> newClusterInfo.availableReplicas(topic).stream())
            .map(
                replicaInfo ->
                    TopicPartitionReplica.of(
                        replicaInfo.topic(), replicaInfo.partition(), replicaInfo.nodeInfo().id()))
            .collect(Collectors.toList());
    if (fromLeader)
      return afterMigrateReplicas.stream()
          .filter(newTPR -> !beforeMigrateReplicas.contains(newTPR))
          .map(
              newTPR -> {
                var tp = toTP(newTPR);
                var sourceBroker = leaderReplicas.get(tp);
                var sinkBroker = newTPR.brokerId();
                return new MigrateInfo(tp, sourceBroker, sinkBroker);
              })
          .collect(Collectors.toList());
    else {
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
      var change = new ArrayList<MigrateInfo>();
      if (sourceChange.keySet().containsAll(sinkChange.keySet())
          && sourceChange.values().size() == sinkChange.values().size())
        sourceChange.forEach(
            (sourceTP, sourceBroker) -> {
              var sinkBroker = sinkChange.get(sourceTP);
              change.add(new MigrateInfo(sourceTP, sourceBroker, sinkBroker));
            });
      return change;
    }
  }
}
