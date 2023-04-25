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
package org.astraea.common.cost;

import static org.astraea.common.cost.MigrationCost.replicaLeaderChanged;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;

/** more replica leaders -> higher cost */
public class ReplicaLeaderCost implements HasBrokerCost, HasClusterCost, HasMoveCost, ResourceUsageHint {
  private final Dispersion dispersion = Dispersion.cov();
  private final Configuration config;
  public static final String MAX_MIGRATE_LEADER_KEY = "max.migrated.leader.number";

  public ReplicaLeaderCost() {
    this.config = Configuration.of(Map.of());
  }

  public ReplicaLeaderCost(Configuration config) {
    this.config = config;
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        leaderCount(clusterInfo).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var brokerScore = leaderCount(clusterInfo);
    var value = dispersion.calculate(brokerScore.values());
    return ClusterCost.of(
        value,
        () ->
            brokerScore.values().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ", "{", "}")));
  }

  static Map<Integer, Integer> leaderCount(ClusterInfo clusterInfo) {
    return clusterInfo.nodes().stream()
        .map(nodeInfo -> Map.entry(nodeInfo.id(), clusterInfo.replicaLeaders(nodeInfo.id()).size()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Optional<MetricSensor> metricSensor() {
    return Optional.of(
        (client, ignored) -> List.of(ServerMetrics.ReplicaManager.LEADER_COUNT.fetch(client)));
  }

  public Configuration config() {
    return this.config;
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    // var moveCost = replicaLeaderChanged(before, after);
    var maxMigratedLeader =
         config.string(MAX_MIGRATE_LEADER_KEY).map(Long::parseLong).orElse(Long.MAX_VALUE);
    // var overflow =
    //     maxMigratedLeader < moveCost.values().stream().map(Math::abs).mapToLong(s -> s).sum();
    long count = before.topicPartitions()
        .stream()
        .filter(tp -> {
          var a = before.replicaLeader(tp).orElseThrow();
          var b = after.replicaLeader(tp).orElseThrow();
          return b.nodeInfo().id() != a.nodeInfo().id();
        })
        .count();
    return () -> count >= maxMigratedLeader;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  @Override
  public ResourceUsage evaluateClusterResourceUsage(ClusterInfo clusterInfo, ClusterBean clusterBean, Replica target) {
    return new ResourceUsage(Map.of(
        "migrated_leaders",
        clusterInfo.replicaLeader(target.topicPartition())
            .map(originLeader ->
                target.isPreferredLeader() &&
                    originLeader.nodeInfo().id() != target.nodeInfo().id() ? 1.0 : 0.0)
            .orElseThrow()));
  }

  @Override
  public ResourceUsage evaluateReplicaResourceUsage(ClusterInfo clusterInfo, ClusterBean clusterBean, Replica target) {
    return new ResourceUsage();
  }

  @Override
  public Collection<ResourceCapacity> evaluateClusterResourceCapacity(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    return List.of(new ResourceCapacity() {
      @Override
      public String resourceName() {
        return "migrated_leaders";
      }

      @Override
      public double optimalUsage() {
        return config.string(MAX_MIGRATE_LEADER_KEY).map(Long::parseLong).orElse(Long.MAX_VALUE);
      }

      @Override
      public double idealness(ResourceUsage usage) {
        var a = usage.usage().getOrDefault(resourceName(), 0.0) / optimalUsage();
        return a;
      }

      @Override
      public Comparator<ResourceUsage> usageIdealnessComparator() {
        return Comparator.comparingDouble(this::idealness);
        // return (a,b) -> {
        //   var aa = a.usage().getOrDefault(resourceName(), 0.0);
        //   var bb = b.usage().getOrDefault(resourceName(), 0.0);

        //   if(aa >= optimalUsage() && bb >= optimalUsage()) return 0;
        //   if(aa >= optimalUsage()) return 1;
        //   if(bb >= optimalUsage()) return -1;
        //   return 0;
        // };
        // return Comparator.comparingDouble(ru -> ru.usage().getOrDefault(resourceName(), 0.0));
      }

      @Override
      public Predicate<ResourceUsage> usageValidnessPredicate() {
        return (ru) -> ru.usage().getOrDefault(resourceName(), 0.0) < optimalUsage();
      }
    });
  }
}
