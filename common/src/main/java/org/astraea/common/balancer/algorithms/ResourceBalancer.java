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
package org.astraea.common.balancer.algorithms;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.CompositeClusterCost;
import org.astraea.common.cost.ResourceCapacity;
import org.astraea.common.cost.ResourceUsage;
import org.astraea.common.cost.ResourceUsageHint;

public class ResourceBalancer implements Balancer {

  @Override
  public Optional<Plan> offer(AlgorithmConfig config) {
    var initialClusterInfo = config.clusterInfo();
    var initialCost =
        config.clusterCostFunction().clusterCost(config.clusterInfo(), config.clusterBean());

    var algorithm = new AlgorithmContext(config);
    var proposalClusterInfo = algorithm.execute();
    var proposalCost =
        config.clusterCostFunction().clusterCost(proposalClusterInfo, config.clusterBean());
    var moveCost =
        config
            .moveCostFunction()
            .moveCost(initialClusterInfo, proposalClusterInfo, config.clusterBean());

    if (proposalCost.value() > initialCost.value() || moveCost.overflow()) return Optional.empty();
    else
      return Optional.of(
          new Plan(initialClusterInfo, initialCost, proposalClusterInfo, proposalCost, moveCost));
  }

  static class AlgorithmContext {

    private final AlgorithmConfig config;
    private final ClusterInfo sourceCluster;
    private final ClusterBean clusterBean;

    private final Set<ResourceUsageHint> usageHints;
    private final List<ResourceCapacity> resourceCapacities;
    private final List<Replica> orderedReplicas;
    private final Predicate<ResourceUsage> feasibleUsage;

    private AlgorithmContext(AlgorithmConfig config) {
      this.config = config;
      this.sourceCluster = config.clusterInfo();
      this.clusterBean = config.clusterBean();

      // hints to estimate the resource usage of replicas
      this.usageHints =
          CompositeClusterCost.decompose(config.clusterCostFunction()).stream()
              .filter(func -> func instanceof ResourceUsageHint)
              .map(func -> (ResourceUsageHint) func)
              .collect(Collectors.toUnmodifiableSet());

      // hints for the capacity of each cluster resource
      this.resourceCapacities =
          usageHints.stream()
              .map(hint -> hint.evaluateClusterResourceCapacity(sourceCluster, clusterBean))
              .flatMap(Collection::stream)
              .collect(Collectors.toUnmodifiableList());

      // replicas are ordered by their resource usage, we tweak the most heavy resource first
      // TODO: add support for balancer.allowed.topics.regex
      // TODO: add support for balancer.allowed.brokers.regex
      this.orderedReplicas =
          sourceCluster.topicPartitions().stream()
              .filter(tp -> BalancerUtils.eligiblePartition(sourceCluster.replicas(tp)))
              .flatMap(tp -> sourceCluster.replicas(tp).stream())
              .sorted(
                  usageDominationComparator(
                      (r) -> {
                        var resource = new ResourceUsage();
                        usageHints.stream()
                            .map(
                                hint ->
                                    hint.evaluateReplicaResourceUsage(
                                        sourceCluster, clusterBean, r.topicPartitionReplica()))
                            .forEach(rrr -> resource.mergeUsage(rrr));
                        return resource;
                      }))
              .collect(Collectors.toUnmodifiableList());

      this.feasibleUsage =
          this.resourceCapacities.stream()
              .map(ResourceCapacity::usageValidnessPredicate)
              .reduce(Predicate::and)
              .orElse((u) -> true);
    }

    ClusterInfo execute() {
      var clusterResourceUsage = new ResourceUsage();
      sourceCluster.replicas().stream()
          .flatMap(this::evaluateReplicaUsage)
          .forEach(clusterResourceUsage::mergeUsage);

      var bestAllocation = new AtomicReference<ClusterInfo>();
      var bestAllocationScore = new AtomicReference<Double>();
      Consumer<List<Replica>> updateAnswer =
          (replicas) -> {
            var newCluster =
                ClusterInfo.of(
                    sourceCluster.clusterId(),
                    sourceCluster.nodes(),
                    sourceCluster.topics(),
                    replicas);
            var clusterCost = config.clusterCostFunction().clusterCost(newCluster, clusterBean);
            var moveCost =
                config.moveCostFunction().moveCost(sourceCluster, newCluster, clusterBean);

            // if movement constraint failed, reject answer
            if (moveCost.overflow()) return;
            // if cluster cost is better, accept answer
            if (clusterCost.value() < bestAllocationScore.get()) bestAllocation.set(newCluster);
          };

      // TODO: the recursion might overflow the stack under large number of replicas. use stack
      // instead.
      search(updateAnswer, 0, orderedReplicas, new HashMap<>(), clusterResourceUsage);

      return bestAllocation.get();
    }

    private int trials(int level) {
      // TODO: customize this
      if (0 <= level && level < 3) return 6;
      else if (level < 10) return 3;
      else if (level < 30) return 2;
      else return 1;
    }

    private void search(
        Consumer<List<Replica>> updateAnswer,
        int next,
        List<Replica> originalReplicas,
        Map<TopicPartition, List<Replica>> currentAllocation,
        ResourceUsage currentResourceUsage) {
      if (originalReplicas.size() == next) {
        // if this is a complete answer, call update function and return
        updateAnswer.accept(
            currentAllocation.entrySet().stream()
                .flatMap(x -> x.getValue().stream())
                .collect(Collectors.toUnmodifiableList()));
      } else {
        var nextReplica = originalReplicas.get(next);

        List<Map.Entry<ResourceUsage, Tweak>> possibleTweaks =
            tweaks(currentAllocation, nextReplica).stream()
                .map(
                    tweaks -> {
                      var usageAfterTweaked = new ResourceUsage(currentResourceUsage.usage());
                      tweaks.toRemove.stream()
                          .flatMap(this::evaluateReplicaUsage)
                          .forEach(usageAfterTweaked::removeUsage);
                      tweaks.toReplace.stream()
                          .flatMap(this::evaluateReplicaUsage)
                          .forEach(usageAfterTweaked::removeUsage);

                      return Map.entry(usageAfterTweaked, tweaks);
                    })
                .filter(e -> feasibleUsage.test(e.getKey()))
                .sorted(
                    Map.Entry.comparingByKey(
                        usageIdealnessDominationComparator(this.resourceCapacities)))
                // TODO: maybe change to probability style
                .limit(trials(next))
                .collect(Collectors.toUnmodifiableList());

        for (Map.Entry<ResourceUsage, Tweak> entry : possibleTweaks) {
          // the tweak we are going to use
          var newResourceUsage = entry.getKey();
          var tweaks = entry.getValue();

          // replace the replicas
          tweaks.toRemove.stream()
              .filter(replica -> !currentAllocation.get(replica.topicPartition()).remove(replica))
              .forEach(
                  nonexistReplica -> {
                    throw new IllegalStateException(
                        "Attempt to remove "
                            + nonexistReplica.topicPartitionReplica()
                            + " but it does not exists");
                  });
          tweaks.toReplace.forEach(
              replica -> currentAllocation.get(replica.topicPartition()).add(replica));

          // start next search stage
          search(updateAnswer, next + 1, originalReplicas, currentAllocation, newResourceUsage);

          // undo the tweak, restore the previous state
          tweaks.toReplace.stream()
              .filter(replica -> !currentAllocation.get(replica.topicPartition()).remove(replica))
              .forEach(
                  nonexistReplica -> {
                    throw new IllegalStateException(
                        "Attempt to remove "
                            + nonexistReplica.topicPartitionReplica()
                            + " but it does not exists");
                  });
          tweaks.toRemove.forEach(
              replica -> currentAllocation.get(replica.topicPartition()).add(replica));
        }
      }
    }

    private List<Tweak> tweaks(
        Map<TopicPartition, List<Replica>> currentAllocation, Replica replica) {
      // 1. no change
      var noMovement = List.of(new Tweak(List.of(), List.of()));

      // 2. leadership change
      var leadership =
          currentAllocation.get(replica.topicPartition()).stream()
              .filter(r -> r.isPreferredLeader() != replica.isPreferredLeader())
              .map(
                  switchTarget -> {
                    var toRemove = List.of(replica, switchTarget);
                    var toReplace =
                        List.of(
                            Replica.builder(replica)
                                .isLeader(!replica.isPreferredLeader())
                                .isPreferredLeader(!replica.isPreferredLeader())
                                .build(),
                            Replica.builder(switchTarget)
                                .isLeader(replica.isPreferredLeader())
                                .isPreferredLeader(replica.isPreferredLeader())
                                .build());

                    return new Tweak(toRemove, toReplace);
                  })
              .collect(Collectors.toUnmodifiableList());

      // 3. move to other data-dir at the same broker
      var dataFolderMovement =
          this.sourceCluster.brokerFolders().get(replica.nodeInfo().id()).stream()
              .filter(folder -> !folder.equals(replica.path()))
              .map(
                  newFolder ->
                      new Tweak(
                          List.of(replica),
                          List.of(Replica.builder(replica).path(newFolder).build())))
              .collect(Collectors.toUnmodifiableList());

      // 4. move to other brokers/data-dirs
      var interBrokerMovement =
          this.sourceCluster.brokers().stream()
              .filter(b -> b.id() == replica.nodeInfo().id())
              .flatMap(
                  b ->
                      b.dataFolders().stream()
                          .map(
                              folder ->
                                  new Tweak(
                                      List.of(replica),
                                      List.of(
                                          Replica.builder()
                                              .nodeInfo(b)
                                              .path(folder.path())
                                              .build()))))
              .collect(Collectors.toUnmodifiableList());

      return Stream.of(noMovement, leadership, dataFolderMovement, interBrokerMovement)
          .flatMap(Collection::stream)
          .collect(Collectors.toUnmodifiableList());
    }

    private Stream<ResourceUsage> evaluateReplicaUsage(Replica replica) {
      return this.usageHints.stream()
          .map(
              hint ->
                  hint.evaluateClusterResourceUsage(
                      sourceCluster, clusterBean, replica.topicPartitionReplica()));
    }

    static Comparator<Replica> usageDominationComparator(
        Function<Replica, ResourceUsage> usageHints) {
      return (lhs, rhs) -> {
        var resourceL = usageHints.apply(lhs);
        var resourceR = usageHints.apply(rhs);

        var dominatedByL =
            resourceL.usage().entrySet().stream()
                .filter(
                    e ->
                        !resourceR.usage().containsKey(e.getKey())
                            || e.getValue() > resourceR.usage().get(e.getKey()))
                .count();
        var dominatedByR =
            resourceR.usage().entrySet().stream()
                .filter(
                    e ->
                        !resourceL.usage().containsKey(e.getKey())
                            || e.getValue() > resourceL.usage().get(e.getKey()))
                .count();

        // reverse the order intentionally, we want the most dominated replica at the beginning of
        // list.
        int compare = Long.compare(dominatedByL, dominatedByR);
        return -compare;
      };
    }

    static Comparator<ResourceUsage> usageIdealnessDominationComparator(
        List<ResourceCapacity> resourceCapacities) {
      var comparators =
          resourceCapacities.stream()
              .map(ResourceCapacity::usageIdealnessComparator)
              .collect(Collectors.toUnmodifiableSet());

      return (lhs, rhs) -> {
        var dominatedByL = comparators.stream().filter(e -> e.compare(lhs, rhs) > 0).count();
        var dominatedByR = comparators.stream().filter(e -> e.compare(rhs, lhs) > 0).count();

        return Long.compare(dominatedByL, dominatedByR);
      };
    }
  }

  private static class Tweak {
    private final List<Replica> toRemove;
    private final List<Replica> toReplace;

    private Tweak(List<Replica> toRemove, List<Replica> toReplace) {
      this.toRemove = toRemove;
      this.toReplace = toReplace;
    }
  }
}
