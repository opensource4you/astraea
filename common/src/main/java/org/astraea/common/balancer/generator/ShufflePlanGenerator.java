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
package org.astraea.common.balancer.generator;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.balancer.RebalancePlanProposal;
import org.astraea.common.balancer.log.ClusterLogAllocation;

/**
 * The {@link ShufflePlanGenerator} proposes a new log placement based on the current log placement,
 * but with a few random placement changes. <br>
 * <br>
 * The following operations are considered as a valid shuffle action:
 *
 * <ol>
 *   <li>Change the leader/follower of a partition to a member of this replica set, the original
 *       leader/follower becomes a follower/leader.
 *   <li>Remove a replica from the replica set, then add another broker(must not be part of the
 *       replica set before this action) into the replica set.
 * </ol>
 */
public class ShufflePlanGenerator implements RebalancePlanGenerator {

  private final Supplier<Integer> numberOfShuffle;

  public ShufflePlanGenerator(int origin, int bound) {
    this(() -> ThreadLocalRandom.current().nextInt(origin, bound));
  }

  public ShufflePlanGenerator(Supplier<Integer> numberOfShuffle) {
    this.numberOfShuffle = numberOfShuffle;
  }

  @Override
  public Stream<RebalancePlanProposal> generate(
      Map<Integer, Set<String>> brokerFolders, ClusterLogAllocation baseAllocation) {
    if (brokerFolders.isEmpty()) {
      return Stream.of(
          RebalancePlanProposal.builder()
              .clusterLogAllocation(baseAllocation)
              .addWarning("There is no broker")
              .build());
    }

    if (brokerFolders.size() == 1) {
      return Stream.of(
          RebalancePlanProposal.builder()
              .clusterLogAllocation(baseAllocation)
              .addWarning("Only one broker exists, unable to do some migration")
              .build());
    }

    if (baseAllocation.topicPartitions().isEmpty()) {
      return Stream.of(
          RebalancePlanProposal.builder()
              .clusterLogAllocation(baseAllocation)
              .addWarning("No non-ignored topic to working on.")
              .build());
    }

    var index = new AtomicInteger(0);
    return Stream.generate(
        () -> {
          final var rebalancePlanBuilder = RebalancePlanProposal.builder();
          final var shuffleCount = numberOfShuffle.get();

          rebalancePlanBuilder.addInfo(
              "Make " + shuffleCount + (shuffleCount > 0 ? " shuffles." : " shuffle."));

          var candidates =
              IntStream.range(0, shuffleCount)
                  .mapToObj(i -> allocationGenerator(brokerFolders, rebalancePlanBuilder))
                  .collect(Collectors.toUnmodifiableList());

          var currentAllocation = baseAllocation;
          for (var candidate : candidates) currentAllocation = candidate.apply(currentAllocation);

          return rebalancePlanBuilder
              .index(index.getAndIncrement())
              .clusterLogAllocation(currentAllocation)
              .build();
        });
  }

  private static Function<ClusterLogAllocation, ClusterLogAllocation> allocationGenerator(
      Map<Integer, Set<String>> brokerFolders, RebalancePlanProposal.Build rebalancePlanBuilder) {
    return currentAllocation -> {
      final var selectedPartition =
          currentAllocation.topicPartitions().stream()
              .filter(tp -> eligiblePartition((currentAllocation.replicas(tp))))
              .map(tp -> Map.entry(tp, ThreadLocalRandom.current().nextInt()))
              .min(Map.Entry.comparingByValue())
              .map(Map.Entry::getKey)
              .orElseThrow();

      // [valid operation 1] change leader/follower identity
      final var currentReplicas = currentAllocation.replicas(selectedPartition);
      final var candidates0 =
          currentReplicas.stream()
              .skip(1)
              .map(
                  follower ->
                      (Supplier<ClusterLogAllocation>)
                          () -> {
                            rebalancePlanBuilder.addInfo(
                                String.format(
                                    "Change the log identity of topic %s partition %d replica at broker %d, from follower to leader",
                                    follower.topic(),
                                    follower.partition(),
                                    follower.nodeInfo().id()));
                            return currentAllocation.becomeLeader(follower.topicPartitionReplica());
                          });

      // [valid operation 2] change replica list
      final var currentIds =
          currentReplicas.stream()
              .map(ReplicaInfo::nodeInfo)
              .map(NodeInfo::id)
              .collect(Collectors.toUnmodifiableSet());
      final var candidates1 =
          brokerFolders.keySet().stream()
              .filter(brokerId -> !currentIds.contains(brokerId))
              .flatMap(
                  toThisBroker ->
                      currentReplicas.stream()
                          .map(
                              replica ->
                                  (Supplier<ClusterLogAllocation>)
                                      () -> {
                                        var toThisDir =
                                            randomElement(brokerFolders.get(toThisBroker));
                                        rebalancePlanBuilder.addInfo(
                                            String.format(
                                                "Change replica set of topic %s partition %d, from %d to %d at %s.",
                                                replica.topic(),
                                                replica.partition(),
                                                replica.nodeInfo().id(),
                                                toThisBroker,
                                                toThisDir));
                                        return currentAllocation.migrateReplica(
                                            replica.topicPartitionReplica(),
                                            toThisBroker,
                                            toThisDir);
                                      }));

      return randomElement(
              Stream.concat(candidates0, candidates1).collect(Collectors.toUnmodifiableSet()))
          .get();
    };
  }

  private static <T> T randomElement(Collection<T> collection) {
    return collection.stream()
        .skip(ThreadLocalRandom.current().nextInt(0, collection.size()))
        .findFirst()
        .orElseThrow();
  }

  private static boolean eligiblePartition(Collection<Replica> replicas) {
    return Stream.<Predicate<Collection<Replica>>>of(
            // only one replica and it is offline
            r -> r.size() == 1 && r.stream().findFirst().orElseThrow().isOffline(),
            // no leader
            r -> r.stream().noneMatch(ReplicaInfo::isLeader))
        .noneMatch(p -> p.test(replicas));
  }
}
