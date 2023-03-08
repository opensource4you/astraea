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
package org.astraea.common.balancer.tweakers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;

/**
 * The {@link ShuffleTweaker} proposes a new log placement based on the current log placement, but
 * with a few random placement changes. <br>
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
public class ShuffleTweaker {

  private final Supplier<Integer> numberOfShuffle;
  private final Predicate<String> topicFilter;

  public ShuffleTweaker(Supplier<Integer> numberOfShuffle, Predicate<String> topicFilter) {
    this.numberOfShuffle = numberOfShuffle;
    this.topicFilter = topicFilter;
  }

  public Stream<ClusterInfo> generate(ClusterInfo baseAllocation) {
    // There is no broker
    if (baseAllocation.nodes().isEmpty()) return Stream.of();

    // No non-ignored topic to working on.
    if (baseAllocation.topicPartitions().isEmpty()) return Stream.of();

    // Only one broker & one folder exists, unable to do any log migration
    if (baseAllocation.nodes().size() == 1
        && baseAllocation.brokerFolders().values().stream().findFirst().orElseThrow().size() == 1)
      return Stream.of();

    return Stream.generate(
        () -> {
          final var shuffleCount = numberOfShuffle.get();
          final var partitionOrder =
              baseAllocation.topicPartitions().stream()
                  .filter(tp -> topicFilter.test(tp.topic()))
                  .map(tp -> Map.entry(tp, ThreadLocalRandom.current().nextInt()))
                  .sorted(Map.Entry.comparingByValue())
                  .map(Map.Entry::getKey)
                  .collect(Collectors.toUnmodifiableList());

          final var finalCluster = ClusterInfoBuilder.builder(baseAllocation);
          for (int i = 0, shuffled = 0; i < partitionOrder.size() && shuffled < shuffleCount; i++) {
            final var tp = partitionOrder.get(i);
            if (!eligiblePartition(baseAllocation.replicas(tp))) continue;
            switch (Operation.random()) {
              case LEADERSHIP_CHANGE:
                {
                  // change leader/follower identity
                  var replica =
                      baseAllocation
                          .replicaStream(tp)
                          .filter(Replica::isFollower)
                          .map(r -> Map.entry(r, ThreadLocalRandom.current().nextInt()))
                          .min(Map.Entry.comparingByValue())
                          .map(Map.Entry::getKey);
                  if (replica.isPresent()) {
                    finalCluster.setPreferredLeader(replica.get().topicPartitionReplica());
                    shuffled++;
                  }
                  break;
                }
              case REPLICA_LIST_CHANGE:
                {
                  // change replica list
                  var replicaList = baseAllocation.replicas(tp);
                  var currentIds =
                      replicaList.stream()
                          .map(Replica::nodeInfo)
                          .map(NodeInfo::id)
                          .collect(Collectors.toUnmodifiableSet());
                  var broker =
                      baseAllocation.brokers().stream()
                          .filter(b -> !currentIds.contains(b.id()))
                          .map(b -> Map.entry(b, ThreadLocalRandom.current().nextInt()))
                          .min(Map.Entry.comparingByValue())
                          .map(Map.Entry::getKey);
                  if (broker.isPresent()) {
                    var replica = randomElement(replicaList);
                    finalCluster.reassignReplica(
                        replica.topicPartitionReplica(),
                        broker.get().id(),
                        randomElement(baseAllocation.brokerFolders().get(broker.get().id())));
                    shuffled++;
                  }
                  break;
                }
              default:
                throw new RuntimeException("Unexpected Condition");
            }
          }

          return finalCluster.build();
        });
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
            r -> r.stream().noneMatch(Replica::isLeader))
        .noneMatch(p -> p.test(replicas));
  }

  enum Operation implements EnumInfo {
    LEADERSHIP_CHANGE,
    REPLICA_LIST_CHANGE;

    private static final List<Operation> OPERATIONS =
        Arrays.stream(Operation.values()).collect(Collectors.toUnmodifiableList());

    public static Operation random() {
      return OPERATIONS.get(ThreadLocalRandom.current().nextInt(OPERATIONS.size()));
    }

    public static Operation ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Operation.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }
  }
}
