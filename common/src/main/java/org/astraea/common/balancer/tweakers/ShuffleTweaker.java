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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartitionReplica;

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
  private final Predicate<String> allowedTopics;
  private final Predicate<Integer> allowedBrokers;

  public ShuffleTweaker(
      Supplier<Integer> numberOfShuffle,
      Predicate<String> allowedTopics,
      Predicate<Integer> allowedBrokers) {
    this.numberOfShuffle = numberOfShuffle;
    this.allowedTopics = allowedTopics;
    this.allowedBrokers = allowedBrokers;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Stream<ClusterInfo> generate(ClusterInfo baseAllocation) {
    // There is no broker
    if (baseAllocation.nodes().isEmpty()) return Stream.of();

    // No replica to working on.
    if (baseAllocation.replicas().size() == 0) return Stream.of();

    // Only one broker & one folder exists, unable to do any meaningful log migration
    if (baseAllocation.nodes().size() == 1
        && baseAllocation.brokerFolders().values().stream().findFirst().orElseThrow().size() == 1)
      return Stream.of();

    final var legalReplicas =
        baseAllocation.topicPartitions().stream()
            .filter(tp -> this.allowedTopics.test(tp.topic()))
            .filter(tp -> eligiblePartition(baseAllocation.replicas(tp)))
            .flatMap(baseAllocation::replicaStream)
            .filter(r -> this.allowedBrokers.test(r.nodeInfo().id()))
            .toList();

    return Stream.generate(
        () -> {
          final var shuffleCount = numberOfShuffle.get();
          final var replicaOrder = Utils.shuffledPermutation(legalReplicas).iterator();
          final var forbiddenReplica = new HashSet<TopicPartitionReplica>();

          final var finalCluster = ClusterInfo.builder(baseAllocation);
          for (int shuffled = 0; replicaOrder.hasNext() && shuffled < shuffleCount; ) {
            final var sourceReplica = replicaOrder.next();

            // the leadership change operation will not only affect source target but also the
            // target replica. To prevent mutating one replica twice in the tweaking loop. We have
            // to mandatory exclude the target replica since it has been touched in this tweak.
            // Tweaking a replica twice is meaningless and incompatible with the design of
            // ClusterInfoBuilder.
            if (forbiddenReplica.contains(sourceReplica.topicPartitionReplica())) continue;

            Supplier<Boolean> leadershipChange =
                () -> {
                  var targetReplica =
                      baseAllocation
                          .replicaStream(sourceReplica.topicPartition())
                          // leader pair follower, follower pair leader
                          .filter(r -> r.isFollower() != sourceReplica.isFollower())
                          // this follower is located at allowed broker
                          .filter(r -> this.allowedBrokers.test(r.nodeInfo().id()))
                          // not forbidden
                          .filter(r -> !forbiddenReplica.contains(r.topicPartitionReplica()))
                          .map(r -> Map.entry(r, ThreadLocalRandom.current().nextInt()))
                          .min(Map.Entry.comparingByValue())
                          .map(Map.Entry::getKey);

                  // allowed broker filter might cause no legal exchange target
                  if (targetReplica.isPresent()) {
                    var theFollower =
                        sourceReplica.isFollower() ? sourceReplica : targetReplica.orElseThrow();
                    finalCluster.setPreferredLeader(theFollower.topicPartitionReplica());

                    forbiddenReplica.add(sourceReplica.topicPartitionReplica());
                    forbiddenReplica.add(targetReplica.orElseThrow().topicPartitionReplica());

                    return true;
                  } else {
                    return false;
                  }
                };
            Supplier<Boolean> replicaListChange =
                () -> {
                  var replicaList = baseAllocation.replicas(sourceReplica.topicPartition());
                  var targetBroker =
                      baseAllocation.brokers().stream()
                          // the candidate should not be part of the replica list
                          .filter(
                              b -> replicaList.stream().noneMatch(r -> r.nodeInfo().id() == b.id()))
                          // should be an allowed broker
                          .filter(b -> this.allowedBrokers.test(b.id()))
                          .map(b -> Map.entry(b, ThreadLocalRandom.current().nextInt()))
                          .min(Map.Entry.comparingByValue())
                          .map(Map.Entry::getKey);

                  if (targetBroker.isPresent()) {
                    finalCluster.reassignReplica(
                        sourceReplica.topicPartitionReplica(),
                        targetBroker.orElseThrow().id(),
                        randomElement(
                            baseAllocation.brokerFolders().get(targetBroker.orElseThrow().id())));

                    forbiddenReplica.add(sourceReplica.topicPartitionReplica());
                    return true;
                  } else {
                    return false;
                  }
                };

            final var isFinished =
                Operation.randomStream()
                    .sequential()
                    .map(
                        operation ->
                            switch (operation) {
                              case LEADERSHIP_CHANGE -> leadershipChange.get();
                              case REPLICA_LIST_CHANGE -> replicaListChange.get();
                            })
                    .filter(finished -> finished)
                    .findFirst()
                    .orElse(false);

            shuffled += isFinished ? 1 : 0;
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

    private static final List<Operation> OPERATIONS = Arrays.stream(Operation.values()).toList();

    public static Stream<Operation> randomStream() {
      return OPERATIONS.stream()
          .map(x -> Map.entry(x, ThreadLocalRandom.current().nextInt()))
          .sorted(Map.Entry.comparingByValue())
          .map(Map.Entry::getKey);
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

  public static class Builder {

    private Supplier<Integer> numberOfShuffle = () -> ThreadLocalRandom.current().nextInt(1, 5);
    private Predicate<String> allowedTopics = (name) -> true;
    private Predicate<Integer> allowedBrokers = (name) -> true;

    private Builder() {}

    public Builder numberOfShuffle(Supplier<Integer> numberOfShuffle) {
      this.numberOfShuffle = numberOfShuffle;
      return this;
    }

    public Builder allowedTopics(Predicate<String> allowedTopics) {
      this.allowedTopics = allowedTopics;
      return this;
    }

    public Builder allowedBrokers(Predicate<Integer> allowedBrokers) {
      this.allowedBrokers = allowedBrokers;
      return this;
    }

    public ShuffleTweaker build() {
      return new ShuffleTweaker(numberOfShuffle, allowedTopics, allowedBrokers);
    }
  }
}
