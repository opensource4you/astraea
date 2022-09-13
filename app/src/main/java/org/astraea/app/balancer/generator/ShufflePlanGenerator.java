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
package org.astraea.app.balancer.generator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.common.admin.TopicPartition;

/**
 * The {@link ShufflePlanGenerator} proposes a new log placement based on the current log placement,
 * but with a few random placement changes. <br>
 * <br>
 * The following operations are considered as a valid shuffle action:
 *
 * <ol>
 *   <li>Remove a replica from the replica set, then add another broker(must not be part of the
 *       replica set before this action) into the replica set. Noted that this operation doesn't
 *       specify which data directory the moving replica will eventually be on the destination
 *       broker. It's likely that the replica will follow the default placement scheme, see the
 *       LogManager#nextLogDirs method implementation in Apache Kafka server for more details.
 *   <li>Change the leader/follower of a partition by a member of this replica set, the original
 *       leader/follower becomes a follower/leader.
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
      var pickingList = new ArrayList<>(currentAllocation.topicPartitions());
      final var sourceTopicPartitionIndex = sourceTopicPartitionSelector(pickingList);
      final var sourceTopicPartition = pickingList.get(sourceTopicPartitionIndex);
      final var sourceLogPlacements = currentAllocation.logPlacements(sourceTopicPartition);
      final var sourceLogPlacementIndex = sourceLogPlacementSelector(sourceLogPlacements);
      final var sourceLogPlacement = sourceLogPlacements.get(sourceLogPlacementIndex);
      final var sourceIsLeader = sourceLogPlacementIndex == 0;
      final var sourceBroker = sourceLogPlacement.broker();
      var targetPlacements =
          sourceLogPlacements.size() > 1
              ? sourceIsLeader
                  ? sourceLogPlacements.stream().skip(1).collect(Collectors.toUnmodifiableList())
                  : List.of(sourceLogPlacement)
              : List.<LogPlacement>of();
      // generate a set of valid migration broker for given placement.
      final var validMigrationCandidates =
          Stream.concat(

                  // [Valid movement 1] add all brokers and remove all
                  // broker in current replica set
                  brokerFolders.keySet().stream()
                      .filter(
                          broker ->
                              sourceLogPlacements.stream().noneMatch(log -> log.broker() == broker))
                      .map(
                          targetBroker ->
                              (Supplier<ClusterLogAllocation>)
                                  () -> {
                                    var destDir = randomElement(brokerFolders.get(targetBroker));
                                    rebalancePlanBuilder.addInfo(
                                        String.format(
                                            "Change replica set of topic %s partition %d, from %d to %d at %s.",
                                            sourceTopicPartition.topic(),
                                            sourceTopicPartition.partition(),
                                            sourceLogPlacement.broker(),
                                            targetBroker,
                                            destDir));
                                    return currentAllocation.migrateReplica(
                                        sourceTopicPartition, sourceBroker, targetBroker, destDir);
                                  }),
                  // [Valid movement 2] add all leader/follower change
                  // candidate
                  targetPlacements.stream()
                      .map(
                          followerReplica ->
                              () -> {
                                rebalancePlanBuilder.addInfo(
                                    String.format(
                                        "Change the log identity of topic %s partition %d replica at broker %d, from %s to %s",
                                        sourceTopicPartition.topic(),
                                        sourceTopicPartition.partition(),
                                        sourceLogPlacement.broker(),
                                        sourceIsLeader ? "leader" : "follower",
                                        sourceIsLeader ? "follower" : "leader"));
                                return currentAllocation.letReplicaBecomeLeader(
                                    sourceTopicPartition, followerReplica.broker());
                              }))
              .collect(Collectors.toUnmodifiableList());
      // pick a migration and execute
      final var selectedMigrationIndex = randomElement(validMigrationCandidates);
      return selectedMigrationIndex.get();
    };
  }

  private static int sourceTopicPartitionSelector(List<TopicPartition> migrationCandidates) {
    return ThreadLocalRandom.current().nextInt(0, migrationCandidates.size());
  }

  private static int sourceLogPlacementSelector(List<LogPlacement> migrationCandidates) {
    return ThreadLocalRandom.current().nextInt(0, migrationCandidates.size());
  }

  private static <T> T randomElement(Collection<T> collection) {
    return collection.stream()
        .skip(ThreadLocalRandom.current().nextInt(0, collection.size()))
        .findFirst()
        .orElseThrow();
  }
}
