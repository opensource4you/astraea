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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.NodeInfo;

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

  private int sourceTopicPartitionSelector(List<TopicPartition> migrationCandidates) {
    return ThreadLocalRandom.current().nextInt(0, migrationCandidates.size());
  }

  private int sourceLogPlacementSelector(List<LogPlacement> migrationCandidates) {
    return ThreadLocalRandom.current().nextInt(0, migrationCandidates.size());
  }

  private <T> T randomElement(Collection<T> collection) {
    return collection.stream()
        .skip(ThreadLocalRandom.current().nextInt(0, collection.size()))
        .findFirst()
        .orElseThrow();
  }

  @Override
  public Stream<RebalancePlanProposal> generate(
      ClusterInfo clusterInfo, ClusterLogAllocation baseAllocation) {
    return Stream.generate(
        () -> {
          final var rebalancePlanBuilder = RebalancePlanProposal.builder();
          final var brokerIds =
              clusterInfo.nodes().stream()
                  .map(NodeInfo::id)
                  .collect(Collectors.toUnmodifiableSet());

          if (brokerIds.size() == 0)
            return rebalancePlanBuilder
                .addWarning("Why there is no broker?")
                .noRebalancePlan()
                .build();

          if (brokerIds.size() == 1)
            return rebalancePlanBuilder
                .addWarning("Only one broker exists. There is no reason to rebalance.")
                .noRebalancePlan()
                .build();

          if (clusterInfo.topics().size() == 0)
            return rebalancePlanBuilder
                .addWarning("No non-ignored topic to working on.")
                .noRebalancePlan()
                .build();

          final var shuffleCount = numberOfShuffle.get();
          final var newAllocation = LayeredClusterLogAllocation.of(baseAllocation);
          final var pickingList =
              newAllocation.topicPartitionStream().collect(Collectors.toUnmodifiableList());

          rebalancePlanBuilder.addInfo(
              "Make " + shuffleCount + (shuffleCount > 0 ? " shuffles." : " shuffle."));
          for (int i = 0; i < shuffleCount; i++) {
            final var sourceTopicPartitionIndex = sourceTopicPartitionSelector(pickingList);
            final var sourceTopicPartition = pickingList.get(sourceTopicPartitionIndex);
            final var sourceLogPlacements = newAllocation.logPlacements(sourceTopicPartition);
            final var sourceLogPlacementIndex = sourceLogPlacementSelector(sourceLogPlacements);
            final var sourceLogPlacement = sourceLogPlacements.get(sourceLogPlacementIndex);
            final var sourceIsLeader = sourceLogPlacementIndex == 0;
            final var sourceBroker = sourceLogPlacement.broker();

            Consumer<Integer> replicaSetMigration =
                (targetBroker) -> {
                  var destDir = randomElement(clusterInfo.dataDirectories(targetBroker));
                  newAllocation.migrateReplica(
                      sourceTopicPartition, sourceBroker, targetBroker, destDir);
                  rebalancePlanBuilder.addInfo(
                      String.format(
                          "Change replica set of topic %s partition %d, from %d to %d at %s.",
                          sourceTopicPartition.topic(),
                          sourceTopicPartition.partition(),
                          sourceLogPlacement.broker(),
                          targetBroker,
                          destDir));
                };
            Consumer<LogPlacement> leaderFollowerMigration =
                (newLeaderReplicaCandidate) -> {
                  newAllocation.letReplicaBecomeLeader(
                      sourceTopicPartition, newLeaderReplicaCandidate.broker());
                  rebalancePlanBuilder.addInfo(
                      String.format(
                          "Change the log identity of topic %s partition %d replica at broker %d, from %s to %s",
                          sourceTopicPartition.topic(),
                          sourceTopicPartition.partition(),
                          sourceLogPlacement.broker(),
                          sourceIsLeader ? "leader" : "follower",
                          sourceIsLeader ? "follower" : "leader"));
                };

            // generate a set of valid migration broker for given placement.
            final var validMigrationCandidates = new ArrayList<Movement>();
            // [Valid movement 1] add all brokers and remove all broker in current replica set
            brokerIds.stream()
                .filter(
                    broker -> sourceLogPlacements.stream().noneMatch(log -> log.broker() == broker))
                .map(targetBroker -> (Runnable) () -> replicaSetMigration.accept(targetBroker))
                .map(Movement::replicaSetMovement)
                .forEach(validMigrationCandidates::add);
            // [Valid movement 2] add all leader/follower change candidate
            if (sourceLogPlacements.size() > 1) {
              if (sourceIsLeader) {
                // leader can migrate its identity to follower.
                sourceLogPlacements.stream()
                    .skip(1)
                    .map(
                        followerReplica ->
                            (Runnable) () -> leaderFollowerMigration.accept(followerReplica))
                    .map(Movement::replicaSetMovement)
                    .forEach(validMigrationCandidates::add);
              } else {
                // follower can migrate its identity to leader.
                validMigrationCandidates.add(
                    Movement.replicaSetMovement(
                        () -> leaderFollowerMigration.accept(sourceLogPlacement)));
              }
            }

            // pick a migration and execute
            final var selectedMigrationIndex = randomElement(validMigrationCandidates);
            selectedMigrationIndex.run();
          }

          return rebalancePlanBuilder.withRebalancePlan(newAllocation).build();
        });
  }

  /** Action to trigger upon this migration */
  interface Movement extends Runnable {
    static ReplicaSetMovement replicaSetMovement(Runnable runnable) {
      return runnable::run;
    }
  }

  interface ReplicaSetMovement extends Movement {}
}
