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
package org.astraea.common.balancer.log;

import java.util.Objects;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

/**
 * Describe the log allocation state that is associate with a subset of topic/partition of a Kafka
 * cluster.
 */
public interface ClusterLogAllocation extends ClusterInfo<Replica> {

  /**
   * Construct a {@link ClusterLogAllocation} from the given list of {@link Replica}.
   *
   * <p>Be aware that this class describes <strong>the replica lists of a subset of
   * topic/partitions</strong>. It doesn't require the topic/partition part to have cluster-wide
   * complete information. But the replica list has to be complete. Provide a partial replica list
   * might result in data loss or unintended replica drop during rebalance plan proposing &
   * execution.
   */
  static ClusterLogAllocation of(ClusterInfo<Replica> clusterInfo) {
    // sanity check: no moving replicas
    if (clusterInfo.replicaStream().anyMatch(r -> r.isFuture() || r.isAdding() || r.isRemoving()))
      throw new IllegalArgumentException("There are moving replicas. Stop re-balance plan");
    clusterInfo
        .topicPartitions()
        .forEach(
            topicPartition -> {
              var replicas = clusterInfo.replicas(topicPartition);
              // sanity check: no duplicate preferred leader
              var preferredLeaderCount =
                  replicas.stream().filter(Replica::isPreferredLeader).count();
              if (preferredLeaderCount > 1)
                throw new IllegalArgumentException(
                    "Duplicate preferred leader in " + topicPartition);
              if (preferredLeaderCount < 1)
                throw new IllegalArgumentException(
                    "Illegal preferred leader count in "
                        + topicPartition
                        + ": "
                        + preferredLeaderCount);
              // sanity check: no duplicate node info
              if (replicas.stream().map(ReplicaInfo::nodeInfo).map(NodeInfo::id).distinct().count()
                  != replicas.size())
                throw new IllegalArgumentException(
                    "Duplicate replica inside the replica list of " + topicPartition);
            });
    return new ClusterLogAllocationImpl(clusterInfo);
  }

  /**
   * let specific broker leave the replica set and let another broker join the replica set.
   *
   * @param replica the replica to perform replica migration
   * @param toBroker the id of the broker about to replace the removed broker
   * @param toDir the absolute path of the data directory this migrated replica is supposed to be on
   *     the destination broker. This value cannot null.
   */
  ClusterLogAllocation migrateReplica(TopicPartitionReplica replica, int toBroker, String toDir);

  /** let specific replica become the preferred leader of its associated topic/partition. */
  ClusterLogAllocation becomeLeader(TopicPartitionReplica replica);

  class ClusterLogAllocationImpl extends ClusterInfo.Optimized<Replica>
      implements ClusterLogAllocation {
    private ClusterLogAllocationImpl(ClusterInfo<Replica> clusterInfo) {
      super(clusterInfo.nodes(), clusterInfo.replicas());
    }

    @Override
    public ClusterLogAllocation migrateReplica(
        TopicPartitionReplica replica, int toBroker, String toDir) {
      // we don't offer a way to take advantage fo kafka implementation detail to decide which
      // data directory the replica should be. This kind of usage might make the executor
      // complicate,
      // and it is probably rarely used. Also, not offering much value to the problem.
      Objects.requireNonNull(toDir, "The destination data directory must be specified explicitly");

      var topicPartition = TopicPartition.of(replica.topic(), replica.partition());
      var theReplica =
          replica(replica)
              .orElseThrow(() -> new IllegalArgumentException("No such replica: " + replica));
      var newReplica =
          Replica.builder(theReplica)
              .nodeInfo(
                  nodes().stream()
                      .filter(n -> n.id() == toBroker)
                      .findFirst()
                      .orElse(NodeInfo.of(toBroker, "?", -1)))
              .path(toDir)
              .build();

      return of(
          ClusterInfo.of(
              nodes(),
              replicaStream()
                  .map(r -> r == theReplica ? newReplica : r)
                  .collect(Collectors.toUnmodifiableList())));
    }

    @Override
    public ClusterLogAllocation becomeLeader(TopicPartitionReplica replica) {
      final var topicPartition = TopicPartition.of(replica.topic(), replica.partition());
      final var source =
          replica(replica)
              .orElseThrow(() -> new IllegalArgumentException("No such replica: " + replica));
      final var target =
          replicas(topicPartition).stream()
              .filter(Replica::isPreferredLeader)
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "No preferred leader found for "
                              + topicPartition
                              + ", this replica list is probably corrupted."));

      final var newSource = Replica.builder(source).isPreferredLeader(true).build();
      final var newTarget = Replica.builder(target).isPreferredLeader(false).build();

      return of(
          ClusterInfo.of(
              nodes(),
              replicaStream()
                  .map(r -> (r == source ? newSource : (r == target ? newTarget : (r))))
                  .collect(Collectors.toUnmodifiableList())));
    }
  }
}
