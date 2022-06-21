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
package org.astraea.app.balancer.log;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.ReplicaInfo;

public class LayeredClusterLogAllocation implements ClusterLogAllocation {

  // set operation is guard by this
  private final AtomicBoolean isLocked = new AtomicBoolean(false);
  private final LayeredClusterLogAllocation upperLayer;

  // guard by this
  private final Map<TopicPartition, List<LogPlacement>> allocation;

  private LayeredClusterLogAllocation(
      LayeredClusterLogAllocation upperLayer, Map<TopicPartition, List<LogPlacement>> allocation) {
    allocation.keySet().stream()
        .collect(Collectors.groupingBy(TopicPartition::topic))
        .forEach(
            (topic, tp) -> {
              int maxPartitionId =
                  tp.stream().mapToInt(TopicPartition::partition).max().orElseThrow();
              if ((maxPartitionId + 1) != tp.size())
                throw new IllegalArgumentException(
                    "The partition size of " + topic + " is illegal");
            });
    allocation.forEach(
        (tp, logs) -> {
          long uniqueBrokers = logs.stream().map(LogPlacement::broker).distinct().count();
          if (uniqueBrokers != logs.size() || logs.size() == 0)
            throw new IllegalArgumentException(
                "The topic "
                    + tp.topic()
                    + " partition "
                    + tp.partition()
                    + " has illegal replica set "
                    + logs);
        });
    this.upperLayer = upperLayer;
    this.allocation = allocation;
  }

  public static LayeredClusterLogAllocation of(ClusterInfo clusterInfo) {
    final Map<TopicPartition, List<LogPlacement>> allocation =
        clusterInfo.topics().stream()
            .map(clusterInfo::replicas)
            .flatMap(Collection::stream)
            .collect(
                Collectors.groupingBy(
                    replica ->
                        TopicPartition.of(replica.topic(), Integer.toString(replica.partition()))))
            .entrySet()
            .stream()
            .map(
                (entry) -> {
                  // validate if the given log placements are valid
                  if (entry.getValue().stream().filter(ReplicaInfo::isLeader).count() != 1)
                    throw new IllegalArgumentException(
                        "The " + entry.getKey() + " leader count mismatch 1.");

                  final var topicPartition = entry.getKey();
                  final var logPlacements =
                      entry.getValue().stream()
                          .sorted(Comparator.comparingInt(replica -> replica.isLeader() ? 0 : 1))
                          .map(
                              replica ->
                                  LogPlacement.of(
                                      replica.nodeInfo().id(), replica.dataFolder().orElse(null)))
                          .collect(Collectors.toList());

                  return Map.entry(topicPartition, logPlacements);
                })
            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
    return new LayeredClusterLogAllocation(null, allocation);
  }

  public static LayeredClusterLogAllocation of(
      Map<TopicPartition, List<LogPlacement>> allocationMap) {
    return new LayeredClusterLogAllocation(null, allocationMap);
  }

  /**
   * Create a new {@link LayeredClusterLogAllocation}. If the given {@link ClusterLogAllocation} is
   * an instance of {@link LayeredClusterLogAllocation}. Then the given {@link
   * LayeredClusterLogAllocation} will be locked and become unmodifiable. The new log allocation
   * will continue from the unmodifiable allocation. Doing so can improve the performance of new log
   * allocation creation as we avoid the heavy data copy work.
   */
  public static LayeredClusterLogAllocation of(ClusterLogAllocation clusterLogAllocation) {
    if (clusterLogAllocation instanceof LayeredClusterLogAllocation) {
      return createNewLayer((LayeredClusterLogAllocation) clusterLogAllocation);
    } else {
      // no layer support, do the expensive copy work.
      final var map = new ConcurrentHashMap<TopicPartition, List<LogPlacement>>();
      clusterLogAllocation
          .topicPartitionStream()
          .forEach(
              topicPartition ->
                  map.put(topicPartition, clusterLogAllocation.logPlacements(topicPartition)));
      return of(map);
    }
  }

  /**
   * Create a new {@link LayeredClusterLogAllocation} based on the given one. The given layered
   * cluster log will become unmodifiable afterward.
   */
  private static LayeredClusterLogAllocation createNewLayer(
      LayeredClusterLogAllocation baseLogAllocation) {
    if (!baseLogAllocation.isLocked()) {
      baseLogAllocation.lockLayer();
    }
    return new LayeredClusterLogAllocation(baseLogAllocation, new ConcurrentHashMap<>());
  }

  /** Make this {@link LayeredClusterLogAllocation} unmodifiable. */
  private synchronized void lockLayer() {
    isLocked.set(true);
  }

  /** Is this {@link LayeredClusterLogAllocation} unmodifiable. */
  private boolean isLocked() {
    return isLocked.get();
  }

  private synchronized void ensureNotLocked() {
    if (isLocked())
      throw new IllegalStateException("This layered cluster log allocation has been locked");
  }

  private static OptionalInt indexOfBroker(List<LogPlacement> logPlacements, int targetBroker) {
    return IntStream.range(0, logPlacements.size())
        .filter(index -> logPlacements.get(index).broker() == targetBroker)
        .findFirst();
  }

  @Override
  public synchronized void migrateReplica(
      TopicPartition topicPartition, int broker, int destinationBroker, String toDir) {
    ensureNotLocked();

    final List<LogPlacement> sourceLogPlacements = this.logPlacements(topicPartition);
    if (sourceLogPlacements == null)
      throw new IllegalMigrationException(
          topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");

    int sourceLogIndex = indexOfBroker(sourceLogPlacements, broker).orElse(-1);
    if (sourceLogIndex == -1)
      throw new IllegalMigrationException(
          broker + " is not part of the replica set for " + topicPartition);

    int destinationLogIndex = indexOfBroker(sourceLogPlacements, destinationBroker).orElse(-1);
    if (destinationLogIndex != -1)
      throw new IllegalArgumentException(
          destinationBroker + " is already part of the replica set, no need to move");

    this.allocation.put(
        topicPartition,
        IntStream.range(0, sourceLogPlacements.size())
            .mapToObj(
                index ->
                    index == sourceLogIndex
                        ? LogPlacement.of(destinationBroker, toDir)
                        : sourceLogPlacements.get(index))
            .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public synchronized void letReplicaBecomeLeader(
      TopicPartition topicPartition, int followerReplica) {
    ensureNotLocked();

    final List<LogPlacement> sourceLogPlacements = this.logPlacements(topicPartition);
    if (sourceLogPlacements == null)
      throw new IllegalMigrationException(
          topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");

    int leaderLogIndex = 0;
    if (sourceLogPlacements.size() == 0)
      throw new IllegalStateException("This partition has no log");

    int followerLogIndex = indexOfBroker(sourceLogPlacements, followerReplica).orElse(-1);
    if (followerLogIndex == -1)
      throw new IllegalArgumentException(
          followerReplica + " is not part of the replica set for " + topicPartition);

    if (leaderLogIndex == followerLogIndex) return; // nothing to do

    final var leaderLog = this.logPlacements(topicPartition).get(leaderLogIndex);
    final var followerLog = this.logPlacements(topicPartition).get(followerLogIndex);

    this.allocation.put(
        topicPartition,
        IntStream.range(0, sourceLogPlacements.size())
            .mapToObj(
                index ->
                    index == leaderLogIndex
                        ? followerLog
                        : index == followerLogIndex ? leaderLog : sourceLogPlacements.get(index))
            .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public synchronized void changeDataDirectory(
      TopicPartition topicPartition, int broker, String path) {
    ensureNotLocked();

    final List<LogPlacement> sourceLogPlacements = this.logPlacements(topicPartition);
    if (sourceLogPlacements == null)
      throw new IllegalMigrationException(
          topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");

    int sourceLogIndex = indexOfBroker(sourceLogPlacements, broker).orElse(-1);
    if (sourceLogIndex == -1)
      throw new IllegalMigrationException(
          broker + " is not part of the replica set for " + topicPartition);

    final var oldLog = this.logPlacements(topicPartition).get(sourceLogIndex);
    final var newLog = LogPlacement.of(oldLog.broker(), path);
    this.allocation.put(
        topicPartition,
        IntStream.range(0, sourceLogPlacements.size())
            .mapToObj(index -> index == sourceLogIndex ? newLog : sourceLogPlacements.get(index))
            .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public List<LogPlacement> logPlacements(TopicPartition topicPartition) {
    if (allocation.containsKey(topicPartition)) return allocation.get(topicPartition);
    else if (upperLayer != null) return upperLayer.logPlacements(topicPartition);
    else return null;
  }

  @Override
  public Stream<TopicPartition> topicPartitionStream() {
    if (upperLayer == null) return this.allocation.keySet().stream();
    else {
      LayeredClusterLogAllocation baseAllocationLookUp = this;
      while (baseAllocationLookUp.upperLayer != null)
        baseAllocationLookUp = baseAllocationLookUp.upperLayer;
      // Ignore all the intermediate layers, just return the keys set of the base layer.
      // THIS WILL WORK, AS LONG AS THE UPPER LAYERS DO NOT DO THE FOLLOWING.
      // 1. delete topic (should balancer do this?)
      // 2. create topic (should balancer do this?)
      // 3. shrink partition size (impossible for the current Kafka)
      // 4. expand partition size (should balancer do this?)
      // Of course, we can consider every element in all the layers, but doing that hurt
      // performance.
      return baseAllocationLookUp.allocation.keySet().stream();
    }
  }
}
