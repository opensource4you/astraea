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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.FakeClusterInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClusterLogAllocationTest {

  @Test
  void creation() {
    // empty replica set
    var badAllocation0 = Map.of(TopicPartition.of("topic", "0"), List.<LogPlacement>of());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation0));

    // partial topic/partition
    var badAllocation1 =
        Map.of(TopicPartition.of("topic", "999"), List.of(LogPlacement.of(1001, "xx")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation1));

    // duplicate replica
    var badAllocation2 =
        Map.of(
            TopicPartition.of("topic", "0"),
            List.of(
                LogPlacement.of(1001, "xx"),
                LogPlacement.of(1001, "xx"),
                LogPlacement.of(1001, "xx")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation2));
  }

  @Test
  void migrateReplica() {
    final var fakeCluster = FakeClusterInfo.of(3, 1, 1, 1, (i) -> Set.of("topic"));
    var clusterLogAllocation = ClusterLogAllocation.of(fakeCluster);
    final var sourceTopicPartition = TopicPartition.of("topic", "0");

    clusterLogAllocation = clusterLogAllocation.migrateReplica(sourceTopicPartition, 0, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.logPlacements(sourceTopicPartition).get(0).broker());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"null", "/tmp/data-directory-0", "/tmp/data-directory-1", "/tmp/data-directory-2"})
  void migrateReplica(String dataDirectory) {
    dataDirectory = dataDirectory.equals("null") ? null : dataDirectory;
    final var fakeCluster = FakeClusterInfo.of(3, 1, 1, 1, (i) -> Set.of("topic"));
    var clusterLogAllocation = ClusterLogAllocation.of(fakeCluster);
    final var sourceTopicPartition0 = TopicPartition.of("topic", "0");

    clusterLogAllocation =
        clusterLogAllocation.migrateReplica(sourceTopicPartition0, 0, 1, dataDirectory);

    Assertions.assertEquals(
        1, clusterLogAllocation.logPlacements(sourceTopicPartition0).get(0).broker());
    Assertions.assertEquals(
        dataDirectory,
        clusterLogAllocation.logPlacements(sourceTopicPartition0).get(0).logDirectory());

    final var sourceTopicPartition1 = TopicPartition.of("topic", "0");
    clusterLogAllocation =
        clusterLogAllocation.migrateReplica(sourceTopicPartition1, 1, 1, dataDirectory);
    Assertions.assertEquals(
        1, clusterLogAllocation.logPlacements(sourceTopicPartition1).get(0).broker());
    Assertions.assertEquals(
        dataDirectory,
        clusterLogAllocation.logPlacements(sourceTopicPartition1).get(0).logDirectory());
  }

  @Test
  void letReplicaBecomeLeader() {
    final var fakeCluster = FakeClusterInfo.of(3, 1, 1, 2, (i) -> Set.of("topic"));
    var clusterLogAllocation = ClusterLogAllocation.of(fakeCluster);
    final var sourceTopicPartition = TopicPartition.of("topic", "0");

    clusterLogAllocation = clusterLogAllocation.letReplicaBecomeLeader(sourceTopicPartition, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.logPlacements(sourceTopicPartition).get(0).broker());
    Assertions.assertEquals(
        0, clusterLogAllocation.logPlacements(sourceTopicPartition).get(1).broker());
  }

  @Test
  void logPlacements() {
    final var allocation =
        ClusterLogAllocation.of(
            Map.of(TopicPartition.of("topic", "0"), List.of(LogPlacement.of(0, "/nowhere"))));

    Assertions.assertEquals(1, allocation.logPlacements(TopicPartition.of("topic", "0")).size());
    Assertions.assertEquals(
        0, allocation.logPlacements(TopicPartition.of("topic", "0")).get(0).broker());
    Assertions.assertEquals(
        "/nowhere",
        allocation.logPlacements(TopicPartition.of("topic", "0")).get(0).logDirectory());
    Assertions.assertEquals(0, allocation.logPlacements(TopicPartition.of("no", "0")).size());
    allocation.logPlacements(TopicPartition.of("no", "0"));
  }

  @Test
  void topicPartitionStream() {
    final var fakeCluster = FakeClusterInfo.of(10, 10, 10, 3);
    var allocation = ClusterLogAllocation.of(fakeCluster);
    for (var replica :
        fakeCluster.topics().stream()
            .flatMap(x -> fakeCluster.replicas(x).stream())
            .collect(Collectors.toUnmodifiableList()))
      allocation =
          allocation.letReplicaBecomeLeader(
              TopicPartition.of(replica.topic(), Integer.toString(replica.partition())), 1);

    final var allTopicPartitions =
        allocation.topicPartitions().stream()
            .sorted(
                Comparator.comparing(TopicPartition::topic)
                    .thenComparing(TopicPartition::partition))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(10 * 10, allTopicPartitions.size());
    final var expectedTopicPartitions =
        fakeCluster.topics().stream()
            .flatMap(x -> fakeCluster.replicas(x).stream())
            .map(x -> TopicPartition.of(x.topic(), Integer.toString(x.partition())))
            .distinct()
            .sorted(
                Comparator.comparing(TopicPartition::topic)
                    .thenComparing(TopicPartition::partition))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(expectedTopicPartitions, allTopicPartitions);
  }

  @Test
  void findNonFulfilledAllocation() {
    final var clusterInfo = FakeClusterInfo.of(3, 10, 10, 2);
    Assertions.assertEquals(
        Set.of(),
        ClusterLogAllocation.findNonFulfilledAllocation(
            ClusterLogAllocation.of(clusterInfo), ClusterLogAllocation.of(clusterInfo)));

    final var source = ClusterLogAllocation.of(clusterInfo);
    final var oneTopicPartition = source.topicPartitions().iterator().next();
    final var twoTopicPartition = source.topicPartitions().stream().skip(1).findFirst().get();

    final var target0 = source.migrateReplica(oneTopicPartition, 0, 0, "/somewhere");
    Assertions.assertEquals(
        Set.of(oneTopicPartition),
        ClusterLogAllocation.findNonFulfilledAllocation(source, target0));

    final var target1 = source.migrateReplica(oneTopicPartition, 0, 2);
    Assertions.assertEquals(
        Set.of(oneTopicPartition),
        ClusterLogAllocation.findNonFulfilledAllocation(source, target1));

    final var target2 = source.letReplicaBecomeLeader(oneTopicPartition, 1);
    Assertions.assertEquals(
        Set.of(oneTopicPartition),
        ClusterLogAllocation.findNonFulfilledAllocation(source, target2));

    final var target3 =
        source
            .migrateReplica(oneTopicPartition, 0, 2)
            .migrateReplica(oneTopicPartition, 2, 2, "/somewhere")
            .letReplicaBecomeLeader(twoTopicPartition, 1);
    Assertions.assertEquals(
        Set.of(oneTopicPartition, twoTopicPartition),
        ClusterLogAllocation.findNonFulfilledAllocation(source, target3));

    final var map4 =
        source.topicPartitions().stream().collect(Collectors.toMap(x -> x, source::logPlacements));
    map4.put(TopicPartition.of("NewTopic", 0), List.of(LogPlacement.of(0, "?")));
    final var target4 = ClusterLogAllocation.of(map4);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClusterLogAllocation.findNonFulfilledAllocation(source, target4));
  }
}
