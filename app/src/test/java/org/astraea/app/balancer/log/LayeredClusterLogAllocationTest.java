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
import org.astraea.app.cost.ClusterInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LayeredClusterLogAllocationTest {

  @Test
  void creation() {
    // empty replica set
    var badAllocation0 = Map.of(TopicPartition.of("topic", "0"), List.<LogPlacement>of());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> LayeredClusterLogAllocation.of(badAllocation0));

    // partial topic/partition
    var badAllocation1 = Map.of(TopicPartition.of("topic", "999"), List.of(LogPlacement.of(1001)));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> LayeredClusterLogAllocation.of(badAllocation1));

    // duplicate replica
    var badAllocation2 =
        Map.of(
            TopicPartition.of("topic", "0"),
            List.of(LogPlacement.of(1001), LogPlacement.of(1001), LogPlacement.of(1001)));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> LayeredClusterLogAllocation.of(badAllocation2));
  }

  @Test
  void migrateReplica() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 1, (i) -> Set.of("topic"));
    final var clusterLogAllocation = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", "0");

    clusterLogAllocation.migrateReplica(sourceTopicPartition, 0, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.logPlacements(sourceTopicPartition).get(0).broker());
    Assertions.assertNull(
        clusterLogAllocation
            .logPlacements(sourceTopicPartition)
            .get(0)
            .logDirectory()
            .orElse(null));
    Assertions.assertDoesNotThrow(() -> LayeredClusterLogAllocation.of(clusterLogAllocation));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"null", "/tmp/data-directory-0", "/tmp/data-directory-1", "/tmp/data-directory-2"})
  void migrateReplica(String dataDirectory) {
    dataDirectory = dataDirectory.equals("null") ? null : dataDirectory;
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 1, (i) -> Set.of("topic"));
    final var clusterLogAllocation = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var sourceTopicPartition0 = TopicPartition.of("topic", "0");

    clusterLogAllocation.migrateReplica(sourceTopicPartition0, 0, 1, dataDirectory);

    Assertions.assertEquals(
        1, clusterLogAllocation.logPlacements(sourceTopicPartition0).get(0).broker());
    Assertions.assertEquals(
        dataDirectory,
        clusterLogAllocation
            .logPlacements(sourceTopicPartition0)
            .get(0)
            .logDirectory()
            .orElse(null));

    final var sourceTopicPartition1 = TopicPartition.of("topic", "0");
    clusterLogAllocation.migrateReplica(sourceTopicPartition1, 1, 1, dataDirectory);
    Assertions.assertEquals(
        1, clusterLogAllocation.logPlacements(sourceTopicPartition1).get(0).broker());
    Assertions.assertEquals(
        dataDirectory,
        clusterLogAllocation
            .logPlacements(sourceTopicPartition1)
            .get(0)
            .logDirectory()
            .orElse(null));

    Assertions.assertDoesNotThrow(() -> LayeredClusterLogAllocation.of(clusterLogAllocation));
  }

  @Test
  void letReplicaBecomeLeader() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 2, (i) -> Set.of("topic"));
    final var clusterLogAllocation = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", "0");

    clusterLogAllocation.letReplicaBecomeLeader(sourceTopicPartition, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.logPlacements(sourceTopicPartition).get(0).broker());
    Assertions.assertEquals(
        0, clusterLogAllocation.logPlacements(sourceTopicPartition).get(1).broker());
    Assertions.assertDoesNotThrow(() -> LayeredClusterLogAllocation.of(clusterLogAllocation));
  }

  @Test
  void logPlacements() {
    final var allocation =
        LayeredClusterLogAllocation.of(
            Map.of(TopicPartition.of("topic", "0"), List.of(LogPlacement.of(0, "/nowhere"))));

    Assertions.assertEquals(1, allocation.logPlacements(TopicPartition.of("topic", "0")).size());
    Assertions.assertEquals(
        0, allocation.logPlacements(TopicPartition.of("topic", "0")).get(0).broker());
    Assertions.assertEquals(
        "/nowhere",
        allocation
            .logPlacements(TopicPartition.of("topic", "0"))
            .get(0)
            .logDirectory()
            .orElseThrow());
    Assertions.assertNull(allocation.logPlacements(TopicPartition.of("no", "0")));
    allocation.logPlacements(TopicPartition.of("no", "0"));
  }

  @Test
  void topicPartitionStream() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(10, 10, 10, 3);
    final var allocation0 = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var allocation1 = LayeredClusterLogAllocation.of(allocation0);
    fakeClusterInfo.topics().stream()
        .flatMap(x -> fakeClusterInfo.replicas(x).stream())
        .forEach(
            replica ->
                allocation1.letReplicaBecomeLeader(
                    TopicPartition.of(replica.topic(), Integer.toString(replica.partition())), 1));

    final var allTopicPartitions =
        allocation1
            .topicPartitionStream()
            .sorted(
                Comparator.comparing(TopicPartition::topic)
                    .thenComparing(TopicPartition::partition))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(10 * 10, allTopicPartitions.size());
    final var expectedTopicPartitions =
        fakeClusterInfo.topics().stream()
            .flatMap(x -> fakeClusterInfo.replicas(x).stream())
            .map(x -> TopicPartition.of(x.topic(), Integer.toString(x.partition())))
            .distinct()
            .sorted(
                Comparator.comparing(TopicPartition::topic)
                    .thenComparing(TopicPartition::partition))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(expectedTopicPartitions, allTopicPartitions);
  }

  @Test
  void lockWorks() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(10, 10, 10, 3);
    final var allocation = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var toModify = allocation.topicPartitionStream().findFirst().orElseThrow();

    // can modify before lock
    Assertions.assertDoesNotThrow(() -> allocation.migrateReplica(toModify, 0, 9));
    Assertions.assertDoesNotThrow(() -> allocation.migrateReplica(toModify, 1, 8, "somewhere"));
    Assertions.assertDoesNotThrow(() -> allocation.letReplicaBecomeLeader(toModify, 2));

    final var extended = LayeredClusterLogAllocation.of(allocation);

    // cannot modify after some other layer rely on it
    Assertions.assertThrows(
        IllegalStateException.class, () -> allocation.migrateReplica(toModify, 9, 0));
    Assertions.assertThrows(
        IllegalStateException.class, () -> allocation.migrateReplica(toModify, 0, 9, "somewhere"));
    Assertions.assertThrows(
        IllegalStateException.class, () -> allocation.letReplicaBecomeLeader(toModify, 0));

    // the extended one can modify
    Assertions.assertDoesNotThrow(() -> extended.migrateReplica(toModify, 9, 0));
    Assertions.assertDoesNotThrow(() -> extended.migrateReplica(toModify, 8, 1, "somewhere"));
    Assertions.assertDoesNotThrow(() -> extended.letReplicaBecomeLeader(toModify, 0));
  }
}
