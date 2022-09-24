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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.balancer.FakeClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClusterLogAllocationTest {

  @Test
  void creation() {
    // empty replica set
    var badAllocation0 = Map.of(TopicPartition.of("topic", "0"), List.<Replica>of());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation0));

    // partial topic/partition
    var badAllocation1 =
        Map.of(
            TopicPartition.of("topic", "999"),
            List.of(
                Replica.of(
                    "topic",
                    999,
                    NodeInfo.of(1001, null, -1),
                    0,
                    0,
                    true,
                    false,
                    false,
                    false,
                    false,
                    "xx")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation1));

    // duplicate replica
    var badAllocation2 =
        Map.of(
            TopicPartition.of("topic", "0"),
            List.of(
                Replica.of(
                    "topic",
                    999,
                    NodeInfo.of(1001, null, -1),
                    0,
                    0,
                    true,
                    false,
                    false,
                    false,
                    false,
                    "xx"),
                Replica.of(
                    "topic",
                    999,
                    NodeInfo.of(1001, null, -1),
                    0,
                    0,
                    true,
                    false,
                    false,
                    false,
                    false,
                    "xx"),
                Replica.of(
                    "topic",
                    999,
                    NodeInfo.of(1001, null, -1),
                    0,
                    0,
                    true,
                    false,
                    false,
                    false,
                    false,
                    "xx")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation2));
  }

  @Test
  void migrateReplica() {
    final var fakeCluster = FakeClusterInfo.of(3, 1, 1, 1, (i) -> Set.of("topic"));
    var clusterLogAllocation = ClusterLogAllocation.of(fakeCluster);
    final var sourceTopicPartition = fakeCluster.replicas("topic").get(0);
    clusterLogAllocation =
        clusterLogAllocation.migrateReplica(sourceTopicPartition.topicPartitionReplica(), 1);

    Assertions.assertEquals(
        1,
        clusterLogAllocation
            .logPlacements(sourceTopicPartition.topicPartition())
            .get(0)
            .nodeInfo()
            .id());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"null", "/tmp/data-directory-0", "/tmp/data-directory-1", "/tmp/data-directory-2"})
  void migrateReplica(String dataDirectory) {
    dataDirectory = dataDirectory.equals("null") ? null : dataDirectory;
    final var fakeCluster = FakeClusterInfo.of(3, 1, 1, 1, (i) -> Set.of("topic"));
    var clusterLogAllocation = ClusterLogAllocation.of(fakeCluster);
    final var sourceReplica = fakeCluster.replicas("topic").get(0);

    clusterLogAllocation =
        clusterLogAllocation.migrateReplica(
            sourceReplica.topicPartitionReplica(), 1, dataDirectory);

    Assertions.assertEquals(
        1,
        clusterLogAllocation.logPlacements(sourceReplica.topicPartition()).get(0).nodeInfo().id());
    Assertions.assertEquals(
        dataDirectory,
        clusterLogAllocation.logPlacements(sourceReplica.topicPartition()).get(0).dataFolder());

    clusterLogAllocation =
        clusterLogAllocation.migrateReplica(
            TopicPartitionReplica.of(sourceReplica.topic(), sourceReplica.partition(), 1),
            1,
            dataDirectory);
    Assertions.assertEquals(
        1,
        clusterLogAllocation.logPlacements(sourceReplica.topicPartition()).get(0).nodeInfo().id());
    Assertions.assertEquals(
        dataDirectory,
        clusterLogAllocation.logPlacements(sourceReplica.topicPartition()).get(0).dataFolder());
  }

  @Test
  void letReplicaBecomeLeader() {
    final var fakeCluster = FakeClusterInfo.of(3, 1, 1, 2, (i) -> Set.of("topic"));
    var clusterLogAllocation = ClusterLogAllocation.of(fakeCluster);
    final var sourceTopicPartition = fakeCluster.replicas("topic").get(1);

    clusterLogAllocation =
        clusterLogAllocation.letReplicaBecomeLeader(sourceTopicPartition.topicPartitionReplica());

    Assertions.assertEquals(
        1,
        clusterLogAllocation
            .logPlacements(sourceTopicPartition.topicPartition())
            .get(0)
            .nodeInfo()
            .id());
    Assertions.assertEquals(
        0,
        clusterLogAllocation
            .logPlacements(sourceTopicPartition.topicPartition())
            .get(1)
            .nodeInfo()
            .id());
  }

  @Test
  void logPlacements() {
    final var allocation =
        ClusterLogAllocation.of(
            Map.of(
                TopicPartition.of("topic", "0"),
                List.of(
                    Replica.of(
                        "topic",
                        0,
                        NodeInfo.of(0, null, -1),
                        0,
                        0,
                        true,
                        true,
                        false,
                        false,
                        false,
                        "/nowhere"))));

    Assertions.assertEquals(1, allocation.logPlacements(TopicPartition.of("topic", "0")).size());
    Assertions.assertEquals(
        0, allocation.logPlacements(TopicPartition.of("topic", "0")).get(0).nodeInfo().id());
    Assertions.assertEquals(
        "/nowhere", allocation.logPlacements(TopicPartition.of("topic", "0")).get(0).dataFolder());
    Assertions.assertEquals(0, allocation.logPlacements(TopicPartition.of("no", "0")).size());
    allocation.logPlacements(TopicPartition.of("no", "0"));
  }

  @Test
  void findNonFulfilledAllocation() {
    final var clusterInfo = FakeClusterInfo.of(3, 10, 10, 2);
    Assertions.assertEquals(
        Set.of(),
        ClusterLogAllocation.findNonFulfilledAllocation(
            ClusterLogAllocation.of(clusterInfo), ClusterLogAllocation.of(clusterInfo)));

    final var source = ClusterLogAllocation.of(clusterInfo);
    final var topicPartition0 =
        clusterInfo.replicas().stream()
            .filter(x -> x.partition() == 0)
            .filter(x -> x.nodeInfo().id() == 0)
            .findFirst()
            .orElseThrow();
    final var topicPartition1 =
        clusterInfo.replicas().stream()
            .filter(x -> x.topic().equals(topicPartition0.topic()))
            .filter(x -> x.partition() == 1)
            .filter(x -> !x.isLeader())
            .findFirst()
            .orElseThrow();

    final var target0 =
        source.migrateReplica(topicPartition0.topicPartitionReplica(), 0, "/somewhere");
    Assertions.assertEquals(
        Set.of(topicPartition0.topicPartition()),
        ClusterLogAllocation.findNonFulfilledAllocation(source, target0));

    final var target1 = source.migrateReplica(topicPartition0.topicPartitionReplica(), 2);
    Assertions.assertEquals(
        Set.of(topicPartition0.topicPartition()),
        ClusterLogAllocation.findNonFulfilledAllocation(source, target1));

    final var target2 = source.letReplicaBecomeLeader(topicPartition1.topicPartitionReplica());
    Assertions.assertEquals(
        Set.of(topicPartition1.topicPartition()),
        ClusterLogAllocation.findNonFulfilledAllocation(source, target2));

    final var target3 =
        source
            .migrateReplica(topicPartition0.topicPartitionReplica(), 2)
            .migrateReplica(
                TopicPartitionReplica.of(topicPartition0.topic(), topicPartition0.partition(), 2),
                2,
                "/somewhere")
            .letReplicaBecomeLeader(topicPartition1.topicPartitionReplica());
    Assertions.assertEquals(
        Set.of(topicPartition0.topicPartition(), topicPartition1.topicPartition()),
        ClusterLogAllocation.findNonFulfilledAllocation(source, target3));

    final var map4 =
        source.topicPartitions().stream().collect(Collectors.toMap(x -> x, source::logPlacements));
    map4.put(
        TopicPartition.of("NewTopic", 0),
        List.of(
            Replica.of(
                "NewTopic",
                0,
                NodeInfo.of(0, null, -1),
                0,
                0,
                true,
                true,
                false,
                false,
                false,
                "?")));
    final var target4 = ClusterLogAllocation.of(map4);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClusterLogAllocation.findNonFulfilledAllocation(source, target4));

    final var allocation0 =
        ClusterLogAllocation.of(
            Map.of(
                TopicPartition.of("topicA", 0),
                    List.of(
                        Replica.of(
                            "topicA",
                            0,
                            NodeInfo.of(0, null, -1),
                            0,
                            0,
                            true,
                            true,
                            false,
                            false,
                            false,
                            "no-change")),
                TopicPartition.of("topicB", 0),
                    List.of(
                        Replica.of(
                            "topicB",
                            0,
                            NodeInfo.of(0, null, -1),
                            0,
                            0,
                            true,
                            true,
                            false,
                            false,
                            false,
                            "no-change"))));
    final var allocation1 =
        ClusterLogAllocation.of(
            Map.of(
                TopicPartition.of("topicB", 0),
                List.of(
                    Replica.of(
                        "topicB",
                        0,
                        NodeInfo.of(0, null, -1),
                        0,
                        0,
                        true,
                        true,
                        false,
                        false,
                        false,
                        "do-change"))));
    Assertions.assertEquals(
        Set.of(TopicPartition.of("topicB", 0)),
        ClusterLogAllocation.findNonFulfilledAllocation(allocation0, allocation1));
  }
}
