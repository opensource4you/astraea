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
package org.astraea.common.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicaInfoTest {

  static org.apache.kafka.common.PartitionInfo partitionInfo() {
    return new org.apache.kafka.common.PartitionInfo(
        "ttt",
        100,
        NodeInfoTest.node(),
        new Node[] {NodeInfoTest.node()},
        new Node[] {NodeInfoTest.node()});
  }

  @Test
  void testAllGetters() {
    var kafkaPartition = partitionInfo();
    var replicaInfo = ReplicaInfo.of(kafkaPartition);
    var followerReplicaNode =
        (Supplier<Set<NodeInfo>>)
            () ->
                Arrays.stream(kafkaPartition.replicas())
                    .map(NodeInfo::of)
                    .filter(node -> !node.equals(NodeInfo.of(kafkaPartition.leader())))
                    .collect(Collectors.toUnmodifiableSet());
    final List<ReplicaInfo> leader =
        replicaInfo.stream().filter(ReplicaInfo::isLeader).collect(Collectors.toList());
    var followers =
        replicaInfo.stream().filter(ReplicaInfo::isFollower).collect(Collectors.toList());

    // test topic/partition
    replicaInfo.forEach(
        replica -> Assertions.assertEquals(kafkaPartition.topic(), replica.topic()));
    replicaInfo.forEach(
        replica -> Assertions.assertEquals(kafkaPartition.partition(), replica.partition()));

    // test leader
    Assertions.assertEquals(1, leader.size());
    Assertions.assertEquals(
        NodeInfo.of(kafkaPartition.leader()), leader.iterator().next().nodeInfo());

    // test followers
    Assertions.assertEquals(
        kafkaPartition.replicas().length - 1,
        followers.size(),
        "The follower replica node count should match");
    Assertions.assertEquals(
        followerReplicaNode.get(),
        followers.stream().map(ReplicaInfo::nodeInfo).collect(Collectors.toUnmodifiableSet()),
        "The follower replica node set should match");
  }
}
