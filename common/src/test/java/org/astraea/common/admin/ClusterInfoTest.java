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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class ClusterInfoTest {

  /**
   * build a cluster info based on replicas. Noted that the node info are collected by the replicas.
   *
   * <p>Be aware that the <code>replicas</code> parameter describes <strong>the replica lists of a
   * subset of topic/partitions</strong>. It doesn't require the topic/partition part to have
   * cluster-wide complete information. But the replica list has to be complete. Provide a partial
   * replica list might result in data loss or unintended replica drop during rebalance plan
   * proposing & execution.
   *
   * @param replicas used to build cluster info
   * @return cluster info
   */
  public static ClusterInfo of(List<Replica> replicas) {
    // TODO: this method is not suitable for production use. Move it to the test scope.
    //  see https://github.com/skiptests/astraea/issues/1185
    return ClusterInfo.of(
        "fake",
        replicas.stream()
            .map(
                r ->
                    new Broker(
                        r.brokerId(),
                        "hpost",
                        22222,
                        false,
                        Config.EMPTY,
                        List.of(),
                        Set.of(),
                        Set.of()))
            .collect(Collectors.groupingBy(Broker::id, Collectors.reducing((x, y) -> x)))
            .values()
            .stream()
            .flatMap(Optional::stream)
            .toList(),
        Map.of(),
        replicas);
  }

  @Test
  void testEmptyCluster() {
    var emptyCluster = ClusterInfo.empty();
    Assertions.assertEquals(0, emptyCluster.brokers().size());
    Assertions.assertEquals(0, emptyCluster.replicaStream().count());
  }

  @RepeatedTest(3)
  void testTopics() {
    var nodes =
        IntStream.range(0, ThreadLocalRandom.current().nextInt(3, 9))
            .boxed()
            .collect(Collectors.toUnmodifiableSet());
    var topics =
        IntStream.range(0, ThreadLocalRandom.current().nextInt(0, 100))
            .mapToObj(x -> Utils.randomString())
            .collect(Collectors.toUnmodifiableSet());
    var builder =
        ClusterInfo.builder()
            .addNode(nodes)
            .addFolders(
                nodes.stream()
                    .collect(Collectors.toUnmodifiableMap(x -> x, x -> Set.of("/folder"))));
    topics.forEach(
        t ->
            builder.addTopic(
                t,
                ThreadLocalRandom.current().nextInt(1, 10),
                (short) ThreadLocalRandom.current().nextInt(1, nodes.size())));

    var cluster = builder.build();
    Assertions.assertEquals(topics, cluster.topics().keySet());
    Assertions.assertEquals(topics, cluster.topicNames());
  }

  @Test
  void testReturnCollectionUnmodifiable() {
    var cluster = ClusterInfo.empty();
    var replica = Replica.builder().topic("topic").partition(0).brokerId(0).path("f").buildLeader();
    Assertions.assertThrows(Exception.class, () -> cluster.replicas().add(replica));
    Assertions.assertThrows(Exception.class, () -> cluster.replicas("t").add(replica));
    Assertions.assertThrows(
        Exception.class, () -> cluster.replicas(TopicPartition.of("t", 0)).add(replica));
    Assertions.assertThrows(
        Exception.class, () -> cluster.replicas(TopicPartitionReplica.of("t", 0, 10)).add(replica));
  }
}
