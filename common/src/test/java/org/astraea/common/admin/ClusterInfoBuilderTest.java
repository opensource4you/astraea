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
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ClusterInfoBuilderTest {

  @Test
  void testBuild() {
    var host1000 = NodeInfo.of(1000, "host1000", 9092);
    var host2000 = NodeInfo.of(2000, "host2000", 9092);
    var host3000 = NodeInfo.of(3000, "host3000", 9092);
    var replica =
        Replica.builder()
            .topic("MyTopic")
            .partition(0)
            .nodeInfo(host1000)
            .size(1024)
            .isPreferredLeader(true)
            .isLeader(true)
            .build();
    var cluster = ClusterInfo.of(Set.of(host1000, host2000, host3000), List.of(replica));

    Assertions.assertEquals(
        Set.of(host1000, host2000, host3000), ClusterInfoBuilder.builder(cluster).build().nodes());
    Assertions.assertEquals(
        List.of(replica), ClusterInfoBuilder.builder(cluster).build().replicas());
    Assertions.assertEquals(Set.of(), ClusterInfoBuilder.builder().build().nodes());
    Assertions.assertEquals(List.of(), ClusterInfoBuilder.builder().build().replicas());
  }

  @Test
  void addNode() {
    Assertions.assertEquals(
        Set.of(), ClusterInfoBuilder.builder().addNode(Set.of()).build().nodes());
    Assertions.assertEquals(
        Set.of(1, 2, 3, 4, 5, 100),
        ClusterInfoBuilder.builder().addNode(Set.of(1, 2, 3, 4, 5, 100)).build().nodes().stream()
            .map(NodeInfo::id)
            .collect(Collectors.toSet()));
    Assertions.assertEquals(
        ClusterInfoBuilder.builder().addNode(Set.of(1, 2, 3)).build().nodes(),
        ClusterInfoBuilder.builder().addNode(Set.of(1, 2, 3)).build().nodes(),
        "The port number is generated by deterministic random");
  }

  @Test
  void addFolders() {
    Assertions.assertEquals(
        Map.of(1, Set.of(), 2, Set.of(), 3, Set.of()),
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(
                Map.of(
                    1, Set.of(),
                    2, Set.of(),
                    3, Set.of()))
            .build()
            .brokerFolders());
    Assertions.assertEquals(
        Map.of(1, Set.of(), 2, Set.of(), 3, Set.of("/ssd1", "/ssd2", "/ssd3")),
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(
                Map.of(
                    1, Set.of(),
                    2, Set.of(),
                    3, Set.of("/ssd1", "/ssd2", "/ssd3")))
            .build()
            .brokerFolders());
    Assertions.assertEquals(
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(
                Map.of(
                    1, Set.of("/ssd1", "/ssd2", "/ssd3"),
                    2, Set.of("/ssd1", "/ssd2", "/hdd3"),
                    3, Set.of("/hdd1", "/hdd2", "/hdd3")))
            .build()
            .brokerFolders(),
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(
                Map.of(
                    1, Set.of("/ssd1", "/ssd2", "/ssd3"),
                    2, Set.of("/ssd1", "/ssd2", "/hdd3"),
                    3, Set.of("/hdd1", "/hdd2", "/hdd3")))
            .build()
            .brokerFolders());
  }

  @Test
  void addTopic() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClusterInfoBuilder.builder().addTopic("topic", 1, (short) 1).build(),
        "Insufficient nodes");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ClusterInfoBuilder.builder().addNode(Set.of(1)).addTopic("topic", 1, (short) 2).build(),
        "Insufficient nodes");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ClusterInfoBuilder.builder().addNode(Set.of(1)).addTopic("topic", 1, (short) 1).build(),
        "No folder info");

    var cluster =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(
                Map.of(
                    1, Set.of("/broker1/ssd1", "/broker1/ssd2", "/broker1/ssd3"),
                    2, Set.of("/broker2/ssd1", "/broker2/ssd2", "/broker2/ssd3"),
                    3, Set.of("/broker3/ssd1", "/broker3/ssd2", "/broker3/ssd3")))
            .addTopic("topic", 10, (short) 2, r -> Replica.builder(r).lag(5566).build())
            .build();
    Assertions.assertTrue(cluster.replicaStream().allMatch(i -> i.lag() == 5566));
    Assertions.assertTrue(cluster.replicaStream().allMatch(i -> i.topic().equals("topic")));
    Assertions.assertEquals(
        Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
        cluster.replicaStream().map(ReplicaInfo::partition).collect(Collectors.toSet()),
        "Correct number of partitions");
    Assertions.assertTrue(
        cluster
            .replicaStream()
            .collect(Collectors.groupingBy(ReplicaInfo::partition, Collectors.counting()))
            .values()
            .stream()
            .allMatch(count -> count == 2),
        "Correct number of replicas");
    for (int id : Set.of(1, 2, 3)) {
      var folderLogs =
          cluster.brokerFolders().get(id).stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      path -> path,
                      path ->
                          cluster
                              .replicaStream()
                              .filter(r -> r.nodeInfo().id() == id)
                              .filter(r -> r.path().equals(path))
                              .count()));
      var summary = folderLogs.values().stream().mapToLong(x -> x).summaryStatistics();

      var logEvenlyDistributed =
          0 <= summary.getMax() - summary.getMin() && summary.getMax() - summary.getMin() <= 1;
      Assertions.assertTrue(
          logEvenlyDistributed,
          "Log is not evenly distributed according to the implementation detail of Kafka: "
              + folderLogs);
    }
  }

  @Test
  void map() {
    var cluster1 =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(Map.of(1, Set.of("/ssd1", "/ssd2", "/ssd3")))
            .addFolders(Map.of(2, Set.of("/ssd1", "/ssd2", "/ssd3")))
            .addFolders(Map.of(3, Set.of("/ssd1", "/ssd2", "/ssd3")))
            .addTopic("topic", 10, (short) 3)
            .addTopic("unrelated", 10, (short) 3)
            .build();
    Assertions.assertTrue(cluster1.replicaStream().allMatch(i -> i.lag() == 0));
    var cluster2 =
        ClusterInfoBuilder.builder(cluster1)
            .mapLog(r -> r.topic().equals("topic") ? Replica.builder(r).lag(5566).build() : r)
            .build();
    Assertions.assertTrue(
        cluster2
            .replicaStream()
            .allMatch(i -> (i.topic().equals("topic")) ? (i.lag() == 5566) : (i.lag() == 0)));
  }

  @Test
  void reassignReplica() {
    var base =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3, 4))
            .addFolders(Map.of(1, Set.of("/ssd1", "/ssd2", "/ssd3", "/ssd4")))
            .addFolders(Map.of(2, Set.of("/ssd1", "/ssd2", "/ssd3", "/ssd4")))
            .addFolders(Map.of(3, Set.of("/ssd1", "/ssd2", "/ssd3", "/ssd4")))
            .addFolders(Map.of(4, Set.of("/ssd1", "/ssd2", "/ssd3", "/ssd4")))
            .build();
    var original =
        ClusterInfoBuilder.builder(base)
            .addTopic(
                "pipeline",
                5,
                (short) 2,
                (replica) ->
                    Replica.builder(replica)
                        .nodeInfo(base.node(replica.isPreferredLeader() ? 1 : 2))
                        .path(replica.isPreferredLeader() ? "/ssd1" : "/ssd2")
                        .build())
            .build();
    var altered =
        ClusterInfoBuilder.builder(original)
            .reassignReplica(TopicPartitionReplica.of("pipeline", 0, 1), 3, "/ssd3")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 1, 1), 3, "/ssd3")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 2, 1), 3, "/ssd3")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 3, 1), 3, "/ssd3")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 4, 1), 3, "/ssd3")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 0, 2), 4, "/ssd4")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 1, 2), 4, "/ssd4")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 2, 2), 4, "/ssd4")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 3, 2), 4, "/ssd4")
            .reassignReplica(TopicPartitionReplica.of("pipeline", 4, 2), 4, "/ssd4")
            .build();

    Assertions.assertTrue(
        original
            .replicaStream()
            .filter(Replica::isPreferredLeader)
            .allMatch(r -> r.nodeInfo().id() == 1 && r.path().equals("/ssd1")));
    Assertions.assertTrue(
        original
            .replicaStream()
            .filter(Predicate.not(Replica::isPreferredLeader))
            .allMatch(r -> r.nodeInfo().id() == 2 && r.path().equals("/ssd2")));
    Assertions.assertTrue(
        altered
            .replicaStream()
            .filter(Replica::isPreferredLeader)
            .allMatch(r -> r.nodeInfo().id() == 3 && r.path().equals("/ssd3")));
    Assertions.assertTrue(
        altered
            .replicaStream()
            .filter(Predicate.not(Replica::isPreferredLeader))
            .allMatch(r -> r.nodeInfo().id() == 4 && r.path().equals("/ssd4")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ClusterInfoBuilder.builder()
                .addNode(Set.of(0))
                .addFolders(Map.of(0, Set.of("/ssd0")))
                .addTopic("topic", 1, (short) 1)
                .reassignReplica(TopicPartitionReplica.of("topic", 0, 43), 1, "/there")
                .build(),
        "No such live broker 0");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ClusterInfoBuilder.builder()
                .reassignReplica(TopicPartitionReplica.of("no", 0, 0), 1, "/there")
                .build(),
        "No such replica");
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            ClusterInfoBuilder.builder()
                .addNode(Set.of(0))
                .addFolders(Map.of(0, Set.of("/ssd0")))
                .addTopic("topic", 2, (short) 1)
                .addNode(Set.of(1))
                .addFolders(Map.of(1, Set.of("/ssd1")))
                .mapLog(r -> Replica.builder(r).partition(0).build())
                .reassignReplica(TopicPartitionReplica.of("topic", 0, 0), 1, "/ssd1")
                .build(),
        "corrupted structure");
  }

  @Test
  void setPreferredLeader() {
    var original =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(0, 1, 2, 3))
            .addFolders(Map.of(0, Set.of("/ssd")))
            .addFolders(Map.of(1, Set.of("/ssd")))
            .addFolders(Map.of(2, Set.of("/ssd")))
            .addFolders(Map.of(3, Set.of("/ssd")))
            .addTopic(
                "pipeline",
                4,
                (short) 4,
                (replica) ->
                    Replica.builder(replica)
                        .isLeader(replica.nodeInfo().id() == 0)
                        .isPreferredLeader(replica.nodeInfo().id() == 0)
                        .build())
            .build();
    var altered =
        ClusterInfoBuilder.builder(original)
            .setPreferredLeader(TopicPartitionReplica.of("pipeline", 0, 0))
            .setPreferredLeader(TopicPartitionReplica.of("pipeline", 1, 1))
            .setPreferredLeader(TopicPartitionReplica.of("pipeline", 2, 2))
            .setPreferredLeader(TopicPartitionReplica.of("pipeline", 3, 3))
            .build();

    Assertions.assertTrue(
        original
            .replicaStream()
            .filter((Replica::isPreferredLeader))
            .allMatch(r -> r.nodeInfo().id() == 0));
    Assertions.assertTrue(
        original.replicaStream().filter((Replica::isLeader)).allMatch(r -> r.nodeInfo().id() == 0));
    Assertions.assertTrue(
        altered
            .replicaStream()
            .filter(Replica::isPreferredLeader)
            .allMatch(r -> r.nodeInfo().id() == r.partition()));
    Assertions.assertTrue(
        altered
            .replicaStream()
            .filter(Replica::isLeader)
            .allMatch(r -> r.nodeInfo().id() == r.partition()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ClusterInfoBuilder.builder()
                .setPreferredLeader(TopicPartitionReplica.of("no", 0, 0))
                .build(),
        "No such replica");
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            ClusterInfoBuilder.builder()
                .addNode(Set.of(0))
                .addFolders(Map.of(0, Set.of("/ssd0")))
                .addTopic("topic", 2, (short) 1)
                .addNode(Set.of(1))
                .addFolders(Map.of(1, Set.of("/ssd1")))
                .mapLog(r -> Replica.builder(r).partition(0).build())
                .setPreferredLeader(TopicPartitionReplica.of("topic", 0, 0))
                .build(),
        "corrupted structure");
  }

  @DisplayName("FakeBroker can interact with normal NodeInfo properly")
  @ParameterizedTest
  @CsvSource(
      value = {
        "  1, host1, 1000",
        " 20, host2, 2000",
        "300, host3, 3000",
      })
  void testFakeBrokerInteraction(int id, String host, int port) {
    var node0 = ClusterInfoBuilder.FakeBroker.of(id, host, port, List.of());
    var node1 = NodeInfo.of(id, host, port);
    var node2 = NodeInfo.of(id + 1, host, port);

    Assertions.assertEquals(node0.hashCode(), node1.hashCode());
    Assertions.assertNotEquals(node0.hashCode(), node2.hashCode());
    Assertions.assertEquals(node0, node1);
    Assertions.assertNotEquals(node0, node2);
  }
}
