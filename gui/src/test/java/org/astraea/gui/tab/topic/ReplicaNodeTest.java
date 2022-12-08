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
package org.astraea.gui.tab.topic;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.gui.Context;
import org.astraea.gui.pane.Argument;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicaNodeTest extends RequireBrokerCluster {

  @Test
  void testTableAction() {
    var topicName = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var action = ReplicaNode.tableViewAction(new Context(admin));
      var log = new AtomicReference<String>();
      var f = action.apply(List.of(), Argument.of(List.of(), Map.of()), log::set);
      Assertions.assertTrue(f.toCompletableFuture().isDone());
      Assertions.assertEquals("nothing to alert", log.get());

      var f2 =
          action.apply(
              List.of(Map.of(ReplicaNode.TOPIC_NAME_KEY, topicName, ReplicaNode.PARTITION_KEY, 0)),
              Argument.of(List.of(), Map.of()),
              log::set);
      Assertions.assertTrue(f2.toCompletableFuture().isDone());
      Assertions.assertEquals("please define " + ReplicaNode.MOVE_BROKER_KEY, log.get());

      var f3 =
          action.apply(
              List.of(Map.of(ReplicaNode.TOPIC_NAME_KEY, topicName, ReplicaNode.PARTITION_KEY, 0)),
              Argument.of(
                  List.of(),
                  Map.of(
                      ReplicaNode.MOVE_BROKER_KEY,
                      Optional.of(
                          brokerIds().stream()
                              .map(String::valueOf)
                              .collect(Collectors.joining(","))))),
              log::set);
      f3.toCompletableFuture().join();
      Assertions.assertEquals("succeed to alter partitions: [" + topicName + "-0]", log.get());
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          3,
          admin
              .clusterInfo(Set.of(topicName))
              .toCompletableFuture()
              .join()
              .replicaStream()
              .count());

      var id = brokerIds().iterator().next();
      var path = List.copyOf(logFolders().get(id)).get(2);

      var f4 =
          action.apply(
              List.of(Map.of(ReplicaNode.TOPIC_NAME_KEY, topicName, ReplicaNode.PARTITION_KEY, 0)),
              Argument.of(
                  List.of(), Map.of(ReplicaNode.MOVE_BROKER_KEY, Optional.of(id + ":" + path))),
              log::set);
      f4.toCompletableFuture().join();
      Assertions.assertEquals("succeed to alter partitions: [" + topicName + "-0]", log.get());
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          1,
          admin
              .clusterInfo(Set.of(topicName))
              .toCompletableFuture()
              .join()
              .replicaStream()
              .count());
      Assertions.assertEquals(
          id,
          admin
              .clusterInfo(Set.of(topicName))
              .toCompletableFuture()
              .join()
              .replicas()
              .get(0)
              .nodeInfo()
              .id());
      Assertions.assertEquals(
          path,
          admin
              .clusterInfo(Set.of(topicName))
              .toCompletableFuture()
              .join()
              .replicas()
              .get(0)
              .path());
    }
  }

  @Test
  void testResult() {
    var topic = Utils.randomString();
    var partition = 0;
    var leaderSize = 100;
    var nodes = List.of(NodeInfo.of(0, "aa", 0), NodeInfo.of(1, "aa", 0), NodeInfo.of(2, "aa", 0));
    var replicas =
        List.of(
            Replica.builder()
                .isLeader(true)
                .topic(topic)
                .partition(partition)
                .nodeInfo(nodes.get(0))
                .size(leaderSize)
                .path("/tmp/aaa")
                .build(),
            Replica.builder()
                .isLeader(false)
                .topic(topic)
                .partition(partition)
                .nodeInfo(nodes.get(1))
                .size(20)
                .build(),
            Replica.builder()
                .isLeader(false)
                .topic(topic)
                .partition(partition)
                .nodeInfo(nodes.get(2))
                .size(30)
                .build());
    var results = ReplicaNode.allResult(ClusterInfo.of(nodes, replicas));
    Assertions.assertEquals(3, results.size());
    Assertions.assertEquals(
        1,
        results.stream()
            .filter(
                m ->
                    !m.containsKey(ReplicaNode.LEADER_SIZE_KEY)
                        && !m.containsKey(ReplicaNode.PROGRESS_KEY))
            .count());
    Assertions.assertEquals(
        2,
        results.stream()
            .filter(
                m ->
                    m.containsKey(ReplicaNode.LEADER_SIZE_KEY)
                        && m.containsKey(ReplicaNode.PROGRESS_KEY))
            .count());
    Assertions.assertEquals(
        1, results.stream().filter(m -> m.containsKey(ReplicaNode.PATH_KEY)).count());
    Assertions.assertEquals(
        Set.of("30.00%", "20.00%"),
        results.stream()
            .filter(
                m ->
                    m.containsKey(ReplicaNode.LEADER_SIZE_KEY)
                        && m.containsKey(ReplicaNode.PROGRESS_KEY))
            .map(m -> m.get(ReplicaNode.PROGRESS_KEY))
            .collect(Collectors.toSet()));
  }
}
