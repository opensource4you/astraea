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
package org.astraea.common.balancer;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Config;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;

public class FakeClusterInfo {

  public static ClusterInfo of(
      int nodeCount, int topicCount, int partitionCount, int replicaCount) {
    return of(nodeCount, topicCount, partitionCount, replicaCount, 3);
  }

  public static ClusterInfo of(
      int nodeCount, int topicCount, int partitionCount, int replicaCount, int folderCount) {
    final var random = new Random();
    random.setSeed(0);

    return of(
        nodeCount,
        topicCount,
        partitionCount,
        replicaCount,
        folderCount,
        (partition) ->
            IntStream.range(0, partition)
                .mapToObj(
                    (ignore) -> {
                      final var suffix =
                          IntStream.range(0, 5)
                              .map(i -> random.nextInt(26))
                              .map(i -> 'a' + i)
                              .mapToObj(i -> String.valueOf((char) i))
                              .collect(Collectors.joining());
                      return "fake-topic-" + suffix;
                    })
                .collect(Collectors.toUnmodifiableSet()));
  }

  public static ClusterInfo of(
      int nodeCount,
      int topicCount,
      int partitionCount,
      int replicaCount,
      int folderCount,
      Function<Integer, Set<String>> topicNameGenerator) {
    final var dataDirectories =
        IntStream.range(0, folderCount)
            .mapToObj(i -> "/tmp/data-directory-" + i)
            .collect(Collectors.toUnmodifiableSet());
    final var nodes =
        IntStream.range(0, nodeCount)
            .mapToObj(nodeId -> Broker.of(nodeId, "host" + nodeId, 9092))
            .map(
                node ->
                    new Broker(
                        node.id(),
                        node.host(),
                        node.port(),
                        false,
                        new Config(Map.of()),
                        dataDirectories.stream()
                            .map(path -> new Broker.DataFolder(path, Map.of(), Map.of()))
                            .toList(),
                        Set.of(),
                        Set.of()))
            .toList();
    final var dataDirectoryList = List.copyOf(dataDirectories);
    final var topics = topicNameGenerator.apply(topicCount);
    final var replicas =
        topics.stream()
            .flatMap(
                topic ->
                    IntStream.range(0, partitionCount)
                        .mapToObj(p -> TopicPartition.of(topic, Integer.toString(p))))
            .flatMap(
                tp ->
                    IntStream.range(0, replicaCount)
                        .mapToObj(
                            r ->
                                Replica.builder()
                                    .topic(tp.topic())
                                    .partition(tp.partition())
                                    .brokerId(nodes.get(r).id())
                                    .lag(0)
                                    .size(-1)
                                    .isLeader(r == 0)
                                    .isSync(true)
                                    .isFuture(false)
                                    .isOffline(false)
                                    .isPreferredLeader(r == 0)
                                    .path(
                                        dataDirectoryList.get(
                                            tp.partition() % dataDirectories.size()))
                                    .build()))
            .toList();

    return ClusterInfo.of("fake", List.copyOf(nodes), Map.of(), replicas);
  }
}
